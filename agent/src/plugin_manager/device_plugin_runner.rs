use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, SystemTime},
};

use akri_shared::uds::unix_stream;
use async_trait::async_trait;
use futures::{StreamExt, TryFutureExt};
use thiserror::Error;
use tokio::net::{UnixListener, UnixStream};
use tokio_stream::wrappers::WatchStream;
use tonic::{
    Request,
    transport::{Endpoint, Server, Uri},
};
use tower::service_fn;

use crate::util::stopper::Stopper;

/// Current version of the API supported by kubelet.
pub const K8S_DEVICE_PLUGIN_VERSION: &str = "v1beta1";

/// DevicePluginPath is the folder the kubelet expects to find Device-Plugin sockets.
pub const DEVICE_PLUGIN_PATH: &str = "/var/lib/kubelet/device-plugins";

/// Path of the Kubelet registry socket
pub const KUBELET_SOCKET: &str = "/var/lib/kubelet/device-plugins/kubelet.sock";

/// How often a plugin checks that its socket still exists. A starting kubelet deletes every plugin
/// socket, and each plugin must then register again.
const SOCKET_CHECK_INTERVAL: Duration = Duration::from_secs(5);

use super::v1beta1::{
    AllocateRequest, AllocateResponse, DevicePluginOptions, Empty, ListAndWatchResponse,
    RegisterRequest,
    device_plugin_server::{DevicePlugin, DevicePluginServer},
    registration_client,
};

#[async_trait]
pub(super) trait InternalDevicePlugin: Sync + Send {
    type DeviceStore: Clone + Send + Sync + 'static;
    async fn list_and_watch(
        &self,
    ) -> Result<tonic::Response<DeviceUsageStream<Self::DeviceStore>>, tonic::Status>;
    async fn allocate(
        &self,
        requests: Request<AllocateRequest>,
    ) -> Result<tonic::Response<AllocateResponse>, tonic::Status>;

    fn get_name(&self) -> String;

    async fn stopped(&self);
    fn stop(&self);
}

pub(super) struct DeviceUsageStream<T: Clone + 'static + Send + Sync> {
    pub device_usage_to_device: fn(&str, &str, T) -> Result<ListAndWatchResponse, tonic::Status>,
    pub input_stream: futures::stream::Abortable<WatchStream<T>>,
    pub device_name: String,
    pub node_name: String,
}

impl<T: Clone + 'static + Send + Sync> futures::Stream for DeviceUsageStream<T> {
    type Item = Result<ListAndWatchResponse, tonic::Status>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        match self.input_stream.poll_next_unpin(cx) {
            std::task::Poll::Ready(Some(i)) => std::task::Poll::Ready(Some((self
                .device_usage_to_device)(
                &self.device_name,
                &self.node_name,
                i,
            ))),
            std::task::Poll::Ready(None) => {
                trace!("Stream Stopped");
                std::task::Poll::Ready(None)
            }
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}

struct DevicePluginImpl<T: Clone + 'static + Send + Sync> {
    inner: Arc<dyn InternalDevicePlugin<DeviceStore = T>>,
}

#[async_trait]
impl<T: Clone + 'static + Send + Sync> DevicePlugin for DevicePluginImpl<T> {
    async fn get_device_plugin_options(
        &self,
        _request: tonic::Request<Empty>,
    ) -> Result<tonic::Response<DevicePluginOptions>, tonic::Status> {
        Ok(tonic::Response::new(DevicePluginOptions {
            pre_start_required: false,
            get_preferred_allocation_available: false,
        }))
    }

    type ListAndWatchStream = DeviceUsageStream<T>;

    async fn list_and_watch(
        &self,
        _request: tonic::Request<Empty>,
    ) -> Result<tonic::Response<Self::ListAndWatchStream>, tonic::Status> {
        self.inner.list_and_watch().await
    }

    async fn allocate(
        &self,
        requests: Request<AllocateRequest>,
    ) -> Result<tonic::Response<AllocateResponse>, tonic::Status> {
        trace!("kubelet called allocate {requests:?}");
        self.inner.allocate(requests).await
    }

    async fn pre_start_container(
        &self,
        _request: Request<super::v1beta1::PreStartContainerRequest>,
    ) -> Result<tonic::Response<super::v1beta1::PreStartContainerResponse>, tonic::Status> {
        error!("pre_start_container - kubelet called pre_start_container !",);
        Ok(tonic::Response::new(
            super::v1beta1::PreStartContainerResponse {},
        ))
    }

    async fn get_preferred_allocation(
        &self,
        _request: tonic::Request<super::v1beta1::PreferredAllocationRequest>,
    ) -> Result<tonic::Response<super::v1beta1::PreferredAllocationResponse>, tonic::Status> {
        error!("get_preferred_allocation - kubelet called get_preferred_allocation",);
        Err(tonic::Status::unimplemented(
            "Get preferred allocation is not implemented for this plugin",
        ))
    }
}

#[derive(Error, Debug)]
pub enum RunnerError {
    #[error("Unable to get current time")]
    TimeError,

    #[error("Unable to serve plugin")]
    ServeError(#[source] std::io::Error),

    #[error("Unable to register plugin to kubelet")]
    RegistrationError(#[from] anyhow::Error),
}

/// Serves the plugin and registers it with the kubelet. Then, until the plugin stops, it serves and
/// registers the plugin again each time the kubelet deletes its socket (kubelet restart).
pub(super) async fn serve_and_register_plugin<T: Clone + 'static + Send + Sync>(
    plugin: Arc<dyn InternalDevicePlugin<DeviceStore = T>>,
) -> Result<(), RunnerError> {
    serve_and_register_plugin_at(
        plugin,
        Path::new(DEVICE_PLUGIN_PATH),
        Path::new(KUBELET_SOCKET),
        SOCKET_CHECK_INTERVAL,
    )
    .await
}

async fn serve_and_register_plugin_at<T: Clone + 'static + Send + Sync>(
    plugin: Arc<dyn InternalDevicePlugin<DeviceStore = T>>,
    plugin_dir: &Path,
    kubelet_socket: &Path,
    check_interval: Duration,
) -> Result<(), RunnerError> {
    let server = match serve_and_register_once(plugin.clone(), plugin_dir, kubelet_socket).await {
        Ok(server) => server,
        Err(e) => {
            plugin.stop();
            return Err(e);
        }
    };
    tokio::spawn(keep_registered(
        plugin,
        server,
        plugin_dir.to_path_buf(),
        kubelet_socket.to_path_buf(),
        check_interval,
    ));
    Ok(())
}

/// One gRPC server of a plugin, on one socket.
struct PluginServer {
    /// Socket file name in the plugin folder, as the kubelet gets it.
    endpoint: String,
    socket_path: PathBuf,
    /// Stops this server only. The plugin's own stop also stops it.
    shutdown: Stopper,
}

async fn serve_and_register_once<T: Clone + 'static + Send + Sync>(
    plugin: Arc<dyn InternalDevicePlugin<DeviceStore = T>>,
    plugin_dir: &Path,
    kubelet_socket: &Path,
) -> Result<PluginServer, RunnerError> {
    let server = serve_plugin(plugin.clone(), plugin_dir).await?;
    if let Err(e) = register_plugin(
        &plugin.get_name(),
        &server.endpoint,
        &server.socket_path,
        kubelet_socket,
    )
    .await
    {
        server.shutdown.stop();
        return Err(RunnerError::RegistrationError(e));
    }
    Ok(server)
}

async fn keep_registered<T: Clone + 'static + Send + Sync>(
    plugin: Arc<dyn InternalDevicePlugin<DeviceStore = T>>,
    mut server: PluginServer,
    plugin_dir: PathBuf,
    kubelet_socket: PathBuf,
    check_interval: Duration,
) {
    loop {
        tokio::select! {
            _ = plugin.stopped() => return,
            _ = socket_removed(&server.socket_path, check_interval) => {},
        }
        info!(
            "socket {} of device plugin {} is gone (kubelet restart), registering again",
            server.socket_path.display(),
            plugin.get_name()
        );
        server.shutdown.stop();
        server = loop {
            match serve_and_register_once(plugin.clone(), &plugin_dir, &kubelet_socket).await {
                Ok(new_server) => break new_server,
                Err(e) => warn!(
                    "device plugin {}: registering again failed, retrying in {}s: {e:?}",
                    plugin.get_name(),
                    check_interval.as_secs_f32()
                ),
            }
            tokio::select! {
                _ = plugin.stopped() => return,
                _ = tokio::time::sleep(check_interval) => {},
            }
        };
    }
}

/// Returns when the socket file is gone.
async fn socket_removed(socket_path: &Path, check_interval: Duration) {
    loop {
        tokio::time::sleep(check_interval).await;
        if let Ok(false) = tokio::fs::try_exists(socket_path).await {
            return;
        }
    }
}

async fn serve_plugin<T: Clone + 'static + Send + Sync>(
    plugin: Arc<dyn InternalDevicePlugin<DeviceStore = T>>,
    plugin_dir: &Path,
) -> Result<PluginServer, RunnerError> {
    let unique_time = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_err(|_| RunnerError::TimeError)?;
    // Milliseconds, so that a new server never reuses the socket path of the old one.
    let endpoint = format!("{}-{}.sock", plugin.get_name(), unique_time.as_millis());
    let socket_path = plugin_dir.join(&endpoint);

    info!(
        "serve - creating a device plugin server that will listen at: {}",
        socket_path.display()
    );
    tokio::fs::create_dir_all(plugin_dir)
        .await
        .map_err(RunnerError::ServeError)?;
    let uds = UnixListener::bind(&socket_path).map_err(RunnerError::ServeError)?;
    let incoming = async_stream::stream! {
        loop {
            let item = uds.accept().map_ok(|(st, _)| unix_stream::UnixStream::new(st)).await;
            yield item;
        }
    };
    let service = DevicePluginServer::new(DevicePluginImpl {
        inner: plugin.clone(),
    });
    let shutdown = Stopper::new();
    let task_shutdown = shutdown.clone();
    let task_socket_path = socket_path.clone();
    tokio::task::spawn(async move {
        let signal = async move {
            tokio::select! {
                _ = plugin.stopped() => {},
                _ = task_shutdown.stopped() => {},
            }
        };
        if let Err(e) = Server::builder()
            .add_service(service)
            .serve_with_incoming_shutdown(incoming, signal)
            .await
        {
            error!(
                "serve - device plugin server at {} failed: {e}",
                task_socket_path.display()
            );
        }
        trace!(
            "serve - gracefully shutdown ... deleting socket {}",
            task_socket_path.display()
        );
        // Socket may already be deleted in the case of the kubelet restart
        std::fs::remove_file(&task_socket_path).unwrap_or(());
    });
    Ok(PluginServer {
        endpoint,
        socket_path,
        shutdown,
    })
}

async fn register_plugin(
    device_plugin_name: &str,
    device_endpoint: &str,
    socket_path: &Path,
    kubelet_socket: &Path,
) -> anyhow::Result<()> {
    use anyhow::Context;

    let capability_id: String = format!("akri.sh/{device_plugin_name}");

    akri_shared::uds::unix_stream::try_connect(&socket_path.to_string_lossy())
        .await
        .with_context(|| format!("while trying to connect to {}", socket_path.display()))?;

    info!("register - entered for Instance {capability_id} and socket_name: {device_endpoint}");
    let op = DevicePluginOptions {
        pre_start_required: false,
        get_preferred_allocation_available: false,
    };

    // We will ignore this dummy uri because UDS does not use it.
    // Some servers will check the uri content so the uri needs to
    // be in valid format even it's not used, the scheme part is used
    // to specific what scheme to use, such as http or https
    let kubelet_socket_closure = kubelet_socket.to_path_buf();
    let channel = Endpoint::try_from("http://[::1]:50051")
        .unwrap()
        .connect_with_connector(service_fn(move |_: Uri| {
            let kubelet_socket_closure = kubelet_socket_closure.clone();
            async move {
                UnixStream::connect(kubelet_socket_closure)
                    .await
                    .map(hyper_util::rt::TokioIo::new)
            }
        }))
        .await
        .with_context(|| format!("while trying to connect to {}", kubelet_socket.display()))?;
    let mut registration_client = registration_client::RegistrationClient::new(channel);

    let register_request = tonic::Request::new(RegisterRequest {
        version: K8S_DEVICE_PLUGIN_VERSION.into(),
        endpoint: device_endpoint.to_string(),
        resource_name: capability_id.to_string(),
        options: Some(op),
    });
    trace!(
        "register - before call to register with the kubelet at socket {}",
        kubelet_socket.display()
    );

    // If fail to register with the kubelet, terminate device plugin
    registration_client.register(register_request).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use tokio::sync::{mpsc, oneshot};

    use super::super::v1beta1::{
        device_plugin_client::DevicePluginClient,
        registration_server::{Registration, RegistrationServer},
    };
    use super::*;

    struct TestPlugin {
        stopper: Stopper,
    }

    #[async_trait]
    impl InternalDevicePlugin for TestPlugin {
        type DeviceStore = ();

        async fn list_and_watch(
            &self,
        ) -> Result<tonic::Response<DeviceUsageStream<()>>, tonic::Status> {
            Err(tonic::Status::unimplemented("test plugin"))
        }

        async fn allocate(
            &self,
            _requests: Request<AllocateRequest>,
        ) -> Result<tonic::Response<AllocateResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented("test plugin"))
        }

        fn get_name(&self) -> String {
            "test-plugin".to_string()
        }

        async fn stopped(&self) {
            self.stopper.stopped().await
        }

        fn stop(&self) {
            self.stopper.stop()
        }
    }

    struct FakeKubelet {
        registrations: mpsc::UnboundedSender<RegisterRequest>,
    }

    #[async_trait]
    impl Registration for FakeKubelet {
        async fn register(
            &self,
            request: Request<RegisterRequest>,
        ) -> Result<tonic::Response<Empty>, tonic::Status> {
            let _ = self.registrations.send(request.into_inner());
            Ok(tonic::Response::new(Empty {}))
        }
    }

    /// Serves a kubelet registration socket until the returned sender is used or dropped.
    fn start_fake_kubelet(
        socket: &Path,
        registrations: mpsc::UnboundedSender<RegisterRequest>,
    ) -> oneshot::Sender<()> {
        let (stop, stopped) = oneshot::channel::<()>();
        let uds = UnixListener::bind(socket).unwrap();
        let incoming = async_stream::stream! {
            loop {
                let item = uds.accept().map_ok(|(st, _)| unix_stream::UnixStream::new(st)).await;
                yield item;
            }
        };
        tokio::spawn(async move {
            Server::builder()
                .add_service(RegistrationServer::new(FakeKubelet { registrations }))
                .serve_with_incoming_shutdown(incoming, async {
                    let _ = stopped.await;
                })
                .await
                .unwrap();
        });
        stop
    }

    async fn next_registration(
        registrations: &mut mpsc::UnboundedReceiver<RegisterRequest>,
    ) -> RegisterRequest {
        tokio::time::timeout(Duration::from_secs(5), registrations.recv())
            .await
            .expect("no registration within 5 s")
            .unwrap()
    }

    #[tokio::test]
    async fn test_register_again_after_kubelet_restart() {
        let _ = env_logger::builder().is_test(true).try_init();
        let dir = tempfile::tempdir().unwrap();
        let kubelet_socket = dir.path().join("kubelet.sock");
        let (registrations_sender, mut registrations) = mpsc::unbounded_channel();
        let kubelet = start_fake_kubelet(&kubelet_socket, registrations_sender.clone());

        let plugin = Arc::new(TestPlugin {
            stopper: Stopper::new(),
        });
        serve_and_register_plugin_at(
            plugin.clone(),
            dir.path(),
            &kubelet_socket,
            Duration::from_millis(100),
        )
        .await
        .unwrap();
        let first = next_registration(&mut registrations).await;
        assert_eq!(first.resource_name, "akri.sh/test-plugin");
        assert!(dir.path().join(&first.endpoint).exists());

        // Kubelet restart: the kubelet stops, and the new one deletes every socket in the folder.
        let _ = kubelet.send(());
        for entry in std::fs::read_dir(dir.path()).unwrap() {
            std::fs::remove_file(entry.unwrap().path()).unwrap();
        }
        // No kubelet listens: the plugin retries and does not register.
        tokio::time::sleep(Duration::from_millis(350)).await;
        assert!(registrations.try_recv().is_err());
        let _kubelet = start_fake_kubelet(&kubelet_socket, registrations_sender);

        let second = next_registration(&mut registrations).await;
        assert_eq!(second.resource_name, "akri.sh/test-plugin");
        assert_ne!(second.endpoint, first.endpoint);
        let new_socket = dir.path().join(&second.endpoint);
        assert!(new_socket.exists());

        // The new socket serves the plugin.
        let connect_socket = new_socket.clone();
        let channel = Endpoint::try_from("http://[::1]:50051")
            .unwrap()
            .connect_with_connector(service_fn(move |_: Uri| {
                let connect_socket = connect_socket.clone();
                async move {
                    UnixStream::connect(connect_socket)
                        .await
                        .map(hyper_util::rt::TokioIo::new)
                }
            }))
            .await
            .unwrap();
        let mut client = DevicePluginClient::new(channel);
        assert!(client.get_device_plugin_options(Empty {}).await.is_ok());
        drop(client);

        // A stopped plugin deletes its socket and does not register again.
        plugin.stop();
        tokio::time::sleep(Duration::from_millis(350)).await;
        assert!(!new_socket.exists());
        assert!(registrations.try_recv().is_err());
    }
}
