use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use akri_shared::{
    akri::{
        configuration::{Configuration, DiscoveryProperty},
        instance::Instance,
    },
    k8s::api::IntoApi,
};
use futures::StreamExt;
use tokio::sync::mpsc;

use crate::discovery_handler_manager::{
    DiscoveryError, discovery_handler_registry::DiscoveryHandlerRegistry,
};

use kube::api::ObjectMeta;
use kube::runtime::{
    Controller,
    controller::Action,
    reflector::{ObjectRef, Store},
};
use kube::{Resource, ResourceExt};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    DiscoveryError(#[from] DiscoveryError),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

const SUCCESS_REQUEUE: Duration = Duration::from_secs(600);

pub trait DiscoveryConfigurationKubeClient: IntoApi<Configuration> + IntoApi<Instance> {}

impl<T: IntoApi<Configuration> + IntoApi<Instance>> DiscoveryConfigurationKubeClient for T {}

pub struct ControllerContext {
    pub instances_cache: Store<Instance>,
    pub dh_registry: Arc<dyn DiscoveryHandlerRegistry>,
    pub client: Arc<dyn DiscoveryConfigurationKubeClient>,
    pub agent_identifier: String,
    pub error_backoffs: Mutex<HashMap<String, Duration>>,
}

/// This function starts the reconciling loop for the Configuration controller.
/// It is expected to run this as a task.
pub async fn start_controller(
    ctx: Arc<ControllerContext>,
    rec: mpsc::Receiver<ObjectRef<Configuration>>,
) {
    let api = ctx.client.all().as_inner();
    let controller = Controller::new(api, Default::default());

    controller
        // Reconcile the Configuration when the discovery handler manager signals a change
        .reconcile_on(tokio_stream::wrappers::ReceiverStream::new(rec))
        .run(reconcile, error_policy, ctx)
        .for_each(|_| futures::future::ready(()))
        .await;
}

/// This function is the main Reconcile function for Configurations resources
/// This will get called every time a Configuration gets added or is changed, it will also be called
/// for every existing configuration on startup.
/// We also set-up discovery manager to trigger reconciliation upon discovery state change
///
/// Here the function will (in order):
///  - Check if Configuration awaits deletion, and if so terminate pending discovery, remove finalizer and return early
///  - Add finalizer if not here already
///  - Start discovery if not already started
///  - Get discovery results (empty list if just started)
///  - Create Instances according to discovery results
///  - Delete Instances that are not in the discovery results, only once every discovery handler answered
pub async fn reconcile(
    dc: Arc<Configuration>,
    ctx: Arc<ControllerContext>,
) -> Result<Action, Error> {
    trace!("Reconciling {:?}::{}", dc.namespace(), dc.name_any());
    let namespace = dc.namespace().unwrap();
    let owner_ref = dc.controller_owner_ref(&()).unwrap();
    if dc.metadata.deletion_timestamp.is_some() {
        ctx.dh_registry.terminate_request(&dc.name_any()).await;

        ctx.client
            .namespaced(&namespace)
            .remove_finalizer(dc.as_ref(), &ctx.agent_identifier)
            .await
            .map_err(|e| Error::Other(e.into()))?;

        return Ok(Action::await_change());
    }

    if !dc.finalizers().contains(&ctx.agent_identifier) {
        ctx.client
            .namespaced(&namespace)
            .add_finalizer(dc.as_ref(), &ctx.agent_identifier)
            .await
            .map_err(|e| Error::Other(e.into()))?
    }

    let dh_name = &dc.spec.discovery_handler.name;
    let dh_details = &dc.spec.discovery_handler.discovery_details;
    let dh_properties: &[DiscoveryProperty] = dc
        .spec
        .discovery_handler
        .discovery_properties
        .as_deref()
        .unwrap_or_default();
    let dh_extra_device_properties = dc.spec.broker_properties.clone();

    let (discovered_instances, discovery_answered): (Vec<Instance>, bool) =
        match ctx.dh_registry.get_request(&dc.name_any()).await {
            Some(req) => {
                req.set_extra_device_properties(dc.spec.broker_properties.clone())
                    .await;
                // Read this before the Instances: an answer that arrives in between only adds devices.
                let answered = req.all_endpoints_answered().await;
                let instances = req
                    .get_instances()
                    .await?
                    .into_iter()
                    .map(|mut instance| {
                        // Add
                        instance.spec.nodes = vec![ctx.agent_identifier.to_owned()];
                        instance.owner_references_mut().push(owner_ref.clone());
                        instance.spec.capacity = dc.spec.capacity;
                        instance
                    })
                    .collect();
                (instances, answered)
            }
            None => {
                ctx.dh_registry
                    .new_request(
                        &dc.name_any(),
                        dh_name,
                        dh_details,
                        dh_properties,
                        dh_extra_device_properties,
                        &dc.namespace().unwrap_or("default".to_string()),
                    )
                    .await?;
                (vec![], false)
            }
        };

    // A new request (agent or discovery handler restart) has no device list yet. Deleting now would
    // delete every Instance of this node and recycle its workloads (upstream #853).
    if discovery_answered {
        for instance in ctx.instances_cache.state() {
            if instance.owner_references().contains(&owner_ref)
                && !discovered_instances
                    .iter()
                    .any(|di| di.name_any() == instance.name_any())
            {
                delete_instance(
                    ctx.client.as_ref(),
                    instance.as_ref(),
                    &ctx.agent_identifier,
                )
                .await?
            }
        }
    } else {
        info!(
            "Configuration {}::{}: keep existing Instances until every discovery handler answered",
            namespace,
            dc.name_any()
        );
    }

    for instance in discovered_instances {
        ctx.client
            .namespaced(&namespace)
            .apply(instance, &ctx.agent_identifier)
            .await
            .map_err(|e| Error::Other(e.into()))?;
    }

    ctx.error_backoffs.lock().unwrap().remove(&dc.name_any());
    Ok(Action::requeue(SUCCESS_REQUEUE))
}

pub fn error_policy(dc: Arc<Configuration>, error: &Error, ctx: Arc<ControllerContext>) -> Action {
    let mut error_backoffs = ctx.error_backoffs.lock().unwrap();
    let previous_duration = error_backoffs
        .get(&dc.name_any())
        .cloned()
        .unwrap_or(Duration::from_millis(500));
    let next_duration = previous_duration * 2;
    warn!(
        "Error during reconciliation for {:?}::{}, retrying in {}s: {:?}",
        dc.namespace(),
        dc.name_any(),
        next_duration.as_secs_f32(),
        error
    );
    error_backoffs.insert(dc.name_any(), next_duration);
    Action::requeue(next_duration)
}

async fn delete_instance(
    client: &dyn DiscoveryConfigurationKubeClient,
    instance: &Instance,
    agent_instance_name: &String,
) -> Result<(), Error> {
    if instance.spec.nodes.contains(agent_instance_name) {
        let api = client.namespaced(&instance.namespace().unwrap());
        if instance.spec.nodes.len() == 1 {
            api.delete(&instance.name_any())
                .await
                .map_err(|e| Error::Other(e.into()))?;
            return Ok(());
        }
        // Apply a clean copy of the instance, cleaning the nodes
        // and keeping the necessary metadata fields.
        let mut new_instance = Instance {
            metadata: ObjectMeta {
                name: instance.metadata.name.clone(),
                namespace: instance.metadata.namespace.clone(),
                ..Default::default()
            },
            spec: instance.spec.clone(),
        };
        new_instance.spec.nodes = vec![];
        api.apply(new_instance, agent_instance_name)
            .await
            .map_err(|e| Error::Other(e.into()))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use akri_shared::{
        akri::{
            configuration::{ConfigurationSpec, DiscoveryHandlerInfo},
            instance::InstanceSpec,
        },
        k8s::api::{Api, MockApi, MockIntoApi},
    };
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
    use kube::core::{ObjectMeta, Status};
    use mockall::predicate::eq;

    use crate::discovery_handler_manager::discovery_handler_registry::{
        MockDiscoveryHandlerRegistry, MockDiscoveryHandlerRequest,
    };

    use super::*;

    #[derive(Default)]
    pub struct MockDiscoveryConfigurationKubeClient {
        instance: MockIntoApi<Instance>,
        config: MockIntoApi<Configuration>,
    }

    impl IntoApi<Instance> for MockDiscoveryConfigurationKubeClient {
        fn all(&self) -> Box<dyn Api<Instance>> {
            self.instance.all()
        }

        fn namespaced(&self, namespace: &str) -> Box<dyn Api<Instance>> {
            self.instance.namespaced(namespace)
        }

        fn default_namespaced(&self) -> Box<dyn Api<Instance>> {
            self.instance.default_namespaced()
        }
    }

    impl IntoApi<Configuration> for MockDiscoveryConfigurationKubeClient {
        fn all(&self) -> Box<dyn Api<Configuration>> {
            self.config.all()
        }

        fn namespaced(&self, namespace: &str) -> Box<dyn Api<Configuration>> {
            self.config.namespaced(namespace)
        }

        fn default_namespaced(&self) -> Box<dyn Api<Configuration>> {
            self.config.default_namespaced()
        }
    }

    #[test]
    fn test_error_policy() {
        let _ = env_logger::builder().is_test(true).try_init();
        let config_1 = Arc::new(Configuration {
            metadata: ObjectMeta {
                name: Some("config-1".to_string()),
                ..Default::default()
            },
            spec: ConfigurationSpec {
                discovery_handler: DiscoveryHandlerInfo {
                    name: "debugEcho".to_string(),
                    discovery_details: String::default(),
                    discovery_properties: None,
                },
                capacity: 1,
                broker_spec: None,
                instance_service_spec: None,
                configuration_service_spec: None,
                broker_properties: Default::default(),
            },
        });
        let config_2 = Arc::new(Configuration {
            metadata: ObjectMeta {
                name: Some("config-2".to_string()),
                ..Default::default()
            },
            spec: ConfigurationSpec {
                discovery_handler: DiscoveryHandlerInfo {
                    name: "debugEcho".to_string(),
                    discovery_details: String::default(),
                    discovery_properties: None,
                },
                capacity: 1,
                broker_spec: None,
                instance_service_spec: None,
                configuration_service_spec: None,
                broker_properties: Default::default(),
            },
        });

        let (store, _) = kube::runtime::reflector::store();

        let ctx = Arc::new(ControllerContext {
            instances_cache: store,
            dh_registry: Arc::new(MockDiscoveryHandlerRegistry::new()),
            client: Arc::new(MockDiscoveryConfigurationKubeClient::default()),
            agent_identifier: "node-a".to_string(),
            error_backoffs: Default::default(),
        });

        assert_eq!(
            error_policy(
                config_1.clone(),
                &Error::Other(anyhow::anyhow!("Error")),
                ctx.clone()
            ),
            Action::requeue(Duration::from_secs(1))
        );
        assert_eq!(
            error_policy(
                config_1.clone(),
                &Error::Other(anyhow::anyhow!("Error")),
                ctx.clone()
            ),
            Action::requeue(Duration::from_secs(2))
        );
        assert_eq!(
            error_policy(
                config_1.clone(),
                &Error::Other(anyhow::anyhow!("Error")),
                ctx.clone()
            ),
            Action::requeue(Duration::from_secs(4))
        );

        assert_eq!(
            error_policy(
                config_2,
                &Error::Other(anyhow::anyhow!("Error")),
                ctx.clone()
            ),
            Action::requeue(Duration::from_secs(1))
        );

        assert_eq!(
            error_policy(config_1, &Error::Other(anyhow::anyhow!("Error")), ctx),
            Action::requeue(Duration::from_secs(8))
        );
    }

    #[tokio::test]
    async fn test_delete_instance_delete() {
        let instance = Instance {
            metadata: ObjectMeta {
                name: Some("instance-1".to_string()),
                namespace: Some("namespace-a".to_string()),
                ..Default::default()
            },
            spec: InstanceSpec {
                capacity: 1,
                configuration_name: Default::default(),
                cdi_name: Default::default(),
                broker_properties: Default::default(),
                shared: false,
                nodes: vec!["node-a".to_string()],
                device_usage: Default::default(),
            },
        };

        let mut mock_client = MockDiscoveryConfigurationKubeClient::default();
        let mut mock_api = MockApi::new();
        let local_instance = instance.clone();
        mock_api
            .expect_delete()
            .with(eq("instance-1"))
            .returning(move |_| Ok(itertools::Either::Left(local_instance.clone())));
        mock_client
            .instance
            .expect_namespaced()
            .with(eq("namespace-a"))
            .return_once(|_| Box::new(mock_api));

        assert!(
            delete_instance(&mock_client, &instance, &"node-a".to_string())
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn test_delete_instance_remove_node() {
        let instance = Instance {
            metadata: ObjectMeta {
                name: Some("instance-1".to_string()),
                namespace: Some("namespace-a".to_string()),
                ..Default::default()
            },
            spec: InstanceSpec {
                capacity: 1,
                configuration_name: Default::default(),
                cdi_name: Default::default(),
                broker_properties: Default::default(),
                shared: false,
                nodes: vec!["node-a".to_string(), "node-b".to_string()],
                device_usage: Default::default(),
            },
        };

        let mut mock_client = MockDiscoveryConfigurationKubeClient::default();
        let mut mock_api = MockApi::new();
        let local_instance = instance.clone();
        mock_api
            .expect_apply()
            .returning(move |_, _| Ok(local_instance.clone()));
        mock_client
            .instance
            .expect_namespaced()
            .with(eq("namespace-a"))
            .return_once(|_| Box::new(mock_api));

        assert!(
            delete_instance(&mock_client, &instance, &"node-a".to_string())
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn test_delete_instance_other_node() {
        let instance = Instance {
            metadata: ObjectMeta {
                name: Some("instance-1".to_string()),
                namespace: Some("namespace-a".to_string()),
                ..Default::default()
            },
            spec: InstanceSpec {
                capacity: 1,
                configuration_name: Default::default(),
                cdi_name: Default::default(),
                broker_properties: Default::default(),
                shared: false,
                nodes: vec!["node-b".to_string()],
                device_usage: Default::default(),
            },
        };

        let mut mock_client = MockDiscoveryConfigurationKubeClient::default();
        let mock_api = MockApi::new();
        mock_client
            .instance
            .expect_namespaced()
            .with(eq("namespace-a"))
            .return_once(|_| Box::new(mock_api));

        assert!(
            delete_instance(&mock_client, &instance, &"node-a".to_string())
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn test_reconcile_nothing_to_do() {
        let (store, _) = kube::runtime::reflector::store();
        let mut client = MockDiscoveryConfigurationKubeClient::default();
        let api = MockApi::new();
        client
            .config
            .expect_namespaced()
            .return_once(|_| Box::new(api));

        let mut registry = MockDiscoveryHandlerRegistry::new();
        let mut request = MockDiscoveryHandlerRequest::new();
        request
            .expect_set_extra_device_properties()
            .returning(|_| {});
        request.expect_all_endpoints_answered().returning(|| true);
        request.expect_get_instances().returning(|| Ok(vec![]));
        registry
            .expect_get_request()
            .return_once(|_| Some(Arc::new(request)));

        let ctx = Arc::new(ControllerContext {
            instances_cache: store,
            dh_registry: Arc::new(registry),
            client: Arc::new(client),
            agent_identifier: "node-a".to_string(),
            error_backoffs: Default::default(),
        });

        let dc = Arc::new(Configuration {
            metadata: ObjectMeta {
                name: Some("config-1".to_string()),
                namespace: Some("namespace-a".to_string()),
                uid: Some("00112233-4455-6677-8899-aabbccddeeff".to_string()),
                finalizers: Some(vec!["node-a".to_string()]),
                ..Default::default()
            },
            spec: ConfigurationSpec {
                discovery_handler: DiscoveryHandlerInfo {
                    name: "debugEcho".to_string(),
                    discovery_details: String::new(),
                    discovery_properties: None,
                },
                capacity: 1,
                broker_spec: None,
                instance_service_spec: None,
                configuration_service_spec: None,
                broker_properties: Default::default(),
            },
        });

        assert!(reconcile(dc, ctx).await.is_ok());
    }

    fn test_instance(name: &str, config: &str, config_uid: &str, node: &str) -> Instance {
        Instance {
            metadata: ObjectMeta {
                namespace: Some("namespace-a".to_string()),
                name: Some(name.to_string()),
                owner_references: Some(vec![OwnerReference {
                    api_version: Instance::api_version(&()).to_string(),
                    block_owner_deletion: None,
                    controller: Some(true),
                    kind: "Configuration".to_string(),
                    name: config.to_string(),
                    uid: config_uid.to_string(),
                }]),
                ..Default::default()
            },
            spec: InstanceSpec {
                configuration_name: config.to_string(),
                cdi_name: format!("akri.sh/{config}=abcdef"),
                capacity: 1,
                broker_properties: HashMap::new(),
                shared: true,
                nodes: vec![node.to_string()],
                device_usage: Default::default(),
            },
        }
    }

    /// instance-1: config-1 on node-a, instance-2: config-1 on node-b, instance-3: config-2 on node-a.
    fn test_instances_cache() -> Store<Instance> {
        let (store, mut writer) = kube::runtime::reflector::store();
        writer.apply_watcher_event(&kube::runtime::watcher::Event::Init);
        for instance in [
            test_instance("instance-1", "config-1", CONFIG_1_UID, "node-a"),
            test_instance("instance-2", "config-1", CONFIG_1_UID, "node-b"),
            test_instance(
                "instance-3",
                "config-2",
                "11112233-4455-6677-8899-aabbccddeeff",
                "node-a",
            ),
        ] {
            writer.apply_watcher_event(&kube::runtime::watcher::Event::InitApply(instance));
        }
        writer.apply_watcher_event(&kube::runtime::watcher::Event::InitDone);
        store
    }

    const CONFIG_1_UID: &str = "00112233-4455-6677-8899-aabbccddeeff";

    fn test_config_1(finalizers: Option<Vec<String>>) -> Arc<Configuration> {
        Arc::new(Configuration {
            metadata: ObjectMeta {
                name: Some("config-1".to_string()),
                namespace: Some("namespace-a".to_string()),
                uid: Some(CONFIG_1_UID.to_string()),
                finalizers,
                ..Default::default()
            },
            spec: ConfigurationSpec {
                discovery_handler: DiscoveryHandlerInfo {
                    name: "debugEcho".to_string(),
                    discovery_details: String::new(),
                    discovery_properties: None,
                },
                capacity: 1,
                broker_spec: None,
                instance_service_spec: None,
                configuration_service_spec: None,
                broker_properties: Default::default(),
            },
        })
    }

    /// The first reconcile after an agent start creates the discovery request and must not
    /// delete the node's Instances (upstream #853).
    #[tokio::test]
    async fn test_reconcile_no_request_keeps_instances() {
        let mut client = MockDiscoveryConfigurationKubeClient::default();
        let mut api = MockApi::new();
        api.expect_add_finalizer().returning(|_, _| Ok(()));
        client
            .config
            .expect_namespaced()
            .return_once(|_| Box::new(api));
        // No expectation on the Instance API: any delete or apply fails the test.

        let mut registry = MockDiscoveryHandlerRegistry::new();
        registry.expect_get_request().return_once(|_| None);
        registry
            .expect_new_request()
            .returning(|_, _, _, _, _, _| Ok(()));

        let ctx = Arc::new(ControllerContext {
            instances_cache: test_instances_cache(),
            dh_registry: Arc::new(registry),
            client: Arc::new(client),
            agent_identifier: "node-a".to_string(),
            error_backoffs: Default::default(),
        });

        assert!(reconcile(test_config_1(None), ctx).await.is_ok());
    }

    /// A reconcile before every discovery handler answered applies what is known and deletes nothing.
    #[tokio::test]
    async fn test_reconcile_unanswered_request_keeps_instances() {
        let mut client = MockDiscoveryConfigurationKubeClient::default();
        let mut instance_api = MockApi::new();
        instance_api
            .expect_apply()
            .withf(|instance: &Instance, _: &str| instance.name_any() == "config-1-discovered")
            .times(1)
            .returning(|instance, _| Ok(instance));
        client
            .instance
            .expect_namespaced()
            .with(eq("namespace-a"))
            .return_once(|_| Box::new(instance_api));

        let mut request = MockDiscoveryHandlerRequest::new();
        request
            .expect_set_extra_device_properties()
            .returning(|_| {});
        request.expect_all_endpoints_answered().returning(|| false);
        request.expect_get_instances().returning(|| {
            Ok(vec![Instance {
                metadata: ObjectMeta {
                    name: Some("config-1-discovered".to_string()),
                    ..Default::default()
                },
                spec: InstanceSpec {
                    configuration_name: "config-1".to_string(),
                    cdi_name: "akri.sh/config-1=discovered".to_string(),
                    capacity: 0,
                    broker_properties: HashMap::new(),
                    shared: false,
                    nodes: vec![],
                    device_usage: Default::default(),
                },
            }])
        });
        let mut registry = MockDiscoveryHandlerRegistry::new();
        registry
            .expect_get_request()
            .return_once(|_| Some(Arc::new(request)));

        let ctx = Arc::new(ControllerContext {
            instances_cache: test_instances_cache(),
            dh_registry: Arc::new(registry),
            client: Arc::new(client),
            agent_identifier: "node-a".to_string(),
            error_backoffs: Default::default(),
        });

        assert!(
            reconcile(test_config_1(Some(vec!["node-a".to_string()])), ctx)
                .await
                .is_ok()
        );
    }

    /// Once every discovery handler answered, Instances of this node that were not discovered are deleted.
    #[tokio::test]
    async fn test_reconcile_answered_request_deletes_missing_instances() {
        let mut client = MockDiscoveryConfigurationKubeClient::default();
        let mut instance_api = MockApi::new();
        instance_api
            .expect_delete()
            .with(eq("instance-1"))
            .times(1)
            .returning(|_| Ok(itertools::Either::Right(Status::default())));
        client
            .instance
            .expect_namespaced()
            .with(eq("namespace-a"))
            .return_once(|_| Box::new(instance_api));

        let mut request = MockDiscoveryHandlerRequest::new();
        request
            .expect_set_extra_device_properties()
            .returning(|_| {});
        request.expect_all_endpoints_answered().returning(|| true);
        request.expect_get_instances().returning(|| Ok(vec![]));
        let mut registry = MockDiscoveryHandlerRegistry::new();
        registry
            .expect_get_request()
            .return_once(|_| Some(Arc::new(request)));

        let ctx = Arc::new(ControllerContext {
            instances_cache: test_instances_cache(),
            dh_registry: Arc::new(registry),
            client: Arc::new(client),
            agent_identifier: "node-a".to_string(),
            error_backoffs: Default::default(),
        });

        assert!(
            reconcile(test_config_1(Some(vec!["node-a".to_string()])), ctx)
                .await
                .is_ok()
        );
    }
}
