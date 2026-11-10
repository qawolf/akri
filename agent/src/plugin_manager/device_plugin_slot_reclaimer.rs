use std::{
    collections::{HashMap, HashSet},
    convert::TryFrom,
    sync::Arc,
    time::{Duration, Instant},
};

use tokio::net::UnixStream;
use tonic::transport::{Endpoint, Uri};
use tower::service_fn;

use crate::plugin_manager::{
    device_plugin_instance_controller::DP_SLOT_PREFIX, v1::ListPodResourcesRequest,
};

use super::{
    device_plugin_instance_controller::DevicePluginManager,
    v1::pod_resources_lister_client as podresources,
};

/// Path of the Kubelet registry socket
pub const KUBELET_SOCKET: &str = "/var/lib/kubelet/pod-resources/kubelet.sock";
const SLOT_GRACE_PERIOD: Duration = Duration::from_secs(20);
const SLOT_RECLAIM_INTERVAL: Duration = Duration::from_secs(10);

/// This function connects to kubelet's resource monitoring interface and extracts
/// the set of resources currently used by pods on the node.
/// It uses this Kubelet interface:
///  <https://kubernetes.io/docs/concepts/extend-kubernetes/compute-storage-net/device-plugins/#grpc-endpoint-list>
async fn get_used_slots() -> Result<HashSet<String>, anyhow::Error> {
    // We will ignore this dummy uri because UDS does not use it.
    // Some servers will check the uri content so the uri needs to
    // be in valid format even it's not used, the scheme part is used
    // to specific what scheme to use, such as http or https
    let kubelet_socket_closure = KUBELET_SOCKET.to_string();
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
        .await?;
    let mut podresources_client = podresources::PodResourcesListerClient::new(channel);

    let list_request = tonic::Request::new(ListPodResourcesRequest {});
    trace!("register - before call to register with the kubelet at socket {KUBELET_SOCKET}");

    // Get the list of allocated device ids from kubelet
    let resources = podresources_client
        .list(list_request)
        .await?
        .into_inner()
        .pod_resources
        .into_iter()
        .flat_map(|pr| {
            pr.containers.into_iter().flat_map(|cr| {
                cr.devices.into_iter().flat_map(|cd| {
                    if cd.resource_name.starts_with(DP_SLOT_PREFIX) {
                        cd.device_ids
                    } else {
                        vec![]
                    }
                })
            })
        })
        .collect();

    Ok(resources)
}

/// One reclaim pass. A slot that this agent holds but no pod uses (the kubelet does not list it) is stalled.
/// Returns the slots stalled for at least `SLOT_GRACE_PERIOD` (with their start time), and the other stalled
/// slots. Both sets use the kubelet device ids: `<instance>-<i>` and `<config>-<i>`.
fn find_slots_to_free(
    theoretical_slots: &HashSet<String>,
    used_slots: &HashSet<String>,
    stalled_slots: &HashMap<String, Instant>,
    now: Instant,
) -> (Vec<(String, Instant)>, HashMap<String, Instant>) {
    let mut to_free = Vec::new();
    let mut still_stalled = HashMap::new();
    for slot in theoretical_slots.difference(used_slots) {
        match stalled_slots.get(slot) {
            Some(at) if now.saturating_duration_since(*at) >= SLOT_GRACE_PERIOD => {
                to_free.push((slot.to_string(), *at))
            }
            Some(at) => {
                still_stalled.insert(slot.to_string(), *at);
            }
            None => {
                still_stalled.insert(slot.to_string(), now);
            }
        }
    }
    (to_free, still_stalled)
}

pub async fn start_reclaimer(dp_manager: Arc<DevicePluginManager>) {
    let mut stalled_slots: HashMap<String, Instant> = HashMap::new();
    loop {
        trace!("reclaiming unused slots - start");
        if let Ok(used_slots) = get_used_slots().await {
            let theoretical_slots = dp_manager.get_used_slots().await;
            let (to_free, mut new_stalled_slots) = find_slots_to_free(
                &theoretical_slots,
                &used_slots,
                &stalled_slots,
                Instant::now(),
            );
            for (slot_to_reclaim, at) in to_free {
                if dp_manager.free_slot(slot_to_reclaim.clone()).await.is_err() {
                    warn!(
                        "Failed to free slot {}, will try again in {}s",
                        slot_to_reclaim,
                        SLOT_RECLAIM_INTERVAL.as_secs()
                    );
                    // To try again we just keep the slot as stalled
                    new_stalled_slots.insert(slot_to_reclaim, at);
                } else {
                    info!(
                        "Freed slot {slot_to_reclaim}: no pod used it for {}s",
                        SLOT_GRACE_PERIOD.as_secs()
                    );
                }
            }
            stalled_slots = new_stalled_slots;
        }
        tokio::select! {
            _ = tokio::time::sleep(SLOT_RECLAIM_INTERVAL) => {},
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ids(v: &[&str]) -> HashSet<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn test_used_slot_is_never_freed() {
        // Kubelet-style ids on both sides: a pod uses each slot.
        let theoretical = ids(&["instance-a-0", "config-a-1"]);
        let used = ids(&["instance-a-0", "config-a-1", "other-b-0"]);
        let start = Instant::now();
        let mut stalled = HashMap::new();
        for pass in 0..5u32 {
            let (to_free, s) = find_slots_to_free(
                &theoretical,
                &used,
                &stalled,
                start + SLOT_RECLAIM_INTERVAL * pass,
            );
            assert!(to_free.is_empty());
            assert!(s.is_empty());
            stalled = s;
        }
    }

    #[test]
    fn test_leaked_slot_is_freed_after_grace_period() {
        let theoretical = ids(&["instance-a-0", "instance-b-0"]);
        let used = ids(&["instance-b-0"]);
        let start = Instant::now();

        let (to_free, stalled) = find_slots_to_free(&theoretical, &used, &HashMap::new(), start);
        assert!(to_free.is_empty());
        assert_eq!(stalled, HashMap::from([("instance-a-0".to_owned(), start)]));

        let (to_free, stalled) =
            find_slots_to_free(&theoretical, &used, &stalled, start + SLOT_RECLAIM_INTERVAL);
        assert!(to_free.is_empty());
        assert_eq!(stalled.len(), 1);

        let (to_free, stalled) =
            find_slots_to_free(&theoretical, &used, &stalled, start + SLOT_GRACE_PERIOD);
        assert_eq!(to_free, vec![("instance-a-0".to_owned(), start)]);
        assert!(stalled.is_empty());
    }

    #[test]
    fn test_slot_used_again_is_no_longer_stalled() {
        let theoretical = ids(&["instance-a-0"]);
        let start = Instant::now();
        let (_, stalled) = find_slots_to_free(&theoretical, &ids(&[]), &HashMap::new(), start);
        assert_eq!(stalled.len(), 1);
        // A new pod got the slot before the grace period ended.
        let (to_free, stalled) = find_slots_to_free(
            &theoretical,
            &ids(&["instance-a-0"]),
            &stalled,
            start + SLOT_GRACE_PERIOD,
        );
        assert!(to_free.is_empty());
        assert!(stalled.is_empty());
    }
}
