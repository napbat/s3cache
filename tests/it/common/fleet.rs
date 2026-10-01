//! Fleet harness shared by the peer-bootstrap suites: origin request tallies,
//! the explicit fleet address book, and nodes on runtimes of their own that a
//! test can stop like a pod.

use std::net::TcpListener;
use std::sync::Arc;

use futures::StreamExt;
use groupnet::consistency::volatile_recovery::{RecoveryConfig, RecoveryStage};
use s3cache::cache::proxy::CachingProxy;
use s3cache::index::ScanConfig;
use s3cache::metrics::Metrics;
use s3cache::sync::coherence::{Consistency, DEFAULT_LEASE_MS, WriteSync};
use s3cache::sync::fleet::config::FleetConfig;

use super::{Origin, gossip_node, proxy_over_with_metrics};

/// The per-object body cap every fleet node runs with.
pub const CAP: usize = 1024 * 1024;

/// Flat keys with one serial LIST chain: the scan is exactly `ceil(rows / 1000)`
/// LIST pages, with no discovery requests, so a restarted scan is countable.
pub const SERIAL_SCAN: ScanConfig = ScanConfig {
    workers: 1,
    discovery_budget: 0,
};

/// Origin requests by kind, as the counting forwarder saw them.
#[derive(Clone, Copy, Debug)]
pub struct Counts {
    pub list: u64,
    pub get: u64,
    pub head: u64,
    pub successful_list: u64,
    pub successful_get: u64,
    pub successful_head: u64,
    pub put: u64,
    pub copy: u64,
    pub delete: u64,
}

impl Counts {
    pub fn take(origin: &Origin) -> Self {
        Self {
            list: origin.ops.list(),
            get: origin.ops.get(),
            head: origin.ops.head(),
            successful_list: origin.ops.successful_list(),
            successful_get: origin.ops.successful_get(),
            successful_head: origin.ops.successful_head(),
            put: origin.ops.put(),
            copy: origin.ops.copy(),
            delete: origin.ops.delete(),
        }
    }

    pub fn since(self, before: Self) -> Self {
        Self {
            list: self.list - before.list,
            get: self.get - before.get,
            head: self.head - before.head,
            successful_list: self.successful_list - before.successful_list,
            successful_get: self.successful_get - before.successful_get,
            successful_head: self.successful_head - before.successful_head,
            put: self.put - before.put,
            copy: self.copy - before.copy,
            delete: self.delete - before.delete,
        }
    }

    pub fn assert_no_writes(self) {
        assert_eq!(self.put, 0, "fleet bootstrap wrote an origin object");
        assert_eq!(self.copy, 0, "fleet bootstrap copied an origin object");
        assert_eq!(self.delete, 0, "fleet bootstrap deleted an origin object");
    }
}

pub fn free_tcp_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("test TCP port")
        .local_addr()
        .expect("bound TCP port")
        .port()
}

/// Seed `rows` flat keys straight into `MinIO`, concurrently and uncounted.
pub async fn seed_rows(origin: &Origin, rows: usize) {
    futures::stream::iter(0..rows)
        .map(|n| async move { origin.seed(&format!("row-{n:07}"), b"x").await })
        .buffer_unordered(64)
        .collect::<()>()
        .await;
}

pub fn fleet_config(
    origin: &Origin,
    bucket: &str,
    name: &str,
    ports: &[(&str, u16)],
) -> FleetConfig {
    let port = ports
        .iter()
        .find_map(|(peer, port)| (*peer == name).then_some(*port))
        .expect("node in the complete fleet address book");
    fleet_config_bound(origin, bucket, name, port, ports)
}

/// `name` listens on `bind` but peers dial it at its `book` address, which a
/// test relay may own.
pub fn fleet_config_bound(
    origin: &Origin,
    bucket: &str,
    name: &str,
    bind: u16,
    book: &[(&str, u16)],
) -> FleetConfig {
    let advertise = book
        .iter()
        .find_map(|(peer, port)| (*peer == name).then(|| format!("127.0.0.1:{port}")))
        .expect("node in the complete fleet address book");
    let book = book
        .iter()
        .map(|(peer, port)| format!("{peer}=127.0.0.1:{port}"))
        .collect::<Vec<_>>()
        .join(",");
    FleetConfig::parse(
        Some(format!("127.0.0.1:{bind}")),
        Some(advertise),
        Some(book),
        Some("minio-fleet-cost-fixture".to_owned()),
        origin.counted_endpoint(),
        "us-east-1",
        name,
        &[bucket.to_owned()],
    )
    .expect("valid explicit fleet configuration")
    .expect("fleet opted in")
}

/// Whether `proxy`'s current recovery gate and its index for `bucket` serve
/// locally.
pub fn serves_locally(proxy: &CachingProxy, bucket: &str) -> bool {
    proxy
        .recovery_status()
        .is_some_and(|status| status.state.stage == RecoveryStage::Ready && status.may_serve)
        && proxy.initially_ready(&[bucket.to_owned()])
}

/// One node on a runtime of its own with one worker thread, as the binary's
/// `#[tokio::main]` runs in a pod limited to one CPU. Dropping it stops every
/// task the node spawned, as a killed pod does.
pub struct Pod(Option<tokio::runtime::Runtime>);

impl Pod {
    pub fn new(name: &str) -> Self {
        Self(Some(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .thread_name(name)
                .enable_all()
                .build()
                .expect("pod runtime"),
        ))
    }

    pub fn handle(&self) -> tokio::runtime::Handle {
        self.0.as_ref().expect("live pod runtime").handle().clone()
    }

    /// Hold the pod's only worker for `pause`: its timers, gossip and
    /// bootstrap worker all run late, as on a throttled, overloaded node.
    pub fn stall(&self, pause: std::time::Duration) {
        drop(
            self.handle()
                .spawn(async move { std::thread::sleep(pause) }),
        );
    }
}

impl Drop for Pod {
    /// Returns once every task is dropped and its sockets are closed, as a
    /// killed process's are before its replacement starts. The shutdown runs
    /// on a thread of its own: a runtime cannot block inside another's task.
    fn drop(&mut self) {
        if let Some(runtime) = self.0.take() {
            std::thread::spawn(move || {
                runtime.shutdown_timeout(std::time::Duration::from_secs(10));
            })
            .join()
            .expect("pod shutdown");
        }
    }
}

/// Build one fleet node on `pod`, so every task it spawns runs there. It
/// listens for bulk transfers on `bind`; peers dial it at its `book` address.
/// `None` keeps the binary's own recovery configuration.
pub async fn pod_node(
    pod: &Pod,
    origin: &Arc<Origin>,
    (name, udp, bind): (&str, u16, u16),
    (peer, peer_udp): (&str, u16),
    book: [(&'static str, u16); 2],
    metrics: &Arc<Metrics>,
    recovery: Option<RecoveryConfig>,
) -> (Arc<WriteSync>, CachingProxy) {
    let (origin, metrics) = (Arc::clone(origin), Arc::clone(metrics));
    let (name, peer) = (name.to_owned(), peer.to_owned());
    pod.handle()
        .spawn(async move {
            let sync = gossip_node(&name, udp, &[(peer.as_str(), peer_udp)]).await;
            let proxy = proxy_over_with_metrics(
                &origin.counted_client(),
                CAP,
                Some(Arc::clone(&sync)),
                &metrics,
            )
            .with_index_scan(SERIAL_SCAN)
            .with_fleet_config(fleet_config_bound(
                &origin,
                origin.bucket(),
                &name,
                bind,
                &book,
            ));
            let proxy = match recovery {
                Some(config) => proxy
                    .with_recovery_config(config)
                    .expect("finite paced recovery bounds"),
                None => proxy,
            };
            (sync, proxy)
        })
        .await
        .expect("pod node construction")
}

/// [`pod_node`] with its gossip on the in-memory `net` instead of UDP, built
/// through the binary's own membership tuning and strong lease, and with the
/// binary's own recovery and claim configuration. Peers dial its bulk
/// listener on `bind` over loopback TCP, as in production.
pub async fn mem_pod_node(
    pod: &Pod,
    net: &groupnet::transport::mem::Network,
    origin: &Arc<Origin>,
    (name, bind): (&str, u16),
    peer: &str,
    book: [(&'static str, u16); 2],
    metrics: &Arc<Metrics>,
) -> (Arc<WriteSync>, CachingProxy) {
    let (net, origin, metrics) = (net.clone(), Arc::clone(origin), Arc::clone(metrics));
    let (name, peer) = (name.to_owned(), peer.to_owned());
    pod.handle()
        .spawn(async move {
            let sync = Arc::new(WriteSync::over_transport(
                net.endpoint(groupnet::core::NodeId::new(name.as_str())),
                &name,
                &[peer.as_str()],
                Consistency::Strong,
                DEFAULT_LEASE_MS,
            ));
            let proxy = proxy_over_with_metrics(
                &origin.counted_client(),
                CAP,
                Some(Arc::clone(&sync)),
                &metrics,
            )
            .with_index_scan(SERIAL_SCAN)
            .with_fleet_config(fleet_config_bound(
                &origin,
                origin.bucket(),
                &name,
                bind,
                &book,
            ));
            (sync, proxy)
        })
        .await
        .expect("pod node construction")
}

/// Print the fleet's info decisions, including transfer aborts and
/// fallbacks, into the test output, each under the pod thread that made it.
pub fn trace_decisions() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "s3cache::sync::volatile=info".into()),
        )
        .with_thread_names(true)
        .with_test_writer()
        .try_init();
}
