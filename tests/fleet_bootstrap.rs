//! Origin cost and liveness of peer index bootstrap against real `MinIO`.

mod common;

use std::net::TcpListener;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use common::{
    Origin, WarmDir, counter, delete, free_udp_port, get, gossip_node, head, list, proxy_over,
    proxy_over_with_metrics, put, request, synthetic_key, wait_for_index, warm_proxy_over,
};
use futures::StreamExt;
use groupnet::consistency::volatile_recovery::{RecoveryConfig, RecoveryStage};
use s3cache::cache::proxy::CachingProxy;
use s3cache::index::ScanConfig;
use s3cache::metrics::Metrics;
use s3cache::sync::coherence::WriteSync;
use s3cache::sync::fleet::config::FleetConfig;
use s3s::S3;
use s3s::dto::ListObjectsV2Input;

const CAP: usize = 1024 * 1024;
const READY_DEADLINE: Duration = Duration::from_secs(30);
const JOIN_READ_DELAY: Duration = Duration::from_secs(8);
const READ_VECTOR_DEADLINE: Duration = Duration::from_secs(10);

struct PausedListRelease(Arc<Origin>);

impl Drop for PausedListRelease {
    fn drop(&mut self) {
        self.0.release_paused_list();
    }
}

#[derive(Clone, Copy, Debug)]
struct Counts {
    list: u64,
    get: u64,
    head: u64,
    successful_list: u64,
    successful_get: u64,
    successful_head: u64,
    put: u64,
    copy: u64,
    delete: u64,
}

impl Counts {
    fn take(origin: &Origin) -> Self {
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

    fn since(self, before: Self) -> Self {
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

    fn assert_no_writes(self) {
        assert_eq!(self.put, 0, "fleet bootstrap wrote an origin object");
        assert_eq!(self.copy, 0, "fleet bootstrap copied an origin object");
        assert_eq!(self.delete, 0, "fleet bootstrap deleted an origin object");
    }
}

fn free_tcp_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("test TCP port")
        .local_addr()
        .expect("bound TCP port")
        .port()
}

async fn mutual_alive(a: &WriteSync, a_name: &str, b: &WriteSync, b_name: &str) {
    tokio::time::timeout(READY_DEADLINE, async {
        loop {
            if a.peer_alive(b_name) && b.peer_alive(a_name) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("both gossip views reach Alive before the healthy-donor join");
}

fn fleet_config(origin: &Origin, bucket: &str, name: &str, ports: &[(&str, u16)]) -> FleetConfig {
    let port = ports
        .iter()
        .find_map(|(peer, port)| (*peer == name).then_some(*port))
        .expect("node in the complete fleet address book");
    let address = format!("127.0.0.1:{port}");
    let book = ports
        .iter()
        .map(|(peer, port)| format!("{peer}=127.0.0.1:{port}"))
        .collect::<Vec<_>>()
        .join(",");
    FleetConfig::parse(
        Some(address.clone()),
        Some(address),
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

async fn ready(proxy: &CachingProxy, bucket: &str) -> Duration {
    ready_by(proxy, bucket, Instant::now() + READY_DEADLINE).await
}

async fn ready_by(proxy: &CachingProxy, bucket: &str, due: Instant) -> Duration {
    ready_all_by(&[proxy], bucket, due).await
}

fn serves_locally(proxy: &CachingProxy, bucket: &str) -> bool {
    proxy
        .recovery_status()
        .is_some_and(|status| status.state.stage == RecoveryStage::Ready && status.may_serve)
        && proxy.initially_ready(&[bucket.to_owned()])
}

async fn ready_all_by(nodes: &[&CachingProxy], bucket: &str, due: Instant) -> Duration {
    let started = Instant::now();
    tokio::time::timeout_at(tokio::time::Instant::from_std(due), async {
        loop {
            if nodes.iter().all(|node| serves_locally(node, bucket)) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("each current recovery gate and index reach Ready before the fixed deadline");
    started.elapsed()
}

#[derive(Clone, Copy, Debug)]
struct ReadTiming {
    get_ms: u128,
    vector_ms: u128,
}

async fn sample_reads(proxy: &CachingProxy, bucket: &str) -> ReadTiming {
    tokio::time::timeout(READ_VECTOR_DEADLINE, async {
        let started = Instant::now();
        assert_eq!(list(proxy, bucket).await, ["body"]);
        let get_started = Instant::now();
        assert_eq!(get(proxy, bucket, "body").await.as_ref(), b"warm-body");
        let get_ms = get_started.elapsed().as_millis();
        assert!(head(proxy, bucket, "body").await.is_ok());
        ReadTiming {
            get_ms,
            vector_ms: started.elapsed().as_millis(),
        }
    })
    .await
    .expect("LIST/GET/HEAD vector remains responsive")
}

async fn stage_warm_body(origin: &Origin, dir: &WarmDir, bucket: &str) {
    {
        let metrics = Arc::new(Metrics::default());
        let prior = warm_proxy_over(&origin.counted_client(), CAP, None, dir, &metrics);
        prior.spawn_background_sync(vec![bucket.to_owned()]);
        wait_for_index(&prior, origin, bucket).await;
        assert_eq!(get(&prior, bucket, "body").await.as_ref(), b"warm-body");
    }
    tokio::time::timeout(READY_DEADLINE, async {
        while dir.files() != 1 {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("the previous process left one persisted warm body");
}

/// Measure the deployment's ordinary immediate-join schedule separately from
/// the mutually Alive donor schedule below. A just-started peer may still be
/// Suspect in the joiner's roster; guarded origin recovery is then legitimate
/// and must remain bounded, but it is not a Class A saving.
#[tokio::test]
async fn immediate_join_records_origin_fallback_cost() {
    let origin = Origin::start("fleet-immediate-join").await;
    origin.seed("body", b"warm-body").await;
    let bucket = origin.bucket().to_owned();
    let client = origin.counted_client();

    let off_before = Counts::take(&origin);
    let (off_donor_udp, off_joiner_udp) = (free_udp_port(), free_udp_port());
    let off_donor_sync = gossip_node(
        "immediate-off-a",
        off_donor_udp,
        &[("immediate-off-b", off_joiner_udp)],
    )
    .await;
    let off_a = proxy_over(&client, CAP, Some(off_donor_sync));
    off_a.start_coherence(std::slice::from_ref(&bucket));
    off_a.spawn_background_sync(vec![bucket.clone()]);
    let off_donor_ready = ready(&off_a, &bucket).await;
    let off_joiner_sync = gossip_node(
        "immediate-off-b",
        off_joiner_udp,
        &[("immediate-off-a", off_donor_udp)],
    )
    .await;
    let off_b = proxy_over(&client, CAP, Some(off_joiner_sync));
    off_b.start_coherence(std::slice::from_ref(&bucket));
    off_b.spawn_background_sync(vec![bucket.clone()]);
    let off_joiner_ready = ready(&off_b, &bucket).await;
    let off_both_ready =
        ready_all_by(&[&off_a, &off_b], &bucket, Instant::now() + READY_DEADLINE).await;
    let off_cost = Counts::take(&origin).since(off_before);

    let on_before = Counts::take(&origin);
    let ports = [
        ("immediate-on-a", free_tcp_port()),
        ("immediate-on-b", free_tcp_port()),
    ];
    let (on_donor_udp, on_joiner_udp) = (free_udp_port(), free_udp_port());
    let on_donor_sync = gossip_node(
        "immediate-on-a",
        on_donor_udp,
        &[("immediate-on-b", on_joiner_udp)],
    )
    .await;
    let on_a = proxy_over(&client, CAP, Some(on_donor_sync)).with_fleet_config(fleet_config(
        &origin,
        &bucket,
        "immediate-on-a",
        &ports,
    ));
    on_a.start_fleet_coherence(std::slice::from_ref(&bucket))
        .await;
    on_a.spawn_background_sync(vec![bucket.clone()]);
    let on_donor_ready = ready(&on_a, &bucket).await;
    let on_joiner_sync = gossip_node(
        "immediate-on-b",
        on_joiner_udp,
        &[("immediate-on-a", on_donor_udp)],
    )
    .await;
    let on_b = proxy_over(&client, CAP, Some(on_joiner_sync)).with_fleet_config(fleet_config(
        &origin,
        &bucket,
        "immediate-on-b",
        &ports,
    ));
    on_b.start_fleet_coherence(std::slice::from_ref(&bucket))
        .await;
    on_b.spawn_background_sync(vec![bucket.clone()]);
    let on_joiner_ready = ready(&on_b, &bucket).await;
    let on_both_ready =
        ready_all_by(&[&on_a, &on_b], &bucket, Instant::now() + READY_DEADLINE).await;
    let on_cost = Counts::take(&origin).since(on_before);

    let off_read_before = Counts::take(&origin);
    let off_read_time = sample_reads(&off_b, &bucket).await;
    let off_read_cost = Counts::take(&origin).since(off_read_before);
    let on_read_before = Counts::take(&origin);
    let on_read_time = sample_reads(&on_b, &bucket).await;
    let on_read_cost = Counts::take(&origin).since(on_read_before);
    assert!(
        on_cost.list <= off_cost.list,
        "immediate fleet join must not add origin LIST attempts: off={off_cost:?}, on={on_cost:?}"
    );
    off_cost.assert_no_writes();
    on_cost.assert_no_writes();
    off_read_cost.assert_no_writes();
    on_read_cost.assert_no_writes();
    println!(
        "fleet_bootstrap immediate_startup_off={off_cost:?} immediate_startup_on={on_cost:?} immediate_reads_off={off_read_cost:?} immediate_reads_on={on_read_cost:?} positive_get_ms=[{},{}] vector_ms=[{},{}] index_ready_ms=[{},{},{},{}] both_current_ready_ms=[{},{}]",
        off_read_time.get_ms,
        on_read_time.get_ms,
        off_read_time.vector_ms,
        on_read_time.vector_ms,
        off_donor_ready.as_millis(),
        off_joiner_ready.as_millis(),
        on_donor_ready.as_millis(),
        on_joiner_ready.as_millis(),
        off_both_ready.as_millis(),
        on_both_ready.as_millis()
    );
    assert!(origin.ops.writes().is_empty(), "no origin control writes");
}

/// Start the same two-node schedule first without and then with fleet mode.
/// Count billed origin attempts, completed responses, and first local-read time.
/// The fleet transfers an index, not object bytes: the first GET may still
/// reach `MinIO`, while repeated same-process hot-body reads remain local.
#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "paired cold and warm request vectors share one origin counter baseline and fixed ordering"
)]
async fn cold_join_prices_origin_lists_and_warm_reads() {
    let origin = Origin::start("fleet-bootstrap-cost").await;
    origin.seed("body", b"warm-body").await;
    let bucket = origin.bucket().to_owned();
    let client = origin.counted_client();

    let off_before = Counts::take(&origin);
    let (off_donor_udp, off_joiner_udp) = (free_udp_port(), free_udp_port());
    let off_donor_sync = gossip_node(
        "fleet-off-a",
        off_donor_udp,
        &[("fleet-off-b", off_joiner_udp)],
    )
    .await;
    let off_a = proxy_over(&client, CAP, Some(Arc::clone(&off_donor_sync)));
    off_a.start_coherence(std::slice::from_ref(&bucket));
    off_a.spawn_background_sync(vec![bucket.clone()]);
    let off_donor_ready = ready(&off_a, &bucket).await;
    let off_joiner_sync = gossip_node(
        "fleet-off-b",
        off_joiner_udp,
        &[("fleet-off-a", off_donor_udp)],
    )
    .await;
    let off_b = proxy_over(&client, CAP, Some(Arc::clone(&off_joiner_sync)));
    mutual_alive(
        &off_donor_sync,
        "fleet-off-a",
        &off_joiner_sync,
        "fleet-off-b",
    )
    .await;
    off_b.start_coherence(std::slice::from_ref(&bucket));
    off_b.spawn_background_sync(vec![bucket.clone()]);
    let off_joiner_ready = ready(&off_b, &bucket).await;
    let off_both_ready =
        ready_all_by(&[&off_a, &off_b], &bucket, Instant::now() + READY_DEADLINE).await;
    let off_cold = Counts::take(&origin).since(off_before);
    assert!(
        off_cold.list >= 2,
        "each disabled node completes an origin scan"
    );
    assert_eq!(off_cold.list, off_cold.successful_list);
    let off_first_a = sample_reads(&off_a, &bucket).await;
    let off_first_b = sample_reads(&off_b, &bucket).await;
    let off_warm_before = Counts::take(&origin);
    let off_warm_a = sample_reads(&off_a, &bucket).await;
    let off_warm_b = sample_reads(&off_b, &bucket).await;
    let off_warm = Counts::take(&origin).since(off_warm_before);

    let on_before = Counts::take(&origin);
    let ports = [
        ("fleet-on-a", free_tcp_port()),
        ("fleet-on-b", free_tcp_port()),
    ];
    let (on_donor_udp, on_joiner_udp) = (free_udp_port(), free_udp_port());
    let on_donor_sync =
        gossip_node("fleet-on-a", on_donor_udp, &[("fleet-on-b", on_joiner_udp)]).await;
    let on_a = proxy_over(&client, CAP, Some(Arc::clone(&on_donor_sync)))
        .with_fleet_config(fleet_config(&origin, &bucket, "fleet-on-a", &ports));
    on_a.start_fleet_coherence(std::slice::from_ref(&bucket))
        .await;
    on_a.spawn_background_sync(vec![bucket.clone()]);
    let on_donor_ready = ready(&on_a, &bucket).await;
    let on_joiner_sync =
        gossip_node("fleet-on-b", on_joiner_udp, &[("fleet-on-a", on_donor_udp)]).await;
    let on_b = proxy_over(&client, CAP, Some(Arc::clone(&on_joiner_sync)))
        .with_fleet_config(fleet_config(&origin, &bucket, "fleet-on-b", &ports));
    mutual_alive(&on_donor_sync, "fleet-on-a", &on_joiner_sync, "fleet-on-b").await;
    on_b.start_fleet_coherence(std::slice::from_ref(&bucket))
        .await;
    on_b.spawn_background_sync(vec![bucket.clone()]);
    let on_joiner_ready = ready(&on_b, &bucket).await;
    let on_both_ready =
        ready_all_by(&[&on_a, &on_b], &bucket, Instant::now() + READY_DEADLINE).await;
    let on_cold = Counts::take(&origin).since(on_before);
    assert!(
        on_cold.list < off_cold.list,
        "fleet join should replace one full origin LIST scan: off={off_cold:?}, on={on_cold:?}"
    );
    assert_eq!(on_cold.list, on_cold.successful_list);
    let on_first_a = sample_reads(&on_a, &bucket).await;
    let on_first_b = sample_reads(&on_b, &bucket).await;
    let on_warm_before = Counts::take(&origin);
    let on_warm_a = sample_reads(&on_a, &bucket).await;
    let on_warm_b = sample_reads(&on_b, &bucket).await;
    let on_warm = Counts::take(&origin).since(on_warm_before);
    assert!(on_warm.get <= off_warm.get, "fleet must not add warm GETs");
    assert!(
        on_warm.head <= off_warm.head,
        "fleet must not add warm HEADs"
    );
    assert_eq!(on_warm.get, on_warm.successful_get);
    assert_eq!(on_warm.head, on_warm.successful_head);

    off_cold.assert_no_writes();
    on_cold.assert_no_writes();
    off_warm.assert_no_writes();
    on_warm.assert_no_writes();
    println!(
        "fleet_bootstrap cold_off={off_cold:?} cold_on={on_cold:?} warm_off={off_warm:?} warm_on={on_warm:?} first_get_ms=[{},{},{},{}] first_vector_ms=[{},{},{},{}] warm_get_ms=[{},{},{},{}] warm_vector_ms=[{},{},{},{}] index_ready_ms=[{},{},{},{}] both_current_ready_ms=[{},{}]",
        off_first_a.get_ms,
        off_first_b.get_ms,
        on_first_a.get_ms,
        on_first_b.get_ms,
        off_first_a.vector_ms,
        off_first_b.vector_ms,
        on_first_a.vector_ms,
        on_first_b.vector_ms,
        off_warm_a.get_ms,
        off_warm_b.get_ms,
        on_warm_a.get_ms,
        on_warm_b.get_ms,
        off_warm_a.vector_ms,
        off_warm_b.vector_ms,
        on_warm_a.vector_ms,
        on_warm_b.vector_ms,
        off_donor_ready.as_millis(),
        off_joiner_ready.as_millis(),
        on_donor_ready.as_millis(),
        on_joiner_ready.as_millis(),
        off_both_ready.as_millis(),
        on_both_ready.as_millis()
    );
    assert!(origin.ops.writes().is_empty(), "no origin control writes");
}

/// Both joining nodes inherit the same persisted body fixture from an earlier
/// process. Hold each joining node's next origin LIST after `MinIO` answers, then
/// issue the same LIST/GET/HEAD schedule at the same fixed delay. The enabled
/// node must reach Ready from its peer without attempting the held origin LIST;
/// the disabled node still pays origin GET/HEAD during its blocked scan.
#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "the two warm-join arms keep one fixed request schedule and their cost baselines together"
)]
async fn later_join_keeps_persisted_warm_body_origin_free() {
    let origin = Origin::start("fleet-warm-join").await;
    origin.seed("body", b"warm-body").await;
    let bucket = origin.bucket().to_owned();
    let client = origin.counted_client();
    let off_dir = WarmDir::new("fleet-warm-off");
    let on_dir = WarmDir::new("fleet-warm-on");
    stage_warm_body(&origin, &off_dir, &bucket).await;
    stage_warm_body(&origin, &on_dir, &bucket).await;

    let (off_donor_udp, off_joiner_udp) = (free_udp_port(), free_udp_port());
    let off_donor_sync = gossip_node(
        "fleet-warm-off-a",
        off_donor_udp,
        &[("fleet-warm-off-b", off_joiner_udp)],
    )
    .await;
    let off_a = proxy_over(&client, CAP, Some(Arc::clone(&off_donor_sync)));
    off_a.start_coherence(std::slice::from_ref(&bucket));
    off_a.spawn_background_sync(vec![bucket.clone()]);
    ready(&off_a, &bucket).await;
    let off_before = Counts::take(&origin);
    let off_joiner_sync = gossip_node(
        "fleet-warm-off-b",
        off_joiner_udp,
        &[("fleet-warm-off-a", off_donor_udp)],
    )
    .await;
    let off_metrics = Arc::new(Metrics::default());
    let off_b = warm_proxy_over(
        &client,
        CAP,
        Some(Arc::clone(&off_joiner_sync)),
        &off_dir,
        &off_metrics,
    );
    mutual_alive(
        &off_donor_sync,
        "fleet-warm-off-a",
        &off_joiner_sync,
        "fleet-warm-off-b",
    )
    .await;
    origin.pause_next_list();
    let release = PausedListRelease(Arc::clone(&origin));
    let off_started = Instant::now();
    off_b.start_coherence(std::slice::from_ref(&bucket));
    off_b.spawn_background_sync(vec![bucket.clone()]);
    let off_read_at = off_started + JOIN_READ_DELAY;
    tokio::time::timeout_at(
        tokio::time::Instant::from_std(off_read_at),
        origin.wait_for_paused_list(),
    )
    .await
    .expect("disabled joining node issued its startup origin LIST");
    tokio::time::sleep_until(tokio::time::Instant::from_std(off_read_at)).await;
    let off_read_time = sample_reads(&off_b, &bucket).await;
    let off_cost = Counts::take(&origin).since(off_before);
    drop(release);
    ready(&off_b, &bucket).await;
    let off_ready = off_started.elapsed();
    sample_reads(&off_b, &bucket).await;
    assert!(counter(&off_metrics, "warm_hit") >= 1);

    let ports = [
        ("fleet-warm-on-a", free_tcp_port()),
        ("fleet-warm-on-b", free_tcp_port()),
    ];
    let (on_donor_udp, on_joiner_udp) = (free_udp_port(), free_udp_port());
    let on_donor_sync = gossip_node(
        "fleet-warm-on-a",
        on_donor_udp,
        &[("fleet-warm-on-b", on_joiner_udp)],
    )
    .await;
    let on_a = proxy_over(&client, CAP, Some(Arc::clone(&on_donor_sync)))
        .with_fleet_config(fleet_config(&origin, &bucket, "fleet-warm-on-a", &ports));
    on_a.start_fleet_coherence(std::slice::from_ref(&bucket))
        .await;
    on_a.spawn_background_sync(vec![bucket.clone()]);
    ready(&on_a, &bucket).await;
    let on_before = Counts::take(&origin);
    let on_joiner_sync = gossip_node(
        "fleet-warm-on-b",
        on_joiner_udp,
        &[("fleet-warm-on-a", on_donor_udp)],
    )
    .await;
    let on_metrics = Arc::new(Metrics::default());
    let on_b = warm_proxy_over(
        &client,
        CAP,
        Some(Arc::clone(&on_joiner_sync)),
        &on_dir,
        &on_metrics,
    )
    .with_fleet_config(fleet_config(&origin, &bucket, "fleet-warm-on-b", &ports));
    mutual_alive(
        &on_donor_sync,
        "fleet-warm-on-a",
        &on_joiner_sync,
        "fleet-warm-on-b",
    )
    .await;
    origin.pause_next_list();
    let on_release = PausedListRelease(Arc::clone(&origin));
    let on_started = Instant::now();
    on_b.start_fleet_coherence(std::slice::from_ref(&bucket))
        .await;
    on_b.spawn_background_sync(vec![bucket.clone()]);
    let on_read_at = on_started + JOIN_READ_DELAY;
    ready_all_by(&[&on_a, &on_b], &bucket, on_read_at).await;
    let on_ready = on_started.elapsed();
    tokio::time::sleep_until(tokio::time::Instant::from_std(on_read_at)).await;
    let on_read_time = sample_reads(&on_b, &bucket).await;
    let on_cost = Counts::take(&origin).since(on_before);
    drop(on_release);
    assert!(counter(&on_metrics, "warm_hit") >= 1);

    assert_eq!(
        on_cost.list, 0,
        "peer-built index became Ready without the joining node attempting origin LIST: off={off_cost:?}, on={on_cost:?}"
    );
    assert!(
        off_cost.get >= 1,
        "blocked scan cannot yet trust the warm body"
    );
    assert_eq!(on_cost.get, 0, "on fixture retained its warm body");
    assert!(on_cost.get < off_cost.get, "fleet saves Class B GETs");
    assert!(
        on_cost.head <= off_cost.head,
        "fleet must not add Class B HEADs: off={off_cost:?}, on={on_cost:?}"
    );
    assert!(
        on_cost.get + on_cost.head < off_cost.get + off_cost.head,
        "fleet saves total Class B requests: off={off_cost:?}, on={on_cost:?}"
    );
    off_cost.assert_no_writes();
    on_cost.assert_no_writes();
    println!(
        "fleet_bootstrap persisted_warm off={off_cost:?} on={on_cost:?} positive_get_ms=[{},{}] vector_ms=[{},{}] index_ready_ms=[{},{}]",
        off_read_time.get_ms,
        on_read_time.get_ms,
        off_read_time.vector_ms,
        on_read_time.vector_ms,
        off_ready.as_millis(),
        on_ready.as_millis()
    );
    assert!(origin.ops.writes().is_empty(), "no origin control writes");
}

/// A follower that already has a peer-built index keeps serving that indexed
/// bucket while one of its own unrelated origin LISTs is waiting on `MinIO`.
#[tokio::test]
async fn ready_follower_keeps_local_reads_during_a_paused_origin_list() {
    let origin = Origin::start("fleet-paused-list").await;
    origin.seed("body", b"warm-body").await;
    let bucket = origin.bucket().to_owned();
    let other = "fleet-paused-list-other";
    origin
        .client()
        .create_bucket()
        .bucket(other)
        .send()
        .await
        .expect("create unindexed passthrough bucket");
    let client = origin.counted_client();
    let ports = [
        ("fleet-pause-a", free_tcp_port()),
        ("fleet-pause-b", free_tcp_port()),
    ];
    let (a_udp, b_udp) = (free_udp_port(), free_udp_port());
    let a_sync = gossip_node("fleet-pause-a", a_udp, &[("fleet-pause-b", b_udp)]).await;
    let a = proxy_over(&client, CAP, Some(Arc::clone(&a_sync))).with_fleet_config(fleet_config(
        &origin,
        &bucket,
        "fleet-pause-a",
        &ports,
    ));
    a.start_fleet_coherence(std::slice::from_ref(&bucket)).await;
    a.spawn_background_sync(vec![bucket.clone()]);
    ready(&a, &bucket).await;
    let before_join = Counts::take(&origin);
    let b_sync = gossip_node("fleet-pause-b", b_udp, &[("fleet-pause-a", a_udp)]).await;
    let b = proxy_over(&client, CAP, Some(Arc::clone(&b_sync))).with_fleet_config(fleet_config(
        &origin,
        &bucket,
        "fleet-pause-b",
        &ports,
    ));
    mutual_alive(&a_sync, "fleet-pause-a", &b_sync, "fleet-pause-b").await;
    b.start_fleet_coherence(std::slice::from_ref(&bucket)).await;
    b.spawn_background_sync(vec![bucket.clone()]);
    ready(&b, &bucket).await;
    ready_all_by(&[&a, &b], &bucket, Instant::now() + READY_DEADLINE).await;
    assert_eq!(
        Counts::take(&origin).list,
        before_join.list,
        "follower became Ready from its peer without a completed origin scan"
    );
    sample_reads(&b, &bucket).await;

    origin.pause_next_list();
    let release = PausedListRelease(Arc::clone(&origin));
    let waiting = b.clone();
    let paused = tokio::spawn(async move { list(&waiting, other).await });
    tokio::time::timeout(READY_DEADLINE, origin.wait_for_paused_list())
        .await
        .expect("follower passthrough LIST reached origin");
    let before = Counts::take(&origin);
    tokio::time::timeout(Duration::from_secs(5), sample_reads(&b, &bucket))
        .await
        .expect("indexed bucket still serves locally during origin stall");
    let after = Counts::take(&origin).since(before);
    assert_eq!(after.list, 0, "indexed LIST did not visit the origin");
    assert_eq!(after.get, 0, "warm body did not visit the origin");
    assert_eq!(after.head, 0, "indexed HEAD did not visit the origin");
    drop(release);
    assert!(
        paused
            .await
            .expect("passthrough request finishes")
            .is_empty()
    );
    after.assert_no_writes();
    assert!(origin.ops.writes().is_empty(), "no origin control writes");
}

/// A changed live roster retires the old finite donor capture. Its already
/// Ready index can be recaptured under a fresh roster so a third joining node
/// does not force another full origin inventory.
#[tokio::test]
async fn third_join_uses_fresh_ready_donor_capture_without_origin_rescan() {
    let origin = Origin::start("fleet-third-join").await;
    origin.seed("body", b"warm-body").await;
    let bucket = origin.bucket().to_owned();
    let client = origin.counted_client();
    let ports = [
        ("fleet-third-a", free_tcp_port()),
        ("fleet-third-b", free_tcp_port()),
        ("fleet-third-c", free_tcp_port()),
    ];
    let udp_a = free_udp_port();
    let udp_b = free_udp_port();
    let udp_c = free_udp_port();
    let a_sync = gossip_node(
        "fleet-third-a",
        udp_a,
        &[("fleet-third-b", udp_b), ("fleet-third-c", udp_c)],
    )
    .await;
    let a = proxy_over(&client, CAP, Some(Arc::clone(&a_sync))).with_fleet_config(fleet_config(
        &origin,
        &bucket,
        "fleet-third-a",
        &ports,
    ));
    a.start_fleet_coherence(std::slice::from_ref(&bucket)).await;
    a.spawn_background_sync(vec![bucket.clone()]);
    ready(&a, &bucket).await;

    let b_sync = gossip_node(
        "fleet-third-b",
        udp_b,
        &[("fleet-third-a", udp_a), ("fleet-third-c", udp_c)],
    )
    .await;
    let b = proxy_over(&client, CAP, Some(Arc::clone(&b_sync))).with_fleet_config(fleet_config(
        &origin,
        &bucket,
        "fleet-third-b",
        &ports,
    ));
    mutual_alive(&a_sync, "fleet-third-a", &b_sync, "fleet-third-b").await;
    b.start_fleet_coherence(std::slice::from_ref(&bucket)).await;
    b.spawn_background_sync(vec![bucket.clone()]);
    ready(&b, &bucket).await;
    ready_all_by(&[&a, &b], &bucket, Instant::now() + READY_DEADLINE).await;
    let before_third = Counts::take(&origin);

    let c_sync = gossip_node(
        "fleet-third-c",
        udp_c,
        &[("fleet-third-a", udp_a), ("fleet-third-b", udp_b)],
    )
    .await;
    let c = proxy_over(&client, CAP, Some(Arc::clone(&c_sync))).with_fleet_config(fleet_config(
        &origin,
        &bucket,
        "fleet-third-c",
        &ports,
    ));
    mutual_alive(&a_sync, "fleet-third-a", &c_sync, "fleet-third-c").await;
    mutual_alive(&b_sync, "fleet-third-b", &c_sync, "fleet-third-c").await;
    c.start_fleet_coherence(std::slice::from_ref(&bucket)).await;
    c.spawn_background_sync(vec![bucket.clone()]);
    let third_ready = ready(&c, &bucket).await;
    let all_current_ready =
        ready_all_by(&[&a, &b, &c], &bucket, Instant::now() + READY_DEADLINE).await;
    let third_cost = Counts::take(&origin).since(before_third);
    assert_eq!(
        third_cost.list, 0,
        "Ready donor recapture avoids a third full origin scan: {third_cost:?}"
    );
    sample_reads(&c, &bucket).await;
    third_cost.assert_no_writes();
    println!(
        "fleet_bootstrap third_join={third_cost:?} index_ready_ms={} all_current_ready_ms={}",
        third_ready.as_millis(),
        all_current_ready.as_millis()
    );
    assert!(origin.ops.writes().is_empty(), "no origin control writes");
}

/// The selected donor's TCP address is unreachable despite live gossip.
/// Peer transfer must stop at a finite deadline and fall back to a guarded
/// origin scan instead of leaving this node permanently unable to serve.
#[tokio::test]
async fn unreachable_donor_falls_back_to_origin_within_budget() {
    let origin = Origin::start("fleet-donor-unreachable").await;
    origin.seed("body", b"warm-body").await;
    let bucket = origin.bucket().to_owned();
    let client = origin.counted_client();
    let actual_a = free_tcp_port();
    let actual_b = free_tcp_port();
    let unreachable_a = free_tcp_port();
    let a_udp = free_udp_port();
    let b_udp = free_udp_port();
    let a_sync = gossip_node("fleet-fault-a", a_udp, &[("fleet-fault-b", b_udp)]).await;
    let good_book = [("fleet-fault-a", actual_a), ("fleet-fault-b", actual_b)];
    let a = proxy_over(&client, CAP, Some(Arc::clone(&a_sync))).with_fleet_config(fleet_config(
        &origin,
        &bucket,
        "fleet-fault-a",
        &good_book,
    ));
    a.start_fleet_coherence(std::slice::from_ref(&bucket)).await;
    a.spawn_background_sync(vec![bucket.clone()]);
    ready(&a, &bucket).await;

    let before = Counts::take(&origin);
    let b_sync = gossip_node("fleet-fault-b", b_udp, &[("fleet-fault-a", a_udp)]).await;
    let broken_book = [
        ("fleet-fault-a", unreachable_a),
        ("fleet-fault-b", actual_b),
    ];
    let b = proxy_over(&client, CAP, Some(Arc::clone(&b_sync)))
        .with_fleet_config(fleet_config(
            &origin,
            &bucket,
            "fleet-fault-b",
            &broken_book,
        ))
        .with_recovery_config(RecoveryConfig {
            max_members: 16,
            max_member_bytes: 128,
            max_barrier_rounds: 4,
            total_ms: 20_000,
            attempt_ms: 3_000,
            settle_ms: 3_000,
            poll_ms: 50,
        })
        .expect("finite fault test recovery budget");
    mutual_alive(&a_sync, "fleet-fault-a", &b_sync, "fleet-fault-b").await;
    b.start_fleet_coherence(std::slice::from_ref(&bucket)).await;
    b.spawn_background_sync(vec![bucket.clone()]);
    let fallback_ready = ready(&b, &bucket).await;
    let fallback = Counts::take(&origin).since(before);
    assert!(
        fallback.successful_list >= 1,
        "failed donor transfer completed a guarded origin scan: {fallback:?}"
    );
    sample_reads(&b, &bucket).await;
    fallback.assert_no_writes();
    println!(
        "fleet_bootstrap donor_unreachable={fallback:?} index_ready_ms={}",
        fallback_ready.as_millis()
    );
    assert!(origin.ops.writes().is_empty(), "no origin control writes");
}

/// A normal serialized writer keeps changing one object and deleting another
/// throughout a follower's peer bootstrap. Its final acknowledged state must
/// be visible from the follower without a second full origin inventory.
#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one serialized mutation schedule and its final follower assertions form one integration scenario"
)]
async fn live_mutations_overlap_follower_bootstrap() {
    let origin = Origin::start("fleet-live-join").await;
    for n in 0..48 {
        origin.seed(&format!("seed-{n:03}"), b"seed body").await;
    }
    let bucket = origin.bucket().to_owned();
    let client = origin.counted_client();
    let ports = [
        ("fleet-live-a", free_tcp_port()),
        ("fleet-live-b", free_tcp_port()),
    ];
    let (a_udp, b_udp) = (free_udp_port(), free_udp_port());
    let a_sync = gossip_node("fleet-live-a", a_udp, &[("fleet-live-b", b_udp)]).await;
    let a = proxy_over(&client, CAP, Some(Arc::clone(&a_sync))).with_fleet_config(fleet_config(
        &origin,
        &bucket,
        "fleet-live-a",
        &ports,
    ));
    a.start_fleet_coherence(std::slice::from_ref(&bucket)).await;
    a.spawn_background_sync(vec![bucket.clone()]);
    ready(&a, &bucket).await;

    let before_join = Counts::take(&origin);
    let stop = Arc::new(AtomicBool::new(false));
    let completed = Arc::new(AtomicU64::new(0));
    let writer_stop = Arc::clone(&stop);
    let writer_completed = Arc::clone(&completed);
    let writer_node = a.clone();
    let writer_bucket = bucket.clone();
    let (first_write, first_done) = tokio::sync::oneshot::channel();
    let writer = tokio::spawn(async move {
        tokio::time::timeout(Duration::from_secs(35), async move {
            let mut first_write = Some(first_write);
            let mut round = 0_u64;
            loop {
                round += 1;
                let body = format!("live-version-{round}");
                put(
                    &writer_node,
                    &writer_bucket,
                    "live-updated",
                    body.as_bytes(),
                )
                .await;
                put(&writer_node, &writer_bucket, "live-deleted", b"transient").await;
                delete(&writer_node, &writer_bucket, "live-deleted").await;
                writer_completed.store(round, Ordering::Release);
                if let Some(first_write) = first_write.take() {
                    let _ = first_write.send(());
                }
                if round >= 3 && writer_stop.load(Ordering::Acquire) {
                    return (body, round);
                }
                tokio::time::sleep(Duration::from_millis(150)).await;
            }
        })
        .await
        .expect("serialized live writer completed within its bound")
    });
    tokio::time::timeout(READY_DEADLINE, first_done)
        .await
        .expect("first live mutation was acknowledged")
        .expect("live writer reported its first mutation");

    let b_sync = gossip_node("fleet-live-b", b_udp, &[("fleet-live-a", a_udp)]).await;
    let b = proxy_over(&client, CAP, Some(Arc::clone(&b_sync))).with_fleet_config(fleet_config(
        &origin,
        &bucket,
        "fleet-live-b",
        &ports,
    ));
    mutual_alive(&a_sync, "fleet-live-a", &b_sync, "fleet-live-b").await;
    let before_join_rounds = completed.load(Ordering::Acquire);
    let join_started = Instant::now();
    b.start_fleet_coherence(std::slice::from_ref(&bucket)).await;
    b.spawn_background_sync(vec![bucket.clone()]);
    let join_ready = ready(&b, &bucket).await;
    let completed_during_join = completed.load(Ordering::Acquire);
    stop.store(true, Ordering::Release);
    let (final_body, rounds) = writer.await.expect("live writer task joins");
    assert!(
        completed_during_join > before_join_rounds,
        "at least one full mutation round must commit while the follower joins"
    );

    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let keys = list(&b, &bucket).await;
            if keys.contains(&"live-updated".to_owned())
                && !keys.contains(&"live-deleted".to_owned())
                && head(&b, &bucket, "live-updated").await.is_ok()
                && get(&b, &bucket, "live-updated").await.as_ref() == final_body.as_bytes()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("follower serves the final acknowledged update and deletion");
    let both_current_ready =
        ready_all_by(&[&a, &b], &bucket, Instant::now() + READY_DEADLINE).await;
    let join_cost = Counts::take(&origin).since(before_join);
    assert_eq!(
        join_cost.list, 0,
        "live writes did not force an independent full origin LIST: {join_cost:?}"
    );
    assert_eq!(join_cost.put, 2 * rounds);
    assert_eq!(join_cost.delete, rounds);
    assert_eq!(join_cost.copy, 0);
    assert_eq!(origin.ops.writes().len() as u64, 3 * rounds);
    println!(
        "fleet_bootstrap live_join={join_cost:?} rounds={rounds} before_join={before_join_rounds} during_join={completed_during_join} index_ready_ms={} both_current_ready_ms={} elapsed_ms={}",
        join_ready.as_millis(),
        both_current_ready.as_millis(),
        join_started.elapsed().as_millis()
    );
}

/// Flat keys with one serial LIST chain: the scan is exactly `ceil(rows / 1000)`
/// LIST pages, with no discovery requests, so a restarted scan is countable.
const SERIAL_SCAN: ScanConfig = ScanConfig {
    workers: 1,
    discovery_budget: 0,
};
/// 12,345 rows are 13 LIST pages; at 1 s a page the scan takes about 13 s.
const SLOW_ROWS: usize = 12_345;
const SLOW_LIST: Duration = Duration::from_secs(1);
const SLOW_READY_DEADLINE: Duration = Duration::from_secs(90);

/// Fixed bounds a slow scan must outlive: an operation that commits no page for
/// 3 s fails, an episode without progress ends after 8 s, and the peer claim
/// policy derived from it gives a builder a 4 s donor wait.
fn stall_bounds() -> RecoveryConfig {
    RecoveryConfig {
        max_members: 16,
        max_member_bytes: 128,
        max_barrier_rounds: 4,
        total_ms: 8_000,
        attempt_ms: 3_000,
        settle_ms: 3_000,
        poll_ms: 50,
    }
}

/// Seed `rows` flat keys straight into `MinIO`, concurrently and uncounted.
async fn seed_rows(origin: &Origin, rows: usize) {
    futures::stream::iter(0..rows)
        .map(|n| async move { origin.seed(&format!("row-{n:07}"), b"x").await })
        .buffer_unordered(64)
        .collect::<()>()
        .await;
}

fn indexed_rows(metrics: &Metrics) -> u64 {
    counter(metrics, "index_objects")
}

/// One origin scan slower than the attempt bound, the episode budget and the
/// donor wait completes in exactly one pass of LIST pages. Before progress
/// reporting, each expired attempt restarted the scan from its first page and
/// the node never became ready.
#[tokio::test]
async fn slow_origin_scan_outlives_attempt_and_episode_bounds_in_one_pass() {
    let origin = Origin::start("fleet-slow-scan").await;
    seed_rows(&origin, SLOW_ROWS).await;
    let bucket = origin.bucket().to_owned();
    let client = origin.counted_client();
    origin.delay_lists(SLOW_LIST);
    let metrics = Arc::new(Metrics::default());
    let sync = gossip_node("slow-scan-a", free_udp_port(), &[]).await;
    let node = proxy_over_with_metrics(&client, CAP, Some(sync), &metrics)
        .with_index_scan(SERIAL_SCAN)
        .with_recovery_config(stall_bounds())
        .expect("finite slow-scan recovery bounds");
    let before = Counts::take(&origin);
    let started = Instant::now();
    node.start_coherence(std::slice::from_ref(&bucket));
    ready_by(&node, &bucket, Instant::now() + SLOW_READY_DEADLINE).await;
    let elapsed = started.elapsed();
    let cost = Counts::take(&origin).since(before);
    assert!(
        elapsed > Duration::from_millis(stall_bounds().total_ms),
        "the scan must outlast the whole fixed episode budget: {elapsed:?}"
    );
    assert_eq!(
        cost.list,
        SLOW_ROWS.div_ceil(1000) as u64,
        "exactly one pass of LIST pages: {cost:?}"
    );
    assert_eq!(cost.list, cost.successful_list);
    assert_eq!(counter(&metrics, "recovery_origin_scans"), 1);
    assert_eq!(indexed_rows(&metrics), SLOW_ROWS as u64);
    cost.assert_no_writes();
    println!(
        "fleet_bootstrap slow_scan={cost:?} index_ready_ms={}",
        elapsed.as_millis()
    );
}

/// Two nodes start together against the same slow origin. The selected builder's
/// scan outlives its donor wait and both episode budgets; the follower waits for
/// it while it keeps committing pages, then installs its image. The whole fleet
/// pays exactly one pass of LIST pages and both nodes index every row.
#[tokio::test]
async fn concurrent_start_follower_waits_for_a_slow_builder_and_lists_once() {
    let origin = Origin::start("fleet-slow-builder").await;
    seed_rows(&origin, SLOW_ROWS).await;
    let bucket = origin.bucket().to_owned();
    let client = origin.counted_client();
    origin.delay_lists(SLOW_LIST);
    let ports = [
        ("fleet-slow-a", free_tcp_port()),
        ("fleet-slow-b", free_tcp_port()),
    ];
    let (a_udp, b_udp) = (free_udp_port(), free_udp_port());
    let a_sync = gossip_node("fleet-slow-a", a_udp, &[("fleet-slow-b", b_udp)]).await;
    let b_sync = gossip_node("fleet-slow-b", b_udp, &[("fleet-slow-a", a_udp)]).await;
    let (a_metrics, b_metrics) = (Arc::new(Metrics::default()), Arc::new(Metrics::default()));
    let a = proxy_over_with_metrics(&client, CAP, Some(Arc::clone(&a_sync)), &a_metrics)
        .with_index_scan(SERIAL_SCAN)
        .with_fleet_config(fleet_config(&origin, &bucket, "fleet-slow-a", &ports))
        .with_recovery_config(stall_bounds())
        .expect("finite slow-builder recovery bounds");
    let b = proxy_over_with_metrics(&client, CAP, Some(Arc::clone(&b_sync)), &b_metrics)
        .with_index_scan(SERIAL_SCAN)
        .with_fleet_config(fleet_config(&origin, &bucket, "fleet-slow-b", &ports))
        .with_recovery_config(stall_bounds())
        .expect("finite slow-builder recovery bounds");
    mutual_alive(&a_sync, "fleet-slow-a", &b_sync, "fleet-slow-b").await;
    let before = Counts::take(&origin);
    let started = Instant::now();
    tokio::join!(
        a.start_fleet_coherence(std::slice::from_ref(&bucket)),
        b.start_fleet_coherence(std::slice::from_ref(&bucket)),
    );
    let ready = ready_all_by(&[&a, &b], &bucket, Instant::now() + SLOW_READY_DEADLINE).await;
    let cost = Counts::take(&origin).since(before);
    assert!(
        started.elapsed() > Duration::from_millis(stall_bounds().total_ms),
        "the builder's scan must outlast every fixed bound"
    );
    assert_eq!(
        cost.list,
        SLOW_ROWS.div_ceil(1000) as u64,
        "the fleet made exactly one pass of LIST pages: {cost:?}"
    );
    assert_eq!(
        counter(&a_metrics, "recovery_origin_scans") + counter(&b_metrics, "recovery_origin_scans"),
        1,
        "only the builder scanned the origin"
    );
    assert_eq!(indexed_rows(&a_metrics), SLOW_ROWS as u64);
    assert_eq!(indexed_rows(&b_metrics), SLOW_ROWS as u64);
    cost.assert_no_writes();
    println!(
        "fleet_bootstrap slow_builder={cost:?} both_ready_ms={}",
        ready.as_millis()
    );
    assert!(origin.ops.writes().is_empty(), "no origin control writes");
}

/// The production index: 800 pages of 1,000 rows. At 250 ms a page the
/// builder's scan runs over three minutes, past the unshrunk production
/// bounds it must outlive: the 60 s claim episode, the 30 s donor wait and the
/// 60 s recovery attempt. (Production lists about 1.5 pages a second, 793k rows
/// in 8.5 minutes; the bounds restart on every page either way.) Its Ready
/// recapture then clones and encodes a production-sized image.
const PACED_ROWS: usize = 800_000;
const PACED_PAGE: Duration = Duration::from_millis(250);
const PACED_READY_DEADLINE: Duration = Duration::from_mins(10);
/// How long the loaded pair runs on after both serve locally.
const PACED_SETTLE: Duration = Duration::from_secs(15);
/// Ready recaptures the load may fail and Groupnet retry, after its backoff,
/// beyond the one after the scan and one per lease lapse. No member joins or
/// leaves, so SWIM churn alone must never add more.
const PACED_RETRIES: u64 = 1;

/// When one node first counted an origin scan and first served locally,
/// from the pair's common start.
#[derive(Clone, Copy, Debug, Default)]
struct Milestones {
    scanned: Option<Duration>,
    ready: Option<Duration>,
}

/// One node on a runtime of its own with one worker thread, as the binary's
/// `#[tokio::main]` runs in a pod limited to one CPU.
struct Pod(Option<tokio::runtime::Runtime>);

impl Pod {
    fn new(name: &str) -> Self {
        Self(Some(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .thread_name(name)
                .enable_all()
                .build()
                .expect("pod runtime"),
        ))
    }

    fn handle(&self) -> tokio::runtime::Handle {
        self.0.as_ref().expect("live pod runtime").handle().clone()
    }

    /// Hold the pod's only worker for `pause`: its timers, gossip and
    /// bootstrap worker all run late, as on a throttled, overloaded node.
    fn stall(&self, pause: Duration) {
        drop(
            self.handle()
                .spawn(async move { std::thread::sleep(pause) }),
        );
    }
}

impl Drop for Pod {
    fn drop(&mut self) {
        if let Some(runtime) = self.0.take() {
            runtime.shutdown_background();
        }
    }
}

/// Build one fleet node on `pod`, so every task it spawns runs there.
async fn pod_node(
    pod: &Pod,
    origin: &Arc<Origin>,
    (name, udp): (&str, u16),
    (peer, peer_udp): (&str, u16),
    ports: [(&'static str, u16); 2],
    metrics: &Arc<Metrics>,
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
            .with_fleet_config(fleet_config(&origin, origin.bucket(), &name, &ports));
            (sync, proxy)
        })
        .await
        .expect("pod node construction")
}

/// Record each node's first origin scan and first local service until both
/// serve locally or the deadline passes, then keep watching for `settle`.
/// Once a builder scans, stall the pods in turn, the follower first, per
/// `[follower pause, builder pause, period]` in ms, until the watch ends: the
/// load outlasts the scan, through the builder's Ready recapture and the
/// follower's transfer, as on the production pair. Returns whether both
/// finished, the milestones, and the number of stalls.
async fn watch_paced_pair(
    pods: &[Pod; 2],
    nodes: [&CachingProxy; 2],
    metrics: &[Arc<Metrics>; 2],
    bucket: &str,
    started: Instant,
    stalls: [u64; 3],
    settle: Duration,
) -> (bool, [Milestones; 2], usize) {
    let mut seen = [Milestones::default(); 2];
    let mut next_stall = None;
    let mut stalled = 0_usize;
    let mut settled = None;
    let finished = tokio::time::timeout(PACED_READY_DEADLINE, async {
        loop {
            for (index, node) in nodes.iter().enumerate() {
                if seen[index].scanned.is_none()
                    && counter(&metrics[index], "recovery_origin_scans") > 0
                {
                    seen[index].scanned = Some(started.elapsed());
                }
                if seen[index].ready.is_none() && serves_locally(node, bucket) {
                    seen[index].ready = Some(started.elapsed());
                }
            }
            if seen.iter().all(|node| node.ready.is_some())
                && Instant::now() >= *settled.get_or_insert(Instant::now() + settle)
            {
                return;
            }
            if let Some(builder) = seen.iter().position(|node| node.scanned.is_some()) {
                let due =
                    *next_stall.get_or_insert(Instant::now() + Duration::from_millis(stalls[2]));
                if Instant::now() >= due {
                    // Alternate: the follower, then the builder.
                    let (pod, pause) = if stalled.is_multiple_of(2) {
                        (&pods[1 - builder], stalls[0])
                    } else {
                        (&pods[builder], stalls[1])
                    };
                    if pause > 0 {
                        pod.stall(Duration::from_millis(pause));
                    }
                    stalled += 1;
                    next_stall = Some(due + Duration::from_millis(stalls[2] / 2));
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .is_ok();
    (finished, seen, stalled)
}

/// Start fleet coherence on each node inside its own pod.
async fn start_pods(pods: &[Pod; 2], nodes: [&CachingProxy; 2], bucket: &str) {
    let starts = [(&pods[0], nodes[0]), (&pods[1], nodes[1])].map(|(pod, node)| {
        let (node, bucket) = (node.clone(), bucket.to_owned());
        pod.handle().spawn(async move {
            node.start_fleet_coherence(std::slice::from_ref(&bucket))
                .await;
        })
    });
    for start in starts {
        start.await.expect("fleet coherence started");
    }
}

/// Two nodes restart together with the binary's own recovery and claim
/// configuration, strong consistency and the default 2 s lease, against a
/// production-paced origin, and stay loaded throughout. The builder's scan
/// outlasts every fixed bound; the follower waits for all of it, then
/// installs the builder's Ready capture. The pair pays exactly one pass of
/// LIST pages, and the builder recaptures its image a bounded number of
/// times, not once per membership refutation the load causes.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_start_follower_waits_out_a_production_paced_builder() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "s3cache::sync::volatile=info".into()),
        )
        .with_test_writer()
        .try_init();
    // Load as on the production node: after the scan starts, one pod's only
    // worker is held for 1.5 s every 5 s, alternating follower and builder,
    // until the follower has installed and the pair has run on for a while.
    // That is past the 1 s observation bound and long enough for the
    // membership layer to suspect the held pod, but inside the 2 s lease
    // and the 3 s claim TTL. [follower pause, builder pause, period] in ms.
    let stalls: [u64; 3] = [1_500, 1_500, 10_000];
    let origin = Origin::start("fleet-paced-builder").await;
    origin.serve_synthetic_listing(PACED_ROWS);
    origin.delay_lists(PACED_PAGE);
    let bucket = origin.bucket().to_owned();
    let ports = [
        ("fleet-paced-a", free_tcp_port()),
        ("fleet-paced-b", free_tcp_port()),
    ];
    let (a_udp, b_udp) = (free_udp_port(), free_udp_port());
    let pods = [Pod::new("fleet-paced-a"), Pod::new("fleet-paced-b")];
    let metrics = [Arc::new(Metrics::default()), Arc::new(Metrics::default())];
    let (a_sync, a) = pod_node(
        &pods[0],
        &origin,
        ("fleet-paced-a", a_udp),
        ("fleet-paced-b", b_udp),
        ports,
        &metrics[0],
    )
    .await;
    let (b_sync, b) = pod_node(
        &pods[1],
        &origin,
        ("fleet-paced-b", b_udp),
        ("fleet-paced-a", a_udp),
        ports,
        &metrics[1],
    )
    .await;
    mutual_alive(&a_sync, "fleet-paced-a", &b_sync, "fleet-paced-b").await;
    let before = Counts::take(&origin);
    let started = Instant::now();
    start_pods(&pods, [&a, &b], &bucket).await;
    let (finished, seen, stalled) = watch_paced_pair(
        &pods,
        [&a, &b],
        &metrics,
        &bucket,
        started,
        stalls,
        PACED_SETTLE,
    )
    .await;
    let cost = Counts::take(&origin).since(before);
    let count = |name| metrics.each_ref().map(|metrics| counter(metrics, name));
    let (scans, recaptures) = (
        count("recovery_origin_scans"),
        count("recovery_ready_recaptures"),
    );
    let rows = metrics.each_ref().map(|metrics| indexed_rows(metrics));
    let lapses = [&a, &b].map(|node| {
        node.recovery_status()
            .map_or(0, |status| status.state.covered_lapses)
    });
    let report = format!(
        "seen={seen:?} scans={scans:?} recaptures={recaptures:?} lapses={lapses:?} \
         rows={rows:?} stalls={stalled} {cost:?}"
    );
    println!("fleet_bootstrap paced_builder {report}");
    assert!(finished, "both nodes serve locally in time: {report}");
    assert_eq!(
        scans.iter().sum::<u64>(),
        1,
        "exactly one origin scan across the pair: {report}"
    );
    let builder = usize::from(scans[1] == 1);
    let follower = 1 - builder;
    // One recapture after the scan, one after each lease lapse the load
    // causes, and at most one paced retry if the load fails an attempt; not
    // one per suspicion and refutation.
    assert!(
        recaptures[builder] <= lapses[builder] + 1 + PACED_RETRIES && recaptures[follower] == 0,
        "the builder's Ready recapture is paced, not retried on every refutation: {report}"
    );
    let built = seen[builder].ready.expect("the builder served locally");
    assert!(
        built > Duration::from_mins(3),
        "the builder's scan outlasts three minutes and every fixed bound: {report}"
    );
    assert!(
        seen[follower].ready.expect("the follower served locally") >= built,
        "the follower waited for the whole scan: {report}"
    );
    assert_eq!(
        cost.list,
        PACED_ROWS.div_ceil(1000) as u64,
        "one pass of LIST pages; the follower installed with none: {report}"
    );
    assert_eq!(cost.list, cost.successful_list);
    assert_eq!(
        rows, [PACED_ROWS as u64; 2],
        "the follower installed the builder's Ready capture: {report}"
    );
    cost.assert_no_writes();
}

/// The production index has about 798,000 rows, eight times the old 100,000-row
/// image cap. `MinIO` cannot be seeded with that many objects in test time, so
/// the counting forwarder answers the donor's LIST pages from a synthetic
/// listing of production-shaped rows. Everything after those pages is the
/// production path: the donor's measured capture at C, the bulk transfer, and
/// the joiner's guarded install.
const PRODUCTION_ROWS: usize = 800_000;
const PRODUCTION_READY_DEADLINE: Duration = Duration::from_mins(5);

/// One page of `bucket` after `start_after`, as `(key, unquoted ETag, size)`,
/// through the joiner.
async fn local_page(
    proxy: &CachingProxy,
    bucket: &str,
    start_after: &str,
) -> Vec<(String, String, i64)> {
    proxy
        .list_objects_v2(request(ListObjectsV2Input {
            bucket: bucket.to_owned(),
            start_after: Some(start_after.to_owned()),
            max_keys: Some(1_000),
            ..Default::default()
        }))
        .await
        .expect("local LIST page")
        .output
        .contents
        .into_iter()
        .flatten()
        .map(|object| {
            (
                object.key.expect("listed key"),
                object.e_tag.expect("listed ETag").into_value(),
                object.size.expect("listed size"),
            )
        })
        .collect()
}

/// A joiner installs a production-sized donor image without listing the
/// origin, then answers a page from deep in the keyspace exactly as one
/// bounded origin LIST does. `RUST_LOG` overrides the fleet's info logs,
/// which record the image's encoded bytes and the transfer time.
///
/// Both nodes share one multi-threaded runtime, as the binary runs. The
/// capture at C holds the recovery fence for its whole clone, and each node's
/// lease watcher reads that fence's state; on one thread a debug-build clone
/// of this size would stall both nodes' gossip for over a second.
#[tokio::test(flavor = "multi_thread")]
async fn peer_bootstrap_transfers_a_production_scale_index() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "s3cache::sync::volatile::fleet=info".into()),
        )
        .with_test_writer()
        .try_init();
    let origin = Origin::start("fleet-production-index").await;
    origin.serve_synthetic_listing(PRODUCTION_ROWS);
    let bucket = origin.bucket().to_owned();
    let client = origin.counted_client();
    let ports = [
        ("fleet-scale-a", free_tcp_port()),
        ("fleet-scale-b", free_tcp_port()),
    ];
    let (a_udp, b_udp) = (free_udp_port(), free_udp_port());
    let a_sync = gossip_node("fleet-scale-a", a_udp, &[("fleet-scale-b", b_udp)]).await;
    let a_metrics = Arc::new(Metrics::default());
    let a = proxy_over_with_metrics(&client, CAP, Some(Arc::clone(&a_sync)), &a_metrics)
        .with_index_scan(SERIAL_SCAN)
        .with_fleet_config(fleet_config(&origin, &bucket, "fleet-scale-a", &ports));
    let before_scan = Counts::take(&origin);
    a.start_fleet_coherence(std::slice::from_ref(&bucket)).await;
    let scan = ready_by(&a, &bucket, Instant::now() + PRODUCTION_READY_DEADLINE).await;
    let scan_cost = Counts::take(&origin).since(before_scan);
    assert_eq!(indexed_rows(&a_metrics), PRODUCTION_ROWS as u64);
    assert_eq!(
        scan_cost.list,
        PRODUCTION_ROWS.div_ceil(1000) as u64,
        "the donor scanned the origin once: {scan_cost:?}"
    );

    let before_join = Counts::take(&origin);
    let b_sync = gossip_node("fleet-scale-b", b_udp, &[("fleet-scale-a", a_udp)]).await;
    let b_metrics = Arc::new(Metrics::default());
    let b = proxy_over_with_metrics(&client, CAP, Some(Arc::clone(&b_sync)), &b_metrics)
        .with_index_scan(SERIAL_SCAN)
        .with_fleet_config(fleet_config(&origin, &bucket, "fleet-scale-b", &ports));
    mutual_alive(&a_sync, "fleet-scale-a", &b_sync, "fleet-scale-b").await;
    b.start_fleet_coherence(std::slice::from_ref(&bucket)).await;
    let join = ready_by(&b, &bucket, Instant::now() + PRODUCTION_READY_DEADLINE).await;
    let join_cost = Counts::take(&origin).since(before_join);
    assert_eq!(
        join_cost.list, 0,
        "the joiner installed the peer image instead of listing: {join_cost:?}"
    );
    assert_eq!(counter(&b_metrics, "recovery_origin_scans"), 0);
    assert_eq!(indexed_rows(&b_metrics), indexed_rows(&a_metrics));
    join_cost.assert_no_writes();

    // One bounded origin check: the joiner answers a page from the middle of
    // the keyspace locally, and one origin LIST of that page agrees with it.
    let after = synthetic_key(PRODUCTION_ROWS / 2 + 7);
    let before_check = Counts::take(&origin);
    let local = local_page(&b, &bucket, &after).await;
    assert_eq!(Counts::take(&origin).since(before_check).list, 0);
    let listed = client
        .list_objects_v2()
        .bucket(&bucket)
        .start_after(&after)
        .max_keys(1_000)
        .send()
        .await
        .expect("one origin LIST page");
    assert_eq!(Counts::take(&origin).since(before_check).list, 1);
    let from_origin: Vec<_> = listed
        .contents()
        .iter()
        .map(|object| {
            (
                object.key().expect("origin key").to_owned(),
                object
                    .e_tag()
                    .expect("origin ETag")
                    .trim_matches('"')
                    .to_owned(),
                object.size().expect("origin size"),
            )
        })
        .collect();
    assert_eq!(local.len(), 1_000);
    assert_eq!(local, from_origin);
    println!(
        "fleet_bootstrap production_join rows={PRODUCTION_ROWS} donor_scan_ms={} \
         join_ready_ms={} scan={scan_cost:?} join={join_cost:?}",
        scan.as_millis(),
        join.as_millis()
    );
}
