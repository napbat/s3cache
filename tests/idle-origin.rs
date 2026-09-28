//! Local-only measurement of origin requests during an idle two-node cluster.
//!
//! Run with `cargo test --locked --test idle-origin -- --ignored --nocapture` after
//! building the pinned `MinIO` test image. The seed writes bypass the counter.

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use common::{Origin, counter, gossip_pair, request, wait_for_index};
use futures::{StreamExt as _, stream};
use s3cache::cache::proxy::{CacheConfig, CachingProxy};
use s3cache::metrics::Metrics;
use s3s::S3 as _;
use s3s::dto::ListObjectsV2Input;

const SHARDS: usize = 2_048;
const WINDOWS: usize = 3;
const WINDOW: Duration = Duration::from_secs(20);
const LISTS_PER_TICK: usize = 64;
const TICK: Duration = Duration::from_secs(1);
const HOT_BYTES: u64 = 2 * 1024 * 1024 * 1024;
const MAX_OBJECT_BYTES: usize = 32 * 1024 * 1024;

#[derive(Clone, Copy, Debug)]
struct Sample {
    list: u64,
    get: u64,
    head: u64,
    put: u64,
    delete: u64,
    copy: u64,
    other: u64,
    list_index_a: u64,
    list_index_b: u64,
    list_passthrough_a: u64,
    list_passthrough_b: u64,
    licence_a: u64,
    licence_b: u64,
    freshness_a: u64,
    freshness_b: u64,
    lapse_retain_a: u64,
    lapse_retain_b: u64,
    lapse_fallback_a: u64,
    lapse_fallback_b: u64,
    gaps_a: u64,
    gaps_b: u64,
}

impl Sample {
    fn take(origin: &Origin, a: &Metrics, b: &Metrics) -> Self {
        Self {
            list: origin.ops.list(),
            get: origin.ops.get(),
            head: origin.ops.head(),
            put: origin.ops.put(),
            delete: origin.ops.delete(),
            copy: origin.ops.copy(),
            other: origin.ops.other(),
            list_index_a: counter(a, "list_from_index"),
            list_index_b: counter(b, "list_from_index"),
            list_passthrough_a: counter(a, "list_passthrough"),
            list_passthrough_b: counter(b, "list_passthrough"),
            licence_a: counter(a, "read_licence_bypasses"),
            licence_b: counter(b, "read_licence_bypasses"),
            freshness_a: counter(a, "read_freshness_bypasses"),
            freshness_b: counter(b, "read_freshness_bypasses"),
            lapse_retain_a: counter(a, "lapse_barrier_retains"),
            lapse_retain_b: counter(b, "lapse_barrier_retains"),
            lapse_fallback_a: counter(a, "lapse_barrier_fallbacks"),
            lapse_fallback_b: counter(b, "lapse_barrier_fallbacks"),
            gaps_a: counter(a, "feed_gaps"),
            gaps_b: counter(b, "feed_gaps"),
        }
    }

    fn report(self, before: Self, label: &str, elapsed: Duration) {
        let d = |after: u64, before: u64| after.saturating_sub(before);
        println!(
            "idle_origin {label} elapsed_s={:.3} origin_list={} origin_get={} origin_head={} origin_put={} origin_delete={} origin_copy={} origin_other={} list_index_a={} list_index_b={} list_passthrough_a={} list_passthrough_b={} licence_a={} licence_b={} freshness_a={} freshness_b={} lapse_retain_a={} lapse_retain_b={} lapse_fallback_a={} lapse_fallback_b={} feed_gaps_a={} feed_gaps_b={}",
            elapsed.as_secs_f64(),
            d(self.list, before.list),
            d(self.get, before.get),
            d(self.head, before.head),
            d(self.put, before.put),
            d(self.delete, before.delete),
            d(self.copy, before.copy),
            d(self.other, before.other),
            d(self.list_index_a, before.list_index_a),
            d(self.list_index_b, before.list_index_b),
            d(self.list_passthrough_a, before.list_passthrough_a),
            d(self.list_passthrough_b, before.list_passthrough_b),
            d(self.licence_a, before.licence_a),
            d(self.licence_b, before.licence_b),
            d(self.freshness_a, before.freshness_a),
            d(self.freshness_b, before.freshness_b),
            d(self.lapse_retain_a, before.lapse_retain_a),
            d(self.lapse_retain_b, before.lapse_retain_b),
            d(self.lapse_fallback_a, before.lapse_fallback_a),
            d(self.lapse_fallback_b, before.lapse_fallback_b),
            d(self.gaps_a, before.gaps_a),
            d(self.gaps_b, before.gaps_b),
        );
    }
}

fn shard_prefix(shard: usize) -> String {
    format!("tenant/sample/shards/{shard:04}/")
}

async fn list_one(proxy: &CachingProxy, bucket: &str, shard: usize) {
    let prefix = shard_prefix(shard);
    let expected = format!("{prefix}manifest");
    let response = proxy
        .list_objects_v2(request(ListObjectsV2Input {
            bucket: bucket.to_owned(),
            prefix: Some(prefix),
            max_keys: Some(8),
            ..Default::default()
        }))
        .await
        .expect("inventory LIST succeeds");
    let keys: Vec<_> = response
        .output
        .contents
        .into_iter()
        .flatten()
        .filter_map(|item| item.key)
        .collect();
    assert_eq!(keys, [expected], "inventory prefix has its manifest");
}

/// Measure three consecutive 20-second windows after the index and gossip settle.
/// No client writes occur during the windows. A LIST reaching the origin is a
/// measured Class A request. The test checks for unexpected object writes.
#[tokio::test]
#[ignore = "local MinIO and at least 60 seconds of wall-clock measurement"]
async fn two_node_strong_idle_origin_requests() {
    let origin = Origin::start("idle-origin").await;
    let bucket = origin.bucket().to_owned();
    stream::iter(0..SHARDS)
        .for_each_concurrent(32, |shard| {
            let origin = Arc::clone(&origin);
            async move {
                origin
                    .seed(&format!("{}manifest", shard_prefix(shard)), b"x")
                    .await;
            }
        })
        .await;

    let startup = Sample::take(&origin, &Metrics::default(), &Metrics::default());
    let (sync_a, sync_b) = gossip_pair("idle-origin-a", "idle-origin-b").await;
    let metrics_a = Arc::new(Metrics::default());
    let metrics_b = Arc::new(Metrics::default());
    let client = origin.counted_client();
    let build_node = |sync, metrics: &Arc<Metrics>| {
        CachingProxy::new(
            s3s_aws::Proxy::from(client.clone()),
            client.clone(),
            CacheConfig {
                cache_bytes: HOT_BYTES,
                max_obj_bytes: MAX_OBJECT_BYTES,
            },
            None,
            Some(sync),
            Arc::clone(metrics),
        )
    };
    let node_a = build_node(sync_a, &metrics_a);
    let node_b = build_node(sync_b, &metrics_b);
    for node in [&node_a, &node_b] {
        node.start_coherence(std::slice::from_ref(&bucket));
        node.spawn_background_sync(vec![bucket.clone()]);
    }
    let warm_started = Instant::now();
    wait_for_index(&node_a, &origin, &bucket).await;
    wait_for_index(&node_b, &origin, &bucket).await;
    let warmed = Sample::take(&origin, &metrics_a, &metrics_b);
    warmed.report(startup, "index_warmup", warm_started.elapsed());

    let settle_started = Instant::now();
    tokio::time::sleep(Duration::from_secs(5)).await;
    let settled = Sample::take(&origin, &metrics_a, &metrics_b);
    settled.report(warmed, "gossip_settle", settle_started.elapsed());
    println!(
        "idle_origin config nodes=2 consistency=strong lease_ms=2000 seeded_shards={SHARDS} max_object_bytes={MAX_OBJECT_BYTES} hot_bytes_per_node={HOT_BYTES} warm_disk=disabled windows={WINDOWS} window_s=20 lists_per_tick={LISTS_PER_TICK} tick_ms=1000"
    );

    let mut prior = settled;
    let mut next_shard = 0;
    for window in 0..WINDOWS {
        let started = Instant::now();
        let deadline = started + WINDOW;
        while Instant::now() < deadline {
            let tick = Instant::now();
            for _ in 0..LISTS_PER_TICK {
                let shard = next_shard % SHARDS;
                let node = if next_shard % 2 == 0 {
                    &node_a
                } else {
                    &node_b
                };
                list_one(node, &bucket, shard).await;
                next_shard += 1;
            }
            tokio::time::sleep_until(tokio::time::Instant::from_std(tick + TICK)).await;
        }
        let current = Sample::take(&origin, &metrics_a, &metrics_b);
        current.report(prior, &format!("window_{}", window + 1), started.elapsed());
        assert_eq!(current.put, settled.put, "idle proxy wrote to origin");
        assert_eq!(
            current.delete, settled.delete,
            "idle proxy deleted at origin"
        );
        assert_eq!(current.copy, settled.copy, "idle proxy copied at origin");
        prior = current;
    }
    assert_eq!(
        prior.list, settled.list,
        "idle inventory reached origin LIST"
    );
    assert_eq!(prior.get, settled.get, "idle inventory reached origin GET");
    assert_eq!(
        prior.head, settled.head,
        "idle inventory reached origin HEAD"
    );
    assert_eq!(
        prior.other, settled.other,
        "idle inventory reached another origin API"
    );
}
