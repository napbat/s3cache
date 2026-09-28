//! Startup tests with a real S3 origin and a blocked background index scan.
//!
//! A gossip node forwards reads to origin until Groupnet's one cold scan
//! finishes. An incomplete index still forwards LIST and cannot license a
//! retained warm body from an earlier process.

mod common;

use std::sync::Arc;

use common::{
    Origin, WarmDir, counter, delete, free_udp_port, get, gossip_node, gossip_pair, list,
    proxy_over, proxy_over_with_metrics, put_typed, request, warm_proxy_over,
};
use groupnet::consistency::volatile_recovery::{RecoveryConfig, RecoveryStage};
use s3cache::cache::proxy::CachingProxy;
use s3cache::metrics::Metrics;
use s3s::S3;
use s3s::dto::GetObjectInput;

const CAP: usize = 1024 * 1024;

fn short_recovery() -> RecoveryConfig {
    RecoveryConfig {
        max_members: 16,
        max_member_bytes: 128,
        max_barrier_rounds: 4,
        total_ms: 5_000,
        attempt_ms: 250,
        settle_ms: 2_000,
        poll_ms: 50,
    }
}

/// An operation timeout creates a fresh guarded scan within the same total
/// budget. A delayed page from the first scan cannot resurrect a deleted key.
#[tokio::test]
async fn timed_out_scan_retries_without_publishing_a_stale_page() {
    let origin = Origin::start("startup-retry-page").await;
    origin.seed("doomed", b"old").await;
    let bucket = origin.bucket().to_owned();
    let sync = gossip_node("startup-retry-node", free_udp_port(), &[]).await;
    let metrics = Arc::new(Metrics::default());
    let proxy = proxy_over_with_metrics(&origin.counted_client(), CAP, Some(sync), &metrics)
        .with_recovery_config(short_recovery())
        .expect("bounded recovery configuration");

    origin.pause_next_list();
    proxy.start_coherence(std::slice::from_ref(&bucket));
    origin.wait_for_paused_list().await;
    origin
        .client()
        .delete_object()
        .bucket(&bucket)
        .key("doomed")
        .send()
        .await
        .expect("delete the old key before the retry");

    eventually!(
        "Groupnet retries the timed-out origin scan",
        counter(&metrics, "recovery_origin_scans") >= 2
    );
    eventually!(
        "the replacement scan reaches a recovery proof",
        proxy
            .recovery_status()
            .is_some_and(|status| status.state.stage == RecoveryStage::Ready)
    );
    origin.release_paused_list();
    assert!(list(&proxy, &bucket).await.is_empty());
    assert!(origin.stored("doomed").await.is_none());
}

/// Exhausting the bounded episode stays origin-only. A deliberate restart
/// starts a new generation after the origin has recovered.
#[tokio::test]
async fn prolonged_list_outage_requires_explicit_recovery_restart() {
    let origin = Origin::start("startup-outage-restart").await;
    origin.seed("present", b"origin").await;
    let bucket = origin.bucket().to_owned();
    let sync = gossip_node("startup-outage-node", free_udp_port(), &[]).await;
    let metrics = Arc::new(Metrics::default());
    let proxy = proxy_over_with_metrics(&origin.counted_client(), CAP, Some(sync), &metrics)
        .with_recovery_config(short_recovery())
        .expect("bounded recovery configuration");

    origin.fail_lists(true);
    proxy.start_coherence(std::slice::from_ref(&bucket));
    eventually!(
        "the origin outage exceeds the total recovery budget",
        proxy
            .recovery_status()
            .is_some_and(|status| status.state.stage == RecoveryStage::OriginOnly)
    );
    assert!(counter(&metrics, "recovery_origin_scans") >= 2);
    origin.fail_lists(false);
    proxy.restart_coherence().expect("start a new episode");
    eventually!(
        "the recovered origin produces an affirmed index",
        proxy
            .recovery_status()
            .is_some_and(|status| status.state.stage == RecoveryStage::Ready)
    );
    assert_eq!(list(&proxy, &bucket).await, ["present"]);
}

/// The default cold bootstrap has no local authority until its scan finishes:
/// repeated positive GETs remain origin requests, though they may fill a body
/// for later validation against the completed index.
async fn assert_origin_gets_during_boot(
    proxy: &CachingProxy,
    origin: &Origin,
    metrics: &Metrics,
    bucket: &str,
    key: &str,
    expected: &[u8],
) {
    let bypasses = counter(metrics, "read_licence_bypasses");
    let gets = origin.ops.get();
    for attempt in 1..=2 {
        assert_eq!(get(proxy, bucket, key).await.as_ref(), expected);
        assert_eq!(origin.ops.get(), gets + attempt);
    }
    assert!(counter(metrics, "read_licence_bypasses") >= bypasses + 2);
}

/// Hold one actual origin LIST response. This keeps the index scan incomplete while
/// both nodes start, read, and mutate objects through the real S3 origin.
#[tokio::test]
async fn strong_boot_forwards_gets_until_the_index_finishes() {
    let origin = Origin::start("startup-early-body").await;
    let bucket = origin.bucket();
    origin.seed("body", b"version-1").await;
    origin.seed("doomed", b"soon-deleted").await;

    let (sync_a, sync_b) = gossip_pair("startup-body-a", "startup-body-b").await;
    let client = origin.counted_client();
    let node_a = proxy_over(&client, CAP, Some(sync_a));
    let metrics_b = Arc::new(Metrics::default());
    let node_b = proxy_over_with_metrics(&client, CAP, Some(sync_b), &metrics_b);
    origin.pause_next_list();
    node_b.start_coherence(&[bucket.to_owned()]);
    origin.wait_for_paused_list().await;
    node_a.start_coherence(&[bucket.to_owned()]);
    node_b.spawn_background_sync(vec![bucket.to_owned()]);
    node_a.spawn_background_sync(vec![bucket.to_owned()]);

    let lists = origin.ops.list();
    let passthroughs = counter(&metrics_b, "list_passthrough");
    assert_eq!(list(&node_b, bucket).await, ["body", "doomed"]);
    assert_eq!(
        counter(&metrics_b, "list_passthrough"),
        passthroughs + 1,
        "the blocked index cannot answer LIST"
    );
    assert!(origin.ops.list() > lists, "the LIST reached the origin");

    assert_origin_gets_during_boot(&node_b, &origin, &metrics_b, bucket, "body", b"version-1")
        .await;

    // The origin has answered this GET, but the cache has not received the
    // response. A peer delete must fence the pending body before it can commit.
    origin.pause_next_get();
    let pending_node = node_b.clone();
    let pending_bucket = bucket.to_owned();
    let pending = tokio::spawn(async move { get(&pending_node, &pending_bucket, "doomed").await });
    origin.wait_for_paused_get().await;
    delete(&node_a, bucket, "doomed").await;
    origin.release_paused_get();
    assert_eq!(
        pending.await.expect("the in-flight GET completes"),
        "soon-deleted"
    );
    let stale = node_b
        .get_object(request(GetObjectInput {
            bucket: bucket.to_owned(),
            key: "doomed".into(),
            ..Default::default()
        }))
        .await
        .expect_err("the deleted body must not be cached by the late GET");
    assert_eq!(stale.status_code().map(|status| status.as_u16()), Some(404));

    let gets = origin.ops.get();
    put_typed(&node_a, bucket, "body", b"version-2", "text/x-fixture").await;
    assert_eq!(get(&node_b, bucket, "body").await, "version-2");
    assert_eq!(
        origin.ops.get(),
        gets + 1,
        "the peer overwrite invalidates the early body"
    );
    assert_eq!(get(&node_b, bucket, "body").await, "version-2");
    assert_eq!(origin.ops.get(), gets + 2, "boot still forwards to origin");

    delete(&node_a, bucket, "body").await;
    let error = node_b
        .get_object(request(GetObjectInput {
            bucket: bucket.to_owned(),
            key: "body".into(),
            ..Default::default()
        }))
        .await
        .expect_err("the peer delete must invalidate the body");
    assert_eq!(error.status_code().map(|status| status.as_u16()), Some(404));

    origin.release_paused_list();
}

/// A disk body from a prior process has no proof in the current lease epoch. The
/// new node must fetch the current origin body while its LIST index is incomplete.
#[tokio::test]
async fn strong_boot_does_not_serve_an_old_disk_body_without_proof() {
    let origin = Origin::start("startup-warm-proof").await;
    let bucket = origin.bucket();
    let dir = WarmDir::new("startup-warm-proof");
    origin.seed("body", b"version-1").await;

    {
        let metrics = Arc::new(Metrics::default());
        let previous = warm_proxy_over(&origin.counted_client(), CAP, None, &dir, &metrics);
        assert_eq!(get(&previous, bucket, "body").await, "version-1");
        eventually!("the old body to reach the disk tier", dir.files() == 1);
    }
    // A peer stays online while this node is absent. Its write is in the feed
    // before the restarted node binds its gossip socket.
    let (port_a, port_b) = (free_udp_port(), free_udp_port());
    let sync_a = gossip_node("startup-warm-a", port_a, &[("startup-warm-b", port_b)]).await;
    let client = origin.counted_client();
    let node_a = proxy_over(&client, CAP, Some(sync_a));
    node_a.start_coherence(&[bucket.to_owned()]);
    put_typed(&node_a, bucket, "body", b"version-2", "text/x-fixture").await;

    let sync_b = gossip_node("startup-warm-b", port_b, &[("startup-warm-a", port_a)]).await;
    let metrics_b = Arc::new(Metrics::default());
    let node_b = warm_proxy_over(&client, CAP, Some(sync_b), &dir, &metrics_b);
    origin.pause_next_list();
    node_b.start_coherence(&[bucket.to_owned()]);
    node_b.spawn_background_sync(vec![bucket.to_owned()]);
    origin.wait_for_paused_list().await;

    assert_origin_gets_during_boot(&node_b, &origin, &metrics_b, bucket, "body", b"version-2")
        .await;
    let passthroughs = counter(&metrics_b, "list_passthrough");
    assert_eq!(list(&node_b, bucket).await, ["body"]);
    assert_eq!(counter(&metrics_b, "list_passthrough"), passthroughs + 1);

    origin.release_paused_list();
}
