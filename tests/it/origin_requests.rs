//! Origin request accounting against a real S3 (`MinIO`): every request the proxy sends
//! upstream lands in exactly one `origin_class_*` counter, and the answers that cost a
//! request without moving data — a 404 GET, a 404 HEAD, a refused conditional PUT —
//! each land in their own counter too. The counting forwarder in front of `MinIO` is the
//! independent witness the proxy's own numbers are checked against.

use crate::common;

use std::sync::Arc;

use common::{Origin, counter, get, head, proxy_over_with_metrics, put, put_conditional, request};
use s3cache::metrics::Metrics;
use s3s::S3;
use s3s::dto::{ETagCondition, GetObjectInput};

const COUNTERS: [&str; 7] = [
    "origin_class_a_requests",
    "origin_class_b_requests",
    "origin_free_requests",
    "origin_get_not_found",
    "origin_head_not_found",
    "origin_write_precondition_failed",
    "origin_write_refused",
];

/// The `origin_*` counters' movement since `before`, by name; zero movers omitted.
fn moved(metrics: &Metrics, before: &[u64; 7]) -> Vec<(&'static str, u64)> {
    COUNTERS
        .iter()
        .zip(before)
        .map(|(name, then)| (*name, counter(metrics, name) - then))
        .filter(|(_, delta)| *delta > 0)
        .collect()
}

fn snapshot(metrics: &Metrics) -> [u64; 7] {
    COUNTERS.map(|name| counter(metrics, name))
}

/// The proxy's own count of requests it sent, against the forwarder's count of what
/// arrived: reads are Class B, everything the forwarder saw otherwise is Class A or free.
fn assert_reconciles(metrics: &Metrics, origin: &Origin) {
    assert_eq!(
        counter(metrics, "origin_class_b_requests"),
        origin.ops.get() + origin.ops.head(),
        "every origin GET/HEAD is a counted Class B request"
    );
    assert_eq!(
        counter(metrics, "origin_class_a_requests") + counter(metrics, "origin_free_requests"),
        origin.ops.list()
            + origin.ops.put()
            + origin.ops.copy()
            + origin.ops.delete()
            + origin.ops.other(),
        "every other origin request is a counted Class A or free request"
    );
}

#[tokio::test]
async fn forwarded_requests_are_counted_by_class_and_by_unproductive_answer() {
    let origin = Origin::start("origin-requests").await;
    let bucket = origin.bucket();
    origin.seed("obj", b"hello").await;
    let metrics = Arc::new(Metrics::default());
    // No background sync: the bucket is never indexed, so every read goes upstream.
    let proxy = proxy_over_with_metrics(&origin.counted_client(), 1024 * 1024, None, &metrics);

    let before = snapshot(&metrics);
    let err = proxy
        .get_object(request(GetObjectInput {
            bucket: bucket.to_owned(),
            key: "ghost".to_owned(),
            ..Default::default()
        }))
        .await
        .expect_err("the key does not exist");
    assert_eq!(err.status_code().map(|s| s.as_u16()), Some(404));
    assert_eq!(
        moved(&metrics, &before),
        [("origin_class_b_requests", 1), ("origin_get_not_found", 1)],
        "a forwarded 404 GET is one Class B request and one origin_get_not_found"
    );
    assert_reconciles(&metrics, &origin);

    let before = snapshot(&metrics);
    let err = head(&proxy, bucket, "ghost")
        .await
        .expect_err("the key does not exist");
    assert_eq!(err.status_code().map(|s| s.as_u16()), Some(404));
    assert_eq!(
        moved(&metrics, &before),
        [("origin_class_b_requests", 1), ("origin_head_not_found", 1)],
        "a forwarded 404 HEAD is one Class B request and one origin_head_not_found"
    );
    assert_reconciles(&metrics, &origin);

    let before = snapshot(&metrics);
    let err = put_conditional(
        &proxy,
        bucket,
        "obj",
        b"other",
        Some(ETagCondition::Any),
        None,
    )
    .await
    .expect_err("put_if_absent over an existing key is refused");
    assert_eq!(err.status_code().map(|s| s.as_u16()), Some(412));
    assert_eq!(
        moved(&metrics, &before),
        [
            ("origin_class_a_requests", 1),
            ("origin_write_precondition_failed", 1)
        ],
        "a refused put_if_absent is one Class A request and one origin_write_precondition_failed"
    );
    assert_reconciles(&metrics, &origin);

    // Successful paths move only their class counter.
    let before = snapshot(&metrics);
    assert_eq!(&get(&proxy, bucket, "obj").await[..], b"hello");
    let after_get = moved(&metrics, &before);
    assert!(
        matches!(after_get[..], [("origin_class_b_requests", n)] if n > 0),
        "a successful forwarded GET moves only Class B: {after_get:?}"
    );
    let before = snapshot(&metrics);
    put(&proxy, bucket, "fresh", b"new").await;
    assert_eq!(
        moved(&metrics, &before),
        [("origin_class_a_requests", 1)],
        "a successful PUT is one Class A request and nothing else"
    );
    assert_reconciles(&metrics, &origin);
}
