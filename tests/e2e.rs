//! End-to-end tests of the proxy against a **real S3 origin**: `MinIO` in a container,
//! reached through a genuine `aws_sdk_s3` client, with a real `CachingProxy` in front of
//! it and a transparent request counter in between (see `common`).
//!
//! Every test states its property twice — once as what the client sees, once as what the
//! origin was asked for. A cache that returns the right bytes by fetching them again is
//! not a cache, and only the origin's counters can tell the difference. Using `MinIO`
//! rather than a hand-written stand-in is what makes the conditional-write, `ETag` and
//! error-code assertions mean anything: those are the origin's semantics, and the claim
//! under test is that the proxy does not alter them.

mod common;

use std::sync::Arc;

use common::{
    Origin, delete, get, get_if_none_match, get_range, head, head_if_none_match, list, list_entry,
    node, put, put_conditional, put_typed, put_typed_conditional, request, wait_for_index,
};
use http::HeaderValue;
use http::header::IF_NONE_MATCH;
use s3s::dto::{CopyObjectInput, CopySource, ETag, ETagCondition};
use s3s::{S3, S3ErrorCode};
use tokio::sync::Barrier;

/// Once a bucket's background sync completes, LIST is answered from the index: the keys
/// are right, they carry the origin's own size and `ETag`, and the origin is never asked
/// again however many times a client lists.
#[tokio::test]
async fn list_is_served_from_the_index_after_the_warm_up_sync() {
    let origin = Origin::start("e2e-list").await;
    let bucket = origin.bucket();
    for key in ["a", "b/1", "b/2", "c"] {
        origin.seed(key, key.as_bytes()).await;
    }
    let proxy = node(&origin, 1024 * 1024);
    wait_for_index(&proxy, &origin, bucket).await;

    let baseline = origin.ops.list();
    for _ in 0..3 {
        assert_eq!(list(&proxy, bucket).await, ["a", "b/1", "b/2", "c"]);
    }
    assert_eq!(
        list_entry(&proxy, bucket, "b/1").await,
        Some((3, origin.etag("b/1").await)),
        "the indexed entry carries the origin's size and ETag"
    );
    assert_eq!(
        origin.ops.list(),
        baseline,
        "a synced bucket costs the origin no LIST at all"
    );
}

/// Destination conditions on `CopyObject` survive both protocol adapters: AWS-compatible
/// origins get the standard header and R2 gets its `cf-copy-destination-*` equivalent.
/// A successful copy whose result `ETag` matches the indexed source also needs no metadata
/// HEAD. `MinIO` does not implement destination-conditional `CopyObject`, so this structural
/// test proves forwarding; the production R2 check proves rejection of an overwrite.
#[tokio::test]
async fn conditional_copy_headers_reach_origin_and_proved_size_skips_head() {
    let origin = Origin::start("e2e-conditional-copy").await;
    origin.seed("source", b"first source").await;
    let proxy = node(&origin, 1024 * 1024);
    wait_for_index(&proxy, &origin, origin.bucket()).await;

    let copy_request = || {
        let mut input = CopyObjectInput::builder();
        input.set_bucket(origin.bucket().to_owned());
        input.set_key("destination".to_owned());
        input.set_copy_source(CopySource::Bucket {
            bucket: origin.bucket().to_owned().into(),
            key: "source".into(),
            version_id: None,
        });
        let mut req = request(input.build().expect("a complete copy request"));
        req.headers
            .insert(IF_NONE_MATCH, HeaderValue::from_static("*"));
        req
    };

    proxy
        .copy_object(copy_request())
        .await
        .expect("the absent destination is created");
    assert_eq!(
        origin.ops.head(),
        0,
        "an ETag-matched source size avoids the copy metadata HEAD"
    );
    assert_eq!(
        origin.stored("destination").await.as_deref(),
        Some(&b"first source"[..])
    );

    assert_eq!(origin.ops.copy(), 1, "the copy reached the origin once");
    assert_eq!(
        origin.ops.conditional_copy(),
        1,
        "both standard and R2 destination conditions reached the origin"
    );
}

/// The first GET pays for the origin; every one after it is served from the local body
/// cache, bytes intact.
#[tokio::test]
async fn a_get_misses_once_and_is_served_locally_after() {
    let origin = Origin::start("e2e-get").await;
    origin.seed("obj", b"hello world").await;
    let proxy = node(&origin, 1024 * 1024);

    assert_eq!(get(&proxy, origin.bucket(), "obj").await, "hello world");
    assert_eq!(origin.ops.get(), 1, "the miss went to the origin");
    assert_eq!(get(&proxy, origin.bucket(), "obj").await, "hello world");
    assert_eq!(origin.ops.get(), 1, "the hit did not");
}

/// Concurrent misses for one whole object share the tier's per-key fill. Docres can
/// ask for the same cold shard object from many requests at once; sending every waiter
/// to R2 turns one miss into an origin stampede and makes all of them slower.
#[tokio::test]
async fn concurrent_whole_get_misses_share_one_origin_fetch() {
    const READERS: usize = 32;
    let origin = Origin::start("e2e-get-singleflight").await;
    let body = vec![b'x'; 1 << 20];
    origin.seed("obj", &body).await;
    let proxy = Arc::new(node(&origin, body.len() * 2));
    let barrier = Arc::new(Barrier::new(READERS + 1));
    let mut reads = tokio::task::JoinSet::new();

    for _ in 0..READERS {
        let proxy = Arc::clone(&proxy);
        let barrier = Arc::clone(&barrier);
        let bucket = origin.bucket().to_owned();
        reads.spawn(async move {
            barrier.wait().await;
            get(&proxy, &bucket, "obj").await
        });
    }
    barrier.wait().await;
    while let Some(read) = reads.join_next().await {
        assert_eq!(read.expect("reader task"), body);
    }

    assert_eq!(
        origin.ops.get(),
        1,
        "all concurrent misses must share one origin GET"
    );
}

/// An object past `max_obj_bytes` is never cached — it streams through, correctly, every
/// time, and each read is an origin read.
#[tokio::test]
async fn objects_over_the_cap_stream_through_uncached() {
    const CAP: usize = 64;
    let origin = Origin::start("e2e-bypass").await;
    let big = vec![b'x'; CAP * 4];
    origin.seed("big", &big).await;
    let proxy = node(&origin, CAP);
    wait_for_index(&proxy, &origin, origin.bucket()).await;

    for expected_gets in 1..=2 {
        assert_eq!(get(&proxy, origin.bucket(), "big").await, big);
        assert_eq!(
            origin.ops.get(),
            expected_gets,
            "an over-cap object is fetched every time"
        );
    }
}

/// A ranged read of a cached object is sliced locally: the right bytes, a 206-shaped
/// `Content-Range`, and no second origin fetch.
#[tokio::test]
async fn ranged_reads_slice_the_cached_body_locally() {
    let origin = Origin::start("e2e-range").await;
    origin.seed("obj", b"0123456789").await;
    let proxy = node(&origin, 1024 * 1024);

    assert_eq!(get(&proxy, origin.bucket(), "obj").await, "0123456789");
    let fetched = origin.ops.get();

    let (slice, range) = get_range(&proxy, origin.bucket(), "obj", 2, 5).await;
    assert_eq!(slice, "2345");
    assert_eq!(range.as_deref(), Some("bytes 2-5/10"));
    assert_eq!(
        origin.ops.get(),
        fetched,
        "the slice came out of the cached body"
    );
}

/// Issue #1: repeated HEADs of an object whose body was never fetched cost the origin
/// nothing, and a HEAD of a key absent from a synced bucket is an authoritative local
/// 404 that costs nothing either.
///
/// The bootstrap LIST that warmed the index proves the key exists and carries its size,
/// mtime and `ETag`, but says nothing about its `Content-Type` or user metadata — so the
/// *first* HEAD is forwarded rather than answered from a record that would differ from
/// the origin's, and its answer completes the entry. Every HEAD after that is local. The
/// pattern the issue was about — one key, HEAD after HEAD — costs exactly one.
#[tokio::test]
async fn head_is_answered_from_the_index_without_touching_the_origin() {
    let origin = Origin::start("e2e-head").await;
    let bucket = origin.bucket();
    origin.seed("obj", b"twelve bytes").await;
    let proxy = node(&origin, 1024 * 1024);
    wait_for_index(&proxy, &origin, bucket).await;

    let out = head(&proxy, bucket, "obj").await.expect("the key exists");
    assert_eq!(out.content_length, Some(12));
    assert_eq!(
        origin.ops.head(),
        1,
        "the first HEAD completed the bootstrap entry"
    );

    for _ in 0..3 {
        let out = head(&proxy, bucket, "obj").await.expect("the key exists");
        assert_eq!(out.content_length, Some(12));
        assert_eq!(
            out.e_tag.map(ETag::into_value),
            origin.etag("obj").await,
            "the index carries the origin's own ETag"
        );
        assert!(out.last_modified.is_some(), "and its last-modified time");
        assert!(
            out.content_type.is_some(),
            "the completed entry carries the Content-Type the bootstrap row did not"
        );
        assert_eq!(out.accept_ranges.as_deref(), Some("bytes"));
    }

    let err = head(&proxy, bucket, "ghost")
        .await
        .expect_err("the key does not exist");
    assert_eq!(*err.code(), S3ErrorCode::NoSuchKey);
    assert_eq!(err.status_code().map(|status| status.as_u16()), Some(404));

    assert_eq!(
        origin.ops.head(),
        1,
        "no HEAD after the first cost the origin anything — the point of the issue"
    );
}

/// A write through the proxy lands upstream and folds into the index *and* into the body
/// cache: LIST, HEAD and the read-after-write are all answered from local state, so a
/// write that a client immediately reads back costs the origin exactly one operation —
/// the write itself.
#[tokio::test]
async fn a_write_through_lands_upstream_and_folds_into_the_index() {
    let origin = Origin::start("e2e-write").await;
    let bucket = origin.bucket();
    let proxy = node(&origin, 1024 * 1024);
    wait_for_index(&proxy, &origin, bucket).await;
    let (lists, heads) = (origin.ops.list(), origin.ops.head());

    put_typed(
        &proxy,
        bucket,
        "fresh",
        b"written through",
        "text/x-fixture",
    )
    .await;
    assert_eq!(
        origin.stored("fresh").await.as_deref(),
        Some(&b"written through"[..]),
        "the write reached the origin"
    );
    assert_eq!(origin.ops.put(), 1, "exactly one upstream write");

    assert_eq!(list(&proxy, bucket).await, ["fresh"]);
    assert_eq!(origin.ops.list(), lists, "LIST came out of the index");
    for _ in 0..2 {
        let out = head(&proxy, bucket, "fresh").await.expect("indexed");
        assert_eq!(out.content_length, Some(15));
        assert_eq!(
            out.e_tag.map(ETag::into_value),
            origin.etag("fresh").await,
            "the write path captured the origin's ETag"
        );
        assert_eq!(out.content_type.as_deref(), Some("text/x-fixture"));
    }
    assert_eq!(
        origin.ops.head(),
        heads,
        "a write that names its Content-Type is HEAD-able locally straight away"
    );

    // The write kept the bytes it was already holding, so the read after it is free.
    assert_eq!(get(&proxy, bucket, "fresh").await, "written through");
    assert_eq!(
        origin.ops.get(),
        0,
        "the read after the write came out of the write's own fill"
    );

    delete(&proxy, bucket, "fresh").await;
    assert!(
        list(&proxy, bucket).await.is_empty(),
        "the delete unindexed"
    );
    assert!(origin.stored("fresh").await.is_none());
}

/// The write path may only keep a body it can describe exactly. A PUT that names no
/// `Content-Type` cannot: the origin invents one (`application/octet-stream` here,
/// `binary/octet-stream` on AWS), and a locally-served answer reporting none would differ
/// from the origin's. Such a write still lands and still indexes — its entry just stays
/// skeletal until a forwarded HEAD completes it, and its body stays the origin's to serve
/// once. This is the same bar `ObjEntry::is_faithful` sets for an index-served HEAD.
#[tokio::test]
async fn a_write_that_names_no_content_type_keeps_no_body() {
    let origin = Origin::start("e2e-write-untyped").await;
    let bucket = origin.bucket();
    let proxy = node(&origin, 1024 * 1024);
    wait_for_index(&proxy, &origin, bucket).await;
    let heads = origin.ops.head();

    put(&proxy, bucket, "untyped", b"written through").await;

    let out = head(&proxy, bucket, "untyped").await.expect("indexed");
    assert_eq!(out.content_length, Some(15));
    assert_eq!(
        origin.ops.head(),
        heads + 1,
        "the first HEAD was forwarded, and completed the skeletal entry"
    );
    let heads = origin.ops.head();
    assert_eq!(
        head(&proxy, bucket, "untyped")
            .await
            .expect("indexed")
            .content_length,
        Some(15)
    );
    assert_eq!(
        origin.ops.head(),
        heads,
        "every HEAD after it is answered from the completed entry"
    );

    assert_eq!(get(&proxy, bucket, "untyped").await, "written through");
    assert_eq!(origin.ops.get(), 1, "the body itself was not kept");
    assert_eq!(get(&proxy, bucket, "untyped").await, "written through");
    assert_eq!(origin.ops.get(), 1, "the read filled it, as ever");
}

/// The writer keeps its *own* fresh copy: an overwrite replaces the cached body with the
/// bytes it just wrote rather than dropping them, so the read after it is both new and
/// free. A write the origin *refuses* (a lost CAS) never caches the rejected bytes; its
/// pre-forward invalidation costs one origin refill without changing durable contents.
#[tokio::test]
async fn a_fillable_overwrite_serves_its_own_new_bytes() {
    let origin = Origin::start("e2e-write-fill").await;
    let bucket = origin.bucket();
    let proxy = node(&origin, 1024 * 1024);
    wait_for_index(&proxy, &origin, bucket).await;

    put_typed(&proxy, bucket, "obj", b"version-1", "text/x-fixture").await;
    assert_eq!(get(&proxy, bucket, "obj").await, "version-1");

    // No settling: the fill lands before the write returns, so the very next read must
    // already be the new bytes — and must not have gone to the origin for them.
    put_typed(&proxy, bucket, "obj", b"version-2", "text/x-fixture").await;
    assert_eq!(
        get(&proxy, bucket, "obj").await,
        "version-2",
        "the writer serves what it just wrote, not what it had"
    );
    assert_eq!(
        head(&proxy, bucket, "obj").await.expect("indexed").e_tag,
        origin.etag("obj").await.map(ETag::Strong),
        "and the kept copy carries the origin's ETag for the new bytes"
    );
    assert_eq!(origin.ops.get(), 0, "neither read reached the origin");

    let err = put_typed_conditional(
        &proxy,
        bucket,
        "obj",
        b"rejected",
        "text/x-fixture",
        Some(ETagCondition::Any),
    )
    .await
    .expect_err("the key exists, so create-if-absent loses");
    assert_eq!(err.status_code().map(|status| status.as_u16()), Some(412));
    assert_eq!(
        get(&proxy, bucket, "obj").await,
        "version-2",
        "a refused write never cached its rejected body"
    );
    assert_eq!(
        origin.ops.get(),
        1,
        "pre-forward invalidation buys the confirmed origin version back"
    );
}

/// A write past `max_obj_bytes` keeps nothing: the body streams through to the origin
/// byte for byte, and reads of it are origin reads like any other over-cap object.
#[tokio::test]
async fn an_over_cap_write_streams_through_and_keeps_nothing() {
    const CAP: usize = 64;
    let origin = Origin::start("e2e-write-over-cap").await;
    let bucket = origin.bucket();
    let big: Vec<u8> = (0..CAP * 4)
        .map(|n| u8::try_from(n % 251).unwrap_or(0))
        .collect();
    let proxy = node(&origin, CAP);
    wait_for_index(&proxy, &origin, bucket).await;

    put_typed(&proxy, bucket, "big", &big, "application/x-fixture").await;
    assert_eq!(
        origin.stored("big").await.as_deref(),
        Some(&big[..]),
        "an over-cap write reaches the origin unaltered"
    );

    for expected_gets in 1..=2 {
        assert_eq!(get(&proxy, bucket, "big").await, big);
        assert_eq!(
            origin.ops.get(),
            expected_gets,
            "an over-cap object is fetched every time, written through us or not"
        );
    }
}

/// A write invalidates the writer's own cached copy: the next read must return the new
/// bytes, refetched, rather than the body cached a moment earlier. (This write names no
/// `Content-Type`, so there is nothing to put in the dropped copy's place — the write
/// that *can* refill is `a_fillable_overwrite_serves_its_own_new_bytes`.)
#[tokio::test]
async fn an_overwrite_invalidates_the_local_copy() {
    let origin = Origin::start("e2e-invalidate").await;
    let bucket = origin.bucket();
    origin.seed("obj", b"version-1").await;
    let proxy = node(&origin, 1024 * 1024);

    assert_eq!(get(&proxy, bucket, "obj").await, "version-1");
    let fetched = origin.ops.get();

    // No settling: a local write invalidates before it returns, so the very next read
    // must already be the new bytes.
    put(&proxy, bucket, "obj", b"version-2").await;
    assert_eq!(
        get(&proxy, bucket, "obj").await,
        "version-2",
        "the stale copy was dropped by the write"
    );
    assert_eq!(
        origin.ops.get(),
        fetched + 1,
        "and the new bytes came from the origin"
    );
}

/// Compare-and-set survives the proxy end to end. The origin is the authority for
/// conditional writes, so the proxy must forward them untouched *and* keep its own
/// state consistent with the outcome: a rejected ordinary stale CAS leaves the index
/// intact (the body was conservatively evicted before forwarding), while an accepted one
/// replaces both.
#[tokio::test]
async fn conditional_writes_keep_their_origin_semantics() {
    let origin = Origin::start("e2e-cas").await;
    let bucket = origin.bucket();
    let proxy = node(&origin, 1024 * 1024);
    wait_for_index(&proxy, &origin, bucket).await;

    // create-if-absent: the first wins, the second is rejected by the origin.
    put_conditional(
        &proxy,
        bucket,
        "cas",
        b"one",
        Some(ETagCondition::Any),
        None,
    )
    .await
    .expect("the key is absent, so the create succeeds");
    assert_eq!(get(&proxy, bucket, "cas").await, "one");
    let (fetched, put_calls) = (origin.ops.get(), origin.ops.put());

    let err = put_conditional(
        &proxy,
        bucket,
        "cas",
        b"two",
        Some(ETagCondition::Any),
        None,
    )
    .await
    .expect_err("the key now exists");
    assert_eq!(err.status_code().map(|status| status.as_u16()), Some(412));
    assert_eq!(*err.code(), S3ErrorCode::PreconditionFailed);

    assert_eq!(origin.stored("cas").await.as_deref(), Some(&b"one"[..]));
    assert_eq!(
        get(&proxy, bucket, "cas").await,
        "one",
        "the rejected write leaves the durable object unchanged"
    );
    assert_eq!(
        origin.ops.get(),
        fetched + 1,
        "the body is conservatively evicted before every forwarded mutation"
    );
    assert_eq!(
        head(&proxy, bucket, "cas").await.unwrap().content_length,
        Some(3)
    );

    // compare-and-swap: the wrong ETag is rejected, the current one is accepted.
    let current = origin.etag("cas").await.expect("the object has an ETag");
    let stale = ETagCondition::ETag(ETag::Strong("0".repeat(32)));
    let err = put_conditional(&proxy, bucket, "cas", b"nope", None, Some(stale))
        .await
        .expect_err("a stale ETag loses the race");
    assert_eq!(err.status_code().map(|status| status.as_u16()), Some(412));

    put_conditional(
        &proxy,
        bucket,
        "cas",
        b"three",
        None,
        Some(ETagCondition::ETag(ETag::Strong(current))),
    )
    .await
    .expect("the current ETag wins the race");

    assert_eq!(
        origin.ops.put(),
        put_calls + 3,
        "each attempt was forwarded"
    );
    assert_eq!(
        get(&proxy, bucket, "cas").await,
        "three",
        "the accepted write invalidated the cached body"
    );
    assert_eq!(
        list_entry(&proxy, bucket, "cas").await,
        Some((5, origin.etag("cas").await)),
        "and folded the new size and ETag into the index"
    );
}

/// Cancellation after `MinIO` has applied a conditional PUT cannot cancel the proxy's
/// coherence tail. The forwarder withholds that successful response, the caller is
/// cancelled, and the response is then replaced with a 500: the detached tail must fence
/// the stale local view and rebuild it before local HEAD/GET/LIST service resumes.
#[tokio::test]
async fn an_applied_put_survives_caller_cancellation_and_reconciles() {
    let origin = Origin::start("e2e-put-cancel").await;
    let bucket = origin.bucket();
    origin
        .seed_rich("writer", b"epoch-1", "text/plain", &[])
        .await;
    let proxy = node(&origin, 1024 * 1024);
    wait_for_index(&proxy, &origin, bucket).await;

    let current = head(&proxy, bucket, "writer")
        .await
        .expect("the indexed writer exists")
        .e_tag
        .expect("the writer has an ETag")
        .into_value();
    assert_eq!(get(&proxy, bucket, "writer").await, "epoch-1");

    origin.fail_next_conditional_put_after_apply();
    let worker = proxy.clone();
    let owned_bucket = bucket.to_owned();
    let mutation = tokio::spawn(async move {
        put_conditional(
            &worker,
            &owned_bucket,
            "writer",
            b"epoch-2",
            None,
            Some(ETagCondition::ETag(ETag::Strong(current))),
        )
        .await
    });

    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        origin.wait_for_faulted_put_to_apply(),
    )
    .await
    .expect("the conditional PUT reached MinIO");
    assert_eq!(
        origin.stored("writer").await.as_deref(),
        Some(&b"epoch-2"[..]),
        "the mutation was durable before cancellation"
    );
    mutation.abort();
    assert!(
        mutation
            .await
            .expect_err("the caller was cancelled")
            .is_cancelled(),
        "the inbound request future was actually dropped"
    );
    let lists_before_reconciliation = origin.ops.list();
    origin.release_faulted_put();

    eventually!(
        "the detached PUT tail to start an origin rebuild",
        origin.ops.list() > lists_before_reconciliation
    );
    wait_for_index(&proxy, &origin, bucket).await;
    let rebuilt = list_entry(&proxy, bucket, "writer").await;
    assert_eq!(
        rebuilt,
        Some((7, origin.etag("writer").await)),
        "the origin rebuild replaced the stale index ETag"
    );
    let reconciled = head(&proxy, bucket, "writer")
        .await
        .expect("HEAD converges to the applied mutation")
        .e_tag
        .expect("the reconciled writer has an ETag")
        .into_value();
    assert_eq!(get(&proxy, bucket, "writer").await, "epoch-2");

    put_conditional(
        &proxy,
        bucket,
        "writer",
        b"epoch-3",
        None,
        Some(ETagCondition::ETag(ETag::Strong(reconciled))),
    )
    .await
    .expect("a CAS using the proxy's reconciled ETag succeeds");
    assert_eq!(
        origin.stored("writer").await.as_deref(),
        Some(&b"epoch-3"[..])
    );
}

/// A conditional read is the origin's to answer. Neither the cached body nor the index
/// can evaluate a precondition, so an `If-None-Match` GET *and* HEAD go upstream and
/// come back `304 Not Modified` rather than a locally-manufactured 200 — even with the
/// body cached and the bucket fully indexed.
#[tokio::test]
async fn conditional_reads_are_answered_by_the_origin() {
    let origin = Origin::start("e2e-conditional-read").await;
    let bucket = origin.bucket();
    origin.seed("obj", b"unchanged").await;
    let proxy = node(&origin, 1024 * 1024);
    wait_for_index(&proxy, &origin, bucket).await;

    assert_eq!(get(&proxy, bucket, "obj").await, "unchanged");
    let (fetched, headed) = (origin.ops.get(), origin.ops.head());
    let etag = origin.etag("obj").await.expect("the object has an ETag");

    let err = get_if_none_match(&proxy, bucket, "obj", &etag)
        .await
        .expect_err("the object is unchanged");
    assert_eq!(err.status_code().map(|status| status.as_u16()), Some(304));
    assert_eq!(
        origin.ops.get(),
        fetched + 1,
        "the condition was evaluated by the origin, not guessed at locally"
    );

    let err = head_if_none_match(&proxy, bucket, "obj", &etag)
        .await
        .expect_err("the object is unchanged");
    assert_eq!(err.status_code().map(|status| status.as_u16()), Some(304));
    assert_eq!(
        origin.ops.head(),
        headed + 1,
        "a conditional HEAD is not the index's to answer either"
    );
}
