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

use common::{
    Origin, delete, get, get_if_none_match, get_range, head, head_if_none_match, list, list_entry,
    node, put, put_conditional, wait_for_index,
};
use s3s::S3ErrorCode;
use s3s::dto::{ETag, ETagCondition};

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

/// An object past `max_obj_bytes` is never cached — it streams through, correctly, every
/// time, and each read is an origin read.
#[tokio::test]
async fn objects_over_the_cap_stream_through_uncached() {
    const CAP: usize = 64;
    let origin = Origin::start("e2e-bypass").await;
    let big = vec![b'x'; CAP * 4];
    origin.seed("big", &big).await;
    let proxy = node(&origin, CAP);

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

/// Issue #1: a HEAD of an object whose body was never fetched is answered from the LIST
/// index — size, mtime and the origin's `ETag`, with the origin never asked — and a HEAD
/// of a key absent from a synced bucket is an authoritative local 404.
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
        out.e_tag.map(ETag::into_value),
        origin.etag("obj").await,
        "the index carries the origin's own ETag"
    );
    assert!(out.last_modified.is_some(), "and its last-modified time");

    let err = head(&proxy, bucket, "ghost")
        .await
        .expect_err("the key does not exist");
    assert_eq!(*err.code(), S3ErrorCode::NoSuchKey);
    assert_eq!(err.status_code().map(|status| status.as_u16()), Some(404));

    assert_eq!(
        origin.ops.head(),
        0,
        "neither answer cost the origin a HEAD — the whole point of the issue"
    );
}

/// A write through the proxy lands upstream and folds into the index: LIST and HEAD then
/// answer from local state with no further origin traffic, and only the body itself
/// still has to be fetched.
#[tokio::test]
async fn a_write_through_lands_upstream_and_folds_into_the_index() {
    let origin = Origin::start("e2e-write").await;
    let bucket = origin.bucket();
    let proxy = node(&origin, 1024 * 1024);
    wait_for_index(&proxy, &origin, bucket).await;
    let (lists, heads) = (origin.ops.list(), origin.ops.head());

    put(&proxy, bucket, "fresh", b"written through").await;
    assert_eq!(
        origin.stored("fresh").await.as_deref(),
        Some(&b"written through"[..]),
        "the write reached the origin"
    );
    assert_eq!(origin.ops.put(), 1, "exactly one upstream write");

    assert_eq!(list(&proxy, bucket).await, ["fresh"]);
    let out = head(&proxy, bucket, "fresh").await.expect("indexed");
    assert_eq!(out.content_length, Some(15));
    assert_eq!(
        out.e_tag.map(ETag::into_value),
        origin.etag("fresh").await,
        "the write path captured the origin's ETag"
    );
    assert_eq!((origin.ops.list(), origin.ops.head()), (lists, heads));

    // A write is not a fill: the body still comes from the origin, once.
    assert_eq!(get(&proxy, bucket, "fresh").await, "written through");
    assert_eq!(origin.ops.get(), 1);

    delete(&proxy, bucket, "fresh").await;
    assert!(
        list(&proxy, bucket).await.is_empty(),
        "the delete unindexed"
    );
    assert!(origin.stored("fresh").await.is_none());
}

/// A write invalidates the writer's own cached copy: the next read must return the new
/// bytes, refetched, rather than the body cached a moment earlier.
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
/// state consistent with the outcome: a rejected write must leave the index and the
/// cached body exactly as they were, an accepted one must invalidate both.
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
        "the rejected write must not have disturbed the cached body"
    );
    assert_eq!(
        origin.ops.get(),
        fetched,
        "and must not have invalidated it either"
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
