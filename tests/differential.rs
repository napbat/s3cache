//! **Differential proof**: the caching proxy must be indistinguishable from the origin.
//!
//! Every row here asks the same question twice — once through the [`CachingProxy`], once
//! straight at the `MinIO` behind it — and asserts a client could not tell which answered.
//! That is a stronger claim than "the cache returns the right bytes": it covers the
//! status, the body, and the headers a client actually branches on (`ETag`,
//! `Content-Length`, `Content-Range`, `Last-Modified`, `Content-Type`, `Accept-Ranges`,
//! `x-amz-meta-*`), plus the whole `ListObjectsV2` envelope.
//!
//! The reference leg is the same `s3s_aws::Proxy` translation the cache uses for its own
//! passthrough (see `common::diff`), so anything both legs share cancels out: a
//! difference can only come from a decision the cache made. Rows are therefore ordered to
//! put the cache in the state under test — uncached, then cached; unindexed, then
//! index-served — and the origin's request counters are asserted alongside, because a row
//! that quietly passed through would compare equal while proving nothing.
//!
//! Where a field is left out of a row's mask it is either compared by a test of its own
//! (so one divergence cannot blanket-ignore a matrix) or documented at the call site.

mod common;

use std::time::{Duration, SystemTime};

use bytes::Bytes;
use common::diff::{
    Answer, Fields, Routes, get_input, head_input, list_input, routes, routes_over, routes_unsynced,
};
use common::{Origin, body_blob, delete, request, wait_for_index};
use s3s::S3;
use s3s::dto::{
    CompleteMultipartUploadInput, CompletedMultipartUpload, CompletedPart, CopyObjectInput,
    CopySource, CreateMultipartUploadInput, Delete, DeleteObjectsInput, ETag, ETagCondition,
    EncodingType, GetObjectInput, HeadObjectInput, ListObjectsV2Input, ObjectIdentifier,
    PutObjectInput, Range, Timestamp, UploadPartInput,
};

/// The LIST fields every matrix row compares. `Last-Modified` and the storage class are
/// left to [`list_last_modified_matches_the_origin`] and
/// [`list_storage_class_matches_the_origin`], which compare them on their own so a single
/// divergence cannot blanket-ignore the matrix.
const LIST_SHAPE: Fields = Fields::LIST
    .without(Fields::LAST_MODIFIED)
    .without(Fields::STORAGE_CLASS);

/// What an answer built by the *write* path is compared on — an index-served HEAD of a
/// just-written key, or a GET served from the body that write kept. Everything except
/// `Last-Modified`, which cannot be asserted equal here because a write is stamped with
/// the local clock rather than the origin's mtime (a `PutObject` response carries no
/// mtime to use instead), so byte-for-byte equality would straddle a second boundary at
/// random. The bound that does hold is asserted explicitly by [`assert_same_moment`].
/// Every other field is compared, locally served or not — a copy the proxy is willing to
/// answer from has to carry all of them.
const WRITE_SHAPE: Fields = Fields::OBJECT.without(Fields::LAST_MODIFIED);

/// How far a write-path index timestamp may sit from the origin's own mtime. Generous
/// enough that a slow container never trips it, tight enough to catch a placeholder.
const CLOCK_SLACK: i64 = 5;

// ---------------------------------------------------------------- conditional requests

/// One row of the conditional matrix: a name, and the preconditions it sets. Held once
/// and driven down GET and HEAD alike, since the two must agree with the origin equally.
struct Cond {
    what: &'static str,
    if_match: Option<ETagCondition>,
    if_none_match: Option<ETagCondition>,
    if_modified_since: Option<Timestamp>,
    if_unmodified_since: Option<Timestamp>,
}

/// The conditional matrix, including the combinations whose *precedence* is the
/// interesting part: `If-None-Match` outranks `If-Modified-Since`, `If-Match` outranks
/// `If-Unmodified-Since`, and a request carrying both `If-Match` and `If-None-Match` for
/// the current entity is a 304, not a 200.
fn conditions(etag: &str, before: &Timestamp, after: &Timestamp) -> Vec<Cond> {
    let current = || Some(ETagCondition::ETag(ETag::Strong(etag.to_owned())));
    let stale = || Some(ETagCondition::ETag(ETag::Strong("0".repeat(32))));
    let row = |what, if_match, if_none_match, if_modified_since, if_unmodified_since| Cond {
        what,
        if_match,
        if_none_match,
        if_modified_since,
        if_unmodified_since,
    };
    let (before, after) = (|| Some(before.clone()), || Some(after.clone()));
    vec![
        row("If-Match: current", current(), None, None, None),
        row("If-Match: stale", stale(), None, None, None),
        row("If-Match: *", Some(ETagCondition::Any), None, None, None),
        row("If-None-Match: current", None, current(), None, None),
        row("If-None-Match: stale", None, stale(), None, None),
        row(
            "If-None-Match: *",
            None,
            Some(ETagCondition::Any),
            None,
            None,
        ),
        row("If-Modified-Since: before", None, None, before(), None),
        row("If-Modified-Since: after", None, None, after(), None),
        row("If-Unmodified-Since: before", None, None, None, before()),
        row("If-Unmodified-Since: after", None, None, None, after()),
        row(
            "If-None-Match current + IMS before",
            None,
            current(),
            before(),
            None,
        ),
        row("If-Match stale + IUS after", stale(), None, None, after()),
        row(
            "If-Match current + If-None-Match current",
            current(),
            current(),
            None,
            None,
        ),
    ]
}

/// A precondition is the origin's to evaluate, and the proxy must not learn to guess.
/// Every row of the matrix runs twice: once with nothing cached, once with the body
/// cached and the bucket indexed — the state where answering locally would be tempting —
/// and both times the answer must be the origin's, having actually reached the origin.
#[tokio::test]
async fn conditional_reads_are_the_origins_verdict_cached_or_not() {
    let origin = Origin::start("diff-conditional").await;
    origin.seed("obj", b"conditional body").await;
    let r = routes(&origin, 1024 * 1024);
    wait_for_index(&r.proxy, &origin, &r.bucket).await;

    let etag = origin.etag("obj").await.expect("the object has an ETag");
    let rows = conditions(&etag, &shifted(-3600), &shifted(3600));
    let count = u64::try_from(rows.len()).expect("row count fits");

    for state in ["uncached", "cached"] {
        let (gets, heads) = (origin.ops.get(), origin.ops.head());
        for row in &rows {
            let what = format!("{} ({state})", row.what);
            r.get(&format!("GET {what}"), Fields::OBJECT, || GetObjectInput {
                if_match: row.if_match.clone(),
                if_none_match: row.if_none_match.clone(),
                if_modified_since: row.if_modified_since.clone(),
                if_unmodified_since: row.if_unmodified_since.clone(),
                ..get_input(&r.bucket, "obj")
            })
            .await;
            r.head(&format!("HEAD {what}"), Fields::OBJECT, || {
                HeadObjectInput {
                    if_match: row.if_match.clone(),
                    if_none_match: row.if_none_match.clone(),
                    if_modified_since: row.if_modified_since.clone(),
                    if_unmodified_since: row.if_unmodified_since.clone(),
                    ..head_input(&r.bucket, "obj")
                }
            })
            .await;
        }
        assert_eq!(
            (origin.ops.get(), origin.ops.head()),
            (gets + count, heads + count),
            "every conditional read reached the origin, which is the only authority on it"
        );
        if state == "uncached" {
            r.get("warm the body cache", Fields::OBJECT, || {
                get_input(&r.bucket, "obj")
            })
            .await;
        }
    }
}

/// `now` plus `offset` seconds, as a precondition timestamp. The fixture is written
/// during the test, so ±1h straddles its mtime with no clock reading required.
fn shifted(offset: i64) -> Timestamp {
    let now = SystemTime::now();
    let delta = Duration::from_secs(offset.unsigned_abs());
    Timestamp::from(if offset < 0 { now - delta } else { now + delta })
}

// --------------------------------------------------------------------------- ranges

/// Ranges the cache can answer by slicing a promoted body — the interesting half.
const INT_RANGES: &[(&str, Range)] = &[
    (
        "bytes=0-0",
        Range::Int {
            first: 0,
            last: Some(0),
        },
    ),
    (
        "bytes=2-5",
        Range::Int {
            first: 2,
            last: Some(5),
        },
    ),
    (
        "bytes=4-",
        Range::Int {
            first: 4,
            last: None,
        },
    ),
    (
        "bytes=0-9 (whole object)",
        Range::Int {
            first: 0,
            last: Some(9),
        },
    ),
    (
        "bytes=7-99 (last past EOF)",
        Range::Int {
            first: 7,
            last: Some(99),
        },
    ),
    (
        "bytes=9-9 (last byte)",
        Range::Int {
            first: 9,
            last: Some(9),
        },
    ),
    (
        "bytes=10-12 (first past EOF)",
        Range::Int {
            first: 10,
            last: Some(12),
        },
    ),
    (
        "bytes=99- (first past EOF, open)",
        Range::Int {
            first: 99,
            last: None,
        },
    ),
];

/// Suffix ranges, which `s3s` models separately and the cache never slices locally.
/// These rows prove the passthrough stays exact rather than the slicing does.
const SUFFIX_RANGES: &[(&str, Range)] = &[
    ("bytes=-3", Range::Suffix { length: 3 }),
    ("bytes=-10 (whole object)", Range::Suffix { length: 10 }),
    (
        "bytes=-99 (longer than object)",
        Range::Suffix { length: 99 },
    ),
];

/// Issue every range in `rows` down both legs. `Content-Range`, the 200-vs-206 status,
/// the sliced bytes and the object headers all have to match.
async fn range_rows(r: &Routes, key: &str, state: &str, rows: &[(&str, Range)]) {
    for (what, range) in rows {
        r.get(&format!("GET {what} ({state})"), Fields::OBJECT, || {
            GetObjectInput {
                range: Some(*range),
                ..get_input(&r.bucket, key)
            }
        })
        .await;
    }
}

/// Ranged reads must be the origin's answer in all three states the proxy can produce
/// one from: streamed through (nothing indexed, so nothing to promote), sliced out of a
/// body promoted on the spot, and sliced out of a body already cached. Clamping past EOF,
/// the 416 shape, open-ended and suffix forms are all in the matrix.
#[tokio::test]
async fn ranges_match_the_origin_in_every_serving_state() {
    // Two rows of the matrix start past the end of the object. Their 416 is not
    // synthesized locally — its shape is the origin's (AWS sends
    // `Content-Range: bytes */<size>`, this origin sends no header at all), and letting
    // the origin answer is the only way to match every origin — so they cost one fetch
    // each in every state, cached or not. See
    // [`an_unsatisfiable_range_reports_content_range_like_the_origin`].
    const UNSATISFIABLE: u64 = 2;

    let origin = Origin::start("diff-range").await;
    origin.seed("obj", b"0123456789").await;

    // A proxy that never syncs is a pure forwarder: no index entry means no promotion,
    // so these rows are the origin's own answers — the baseline the other states must
    // reproduce. (Deterministic, unlike racing the background warm-up.)
    let raw = routes_unsynced(&origin, 1024 * 1024);
    range_rows(&raw, "obj", "passthrough", INT_RANGES).await;
    range_rows(&raw, "obj", "passthrough", SUFFIX_RANGES).await;

    let r = routes(&origin, 1024 * 1024);
    wait_for_index(&r.proxy, &origin, &r.bucket).await;

    let fetched = origin.ops.get();
    range_rows(&r, "obj", "promoted", INT_RANGES).await;
    assert_eq!(
        origin.ops.get(),
        fetched + 1 + UNSATISFIABLE,
        "the first range promoted the whole object exactly once, and only the \
         unsatisfiable rows went back to the origin"
    );
    let promoted = origin.ops.get();
    range_rows(&r, "obj", "cached", INT_RANGES).await;
    assert_eq!(
        origin.ops.get(),
        promoted + UNSATISFIABLE,
        "and every satisfiable range after it was sliced locally"
    );
    range_rows(&r, "obj", "cached", SUFFIX_RANGES).await;
}

// ------------------------------------------------------------------------ list matrix

/// A keyspace with the shapes a LIST algorithm gets wrong: nested prefixes, an empty
/// directory marker, keys sorting either side of the delimiter (`0x20 ' '`, `0x21 '!'`,
/// `0x2f '/'`, `0x30 '0'`), a multi-byte key, and numeric-looking keys whose byte order is
/// not their numeric order — plus enough keys under one prefix to page through.
///
/// It deliberately holds no key that is *also* a prefix of another (`a` alongside `a/b`):
/// `MinIO` cannot store both, and drops the whole subtree from a plain LIST while still
/// answering a prefixed LIST for it. An index built from LIST can only ever be as
/// consistent as the LIST it was built from, so an origin that contradicts itself is not
/// a difference this suite can attribute to the proxy.
const KEYSPACE: &[&str] = &[
    "a",
    "a b",
    "a!b",
    "a0",
    "a\u{e9}",
    "m/",
    "p/w",
    "p/w!",
    "p/w0",
    "p/x/1",
    "p/x/2",
    "p/x/deep/1",
    "p/y/1",
    "p/z",
    "q/1",
    "q/10",
    "q/11",
    "q/12",
    "q/2",
    "q/3",
    "z",
];

/// One LIST row: name, prefix, delimiter, start-after, max-keys.
type ListRow = (
    &'static str,
    Option<&'static str>,
    Option<&'static str>,
    Option<&'static str>,
    Option<i32>,
);

/// The LIST matrix. Every row is run as a single request *and* walked to exhaustion.
const LIST_ROWS: &[ListRow] = &[
    ("plain", None, None, None, None),
    ("prefix p/", Some("p/"), None, None, None),
    ("prefix p", Some("p"), None, None, None),
    ("prefix p/x/", Some("p/x/"), None, None, None),
    ("prefix matching nothing", Some("zzz"), None, None, None),
    ("delimiter /", None, Some("/"), None, None),
    ("prefix p/ + delimiter /", Some("p/"), Some("/"), None, None),
    (
        "prefix p/x + delimiter /",
        Some("p/x"),
        Some("/"),
        None,
        None,
    ),
    (
        "prefix p/x/ + delimiter /",
        Some("p/x/"),
        Some("/"),
        None,
        None,
    ),
    ("prefix q/ + delimiter /", Some("q/"), Some("/"), None, None),
    ("delimiter 0 (not a slash)", None, Some("0"), None, None),
    (
        "delimiter deep (multi-byte)",
        Some("p/x/"),
        Some("deep"),
        None,
        None,
    ),
    ("start-after p/w", None, None, Some("p/w"), None),
    ("start-after past the end", None, None, Some("zzzz"), None),
    ("start-after + prefix", Some("q/"), None, Some("q/10"), None),
    ("start-after + delimiter", None, Some("/"), Some("m/"), None),
    ("max-keys 1", None, None, None, Some(1)),
    ("max-keys 3", None, None, None, Some(3)),
    ("max-keys 3 + delimiter", None, Some("/"), None, Some(3)),
    ("max-keys 2 + prefix", Some("q/"), None, None, Some(2)),
    ("max-keys past the end", None, None, None, Some(500)),
];

/// Seed [`KEYSPACE`]. A key ending in the delimiter is a directory marker, which is
/// empty by convention — and which `MinIO` only stores faithfully that way.
async fn seed_keyspace(origin: &Origin) {
    for key in KEYSPACE {
        let body: &[u8] = if key.ends_with('/') {
            b""
        } else {
            key.as_bytes()
        };
        origin.seed(key, body).await;
    }
}

fn list_row(r: &Routes, row: &ListRow) -> ListObjectsV2Input {
    let (_, prefix, delimiter, start_after, max_keys) = *row;
    ListObjectsV2Input {
        prefix: prefix.map(str::to_owned),
        delimiter: delimiter.map(str::to_owned),
        start_after: start_after.map(str::to_owned),
        max_keys,
        ..list_input(&r.bucket)
    }
}

/// A synced bucket answers LIST from the index, and the whole point is that a client
/// cannot tell: same keys in the same byte order, same common prefixes, same paging
/// envelope, same sizes and `ETag`s — for plain lists, prefixes, delimiters, `start-after`
/// and every page size — and none of it costing the origin a single LIST.
#[tokio::test]
async fn list_matches_the_origin_across_the_matrix() {
    let origin = Origin::start("diff-list").await;
    seed_keyspace(&origin).await;
    let r = routes(&origin, 1024 * 1024);
    wait_for_index(&r.proxy, &origin, &r.bucket).await;

    let lists = origin.ops.list();
    for row in LIST_ROWS {
        r.list(row.0, LIST_SHAPE, || list_row(&r, row)).await;
    }
    // The same rows again, this time following continuation tokens to exhaustion: the
    // token values are opaque per implementation, the sequence they rebuild is not.
    for row in LIST_ROWS {
        r.walk(row.0, || list_row(&r, row)).await;
    }
    assert_eq!(
        origin.ops.list(),
        lists,
        "the whole matrix was answered from the index"
    );
}

/// The `Last-Modified` a LIST reports per key, compared at the granularity the LIST XML
/// carries (ISO 8601 with milliseconds).
///
/// Was: proxy `"2026-08-05T04:46:40.000Z"` where the origin said
/// `"2026-08-05T04:46:40.042Z"` — the bootstrap in `sync_bucket_into` (src/index.rs)
/// kept only `d.secs()` of each listed timestamp, so every indexed mtime landed up to a
/// second early. It now keeps the sub-second part the origin sent.
#[tokio::test]
async fn list_last_modified_matches_the_origin() {
    let origin = Origin::start("diff-list-mtime").await;
    for key in ["a", "b/1", "c"] {
        origin.seed(key, key.as_bytes()).await;
    }
    let r = routes(&origin, 1024 * 1024);
    wait_for_index(&r.proxy, &origin, &r.bucket).await;

    r.list(
        "LIST reports the origin's own mtimes",
        Fields::KEYS | Fields::LAST_MODIFIED,
        || list_input(&r.bucket),
    )
    .await;
}

/// The storage class a LIST reports per key.
///
/// Was: proxy `[None, None, None]` where the origin said
/// `[Some("STANDARD"), Some("STANDARD"), Some("STANDARD")]` — `ObjEntry` (src/index.rs)
/// had no storage-class field. It now carries one on every path: the bootstrap reads it
/// off the listed row, a write off the request (S3's default when the request names
/// none), and a peer's write off the v2 feed envelope.
#[tokio::test]
async fn list_storage_class_matches_the_origin() {
    let origin = Origin::start("diff-list-class").await;
    for key in ["a", "b/1", "c"] {
        origin.seed(key, key.as_bytes()).await;
    }
    let r = routes(&origin, 1024 * 1024);
    wait_for_index(&r.proxy, &origin, &r.bucket).await;

    r.list(
        "LIST reports the origin's storage class",
        Fields::KEYS | Fields::STORAGE_CLASS,
        || list_input(&r.bucket),
    )
    .await;
}

// ------------------------------------------------------------------------ head fidelity

/// Every state the proxy can answer a HEAD from: forwarded (no index yet), from the index
/// (the body was never fetched), from a cached body, and the local 404 for a key a synced
/// bucket does not hold.
#[tokio::test]
async fn head_matches_the_origin_from_the_index_and_from_the_cache() {
    let origin = Origin::start("diff-head").await;
    origin
        .seed_rich("obj", b"twelve bytes", "text/x-fixture", &[("k", "v")])
        .await;

    // Forwarded: an unsynced proxy has nothing to answer with, so this is the origin's
    // own answer and the state the other two have to reproduce.
    let raw = routes_unsynced(&origin, 1024 * 1024);
    raw.head("HEAD passed through", Fields::OBJECT, || {
        head_input(&raw.bucket, "obj")
    })
    .await;

    let r = routes(&origin, 1024 * 1024);
    wait_for_index(&r.proxy, &origin, &r.bucket).await;

    // A bootstrap LIST row proves the key exists but carries neither its Content-Type
    // nor its user metadata, so the first HEAD is forwarded rather than answered from a
    // record that would differ from the origin's — and the answer completes the entry.
    let heads = origin.ops.head();
    r.head("HEAD from a skeletal index entry", Fields::OBJECT, || {
        head_input(&r.bucket, "obj")
    })
    .await;
    assert_eq!(
        origin.ops.head(),
        heads + 1,
        "one forwarded HEAD is what completes a bootstrap entry"
    );

    let heads = origin.ops.head();
    r.head(
        "HEAD from the completed index entry",
        Fields::OBJECT,
        || head_input(&r.bucket, "obj"),
    )
    .await;
    // A HEAD 404 carries no body on the wire, so the reference leg's error *code* is the
    // SDK's synthesis rather than anything the origin said: only the status compares.
    r.head("HEAD of a missing key", Fields::STATUS, || {
        head_input(&r.bucket, "ghost")
    })
    .await;
    assert_eq!(
        origin.ops.head(),
        heads,
        "both answers came out of the index"
    );

    r.get("warm the body cache", Fields::OBJECT, || {
        get_input(&r.bucket, "obj")
    })
    .await;
    r.head("HEAD from the body cache", Fields::OBJECT, || {
        head_input(&r.bucket, "obj")
    })
    .await;
    assert_eq!(origin.ops.head(), heads, "nor did the cached one");
}

/// The index-served HEAD, field for field — the strict version of the row in
/// [`head_matches_the_origin_from_the_index_and_from_the_cache`], on an object that
/// actually carries the fields at issue.
///
/// Was, against a seeded object with `Content-Type: text/x-fixture` and two user
/// metadata entries:
///
/// * `content-type: proxy=None origin=Some("text/x-fixture")`
/// * `accept-ranges: proxy=None origin=Some("bytes")`
/// * `metadata: proxy=None origin=Some({"fixture": "value", "second": "entry"})`
///
/// `head_object_from_index` (src/index.rs) built a `HeadObjectOutput` out of size, mtime
/// and `ETag`, which was all `ObjEntry` held — so the same HEAD answered differently
/// depending on whether the body happened to be cached. An entry is now either
/// **faithful** (it carries everything a HEAD reports) or **skeletal**, and only a
/// faithful one answers: a bootstrap row is completed by the first forwarded HEAD, which
/// is what this row walks through. The issue-#1 absorption is unchanged for the pattern
/// that motivated it — repeated HEADs of the same key — it just costs one HEAD to start.
#[tokio::test]
async fn head_from_the_index_matches_the_origin_field_for_field() {
    let origin = Origin::start("diff-head-strict").await;
    origin
        .seed_rich(
            "obj",
            b"twelve bytes",
            "text/x-fixture",
            &[("fixture", "value"), ("second", "entry")],
        )
        .await;
    let r = routes(&origin, 1024 * 1024);
    wait_for_index(&r.proxy, &origin, &r.bucket).await;

    let heads = origin.ops.head();
    r.head(
        "HEAD that completes the index entry",
        Fields::OBJECT,
        || head_input(&r.bucket, "obj"),
    )
    .await;
    assert_eq!(
        origin.ops.head(),
        heads + 1,
        "a skeletal entry is completed rather than answered from"
    );

    // And now the row this test exists for: the same HEAD, answered out of the index,
    // identical to the origin's in every field — and costing the origin nothing.
    let heads = origin.ops.head();
    r.head("HEAD from the index", Fields::OBJECT, || {
        head_input(&r.bucket, "obj")
    })
    .await;
    assert_eq!(origin.ops.head(), heads, "answered from the index");

    // The same HEAD once the body is cached, which is the answer the index-served one
    // has to reproduce: identical to the origin's, every field.
    r.get("warm the body cache", Fields::OBJECT, || {
        get_input(&r.bucket, "obj")
    })
    .await;
    r.head("HEAD from the body cache", Fields::OBJECT, || {
        head_input(&r.bucket, "obj")
    })
    .await;
}

// --------------------------------------------------------------------------- writes

/// A write is only correct if every read path agrees with the origin afterwards. PUT
/// (fresh and overwriting), then DELETE, each followed by GET, HEAD and LIST down both
/// legs — including the shape of a read of the deleted key.
///
/// The reads after a PUT are answered from what the write itself kept: the index entry it
/// folded in, and the body it was already holding. So this row is also the proof that the
/// copy a write constructs is faithful — `Content-Type`, user metadata, `ETag`,
/// `Accept-Ranges`, `Content-Length` and the bytes are compared field for field against
/// the origin's own answer, with only the write clock's `Last-Modified` held to a bound
/// (see [`WRITE_SHAPE`]) — and the origin's counters say it never left this process.
#[tokio::test]
async fn writes_read_back_identically_on_every_path() {
    let origin = Origin::start("diff-write").await;
    let r = routes(&origin, 1024 * 1024);
    wait_for_index(&r.proxy, &origin, &r.bucket).await;

    put_through(&r, "obj", b"first version", Some("text/x-fixture"), &[]).await;
    let fetched = origin.ops.get();
    let head = r
        .head("HEAD after PUT (index-served)", WRITE_SHAPE, || {
            head_input(&r.bucket, "obj")
        })
        .await;
    assert_same_moment(&head, &r, "obj").await;
    let read = r
        .get("GET after PUT", WRITE_SHAPE, || get_input(&r.bucket, "obj"))
        .await;
    assert_same_moment(&read, &r, "obj").await;
    r.head("HEAD after PUT (body cached)", WRITE_SHAPE, || {
        head_input(&r.bucket, "obj")
    })
    .await;
    r.list("LIST after PUT", LIST_SHAPE, || list_input(&r.bucket))
        .await;
    assert_eq!(
        origin.ops.get(),
        fetched,
        "the read after the write was answered from the body the write kept"
    );

    put_through(
        &r,
        "obj",
        b"second version, longer",
        Some("text/x-fixture"),
        &[],
    )
    .await;
    let read = r
        .get("GET after overwrite", WRITE_SHAPE, || {
            get_input(&r.bucket, "obj")
        })
        .await;
    assert_same_moment(&read, &r, "obj").await;
    r.head("HEAD after overwrite", WRITE_SHAPE, || {
        head_input(&r.bucket, "obj")
    })
    .await;
    r.list("LIST after overwrite", LIST_SHAPE, || list_input(&r.bucket))
        .await;
    assert_eq!(
        origin.ops.get(),
        fetched,
        "and the overwrite replaced that body rather than dropping it"
    );

    delete(&r.proxy, &r.bucket, "obj").await;
    r.get("GET after DELETE", Fields::OBJECT, || {
        get_input(&r.bucket, "obj")
    })
    .await;
    r.head("HEAD after DELETE", Fields::STATUS, || {
        head_input(&r.bucket, "obj")
    })
    .await;
    r.list("LIST after DELETE", LIST_SHAPE, || list_input(&r.bucket))
        .await;
}

/// A copy must read back as the origin describes it: the source's `Content-Type` and user
/// metadata carried over, and the destination's own `ETag`.
#[tokio::test]
async fn copy_object_reads_back_identically() {
    let origin = Origin::start("diff-copy").await;
    let r = routes(&origin, 1024 * 1024);
    wait_for_index(&r.proxy, &origin, &r.bucket).await;

    let meta = [("fixture", "value"), ("second", "entry")];
    put_through(&r, "src", b"copy me", Some("text/x-fixture"), &meta).await;
    let mut input = CopyObjectInput::builder();
    input.set_bucket(r.bucket.clone());
    input.set_key("dst".to_owned());
    input.set_copy_source(CopySource::Bucket {
        bucket: r.bucket.clone().into(),
        key: "src".into(),
        version_id: None,
    });
    r.proxy
        .copy_object(request(input.build().expect("a complete copy request")))
        .await
        .expect("the copy succeeds");

    r.head("HEAD the copy (index-served)", WRITE_SHAPE, || {
        head_input(&r.bucket, "dst")
    })
    .await;
    r.get("GET the copy", Fields::OBJECT, || {
        get_input(&r.bucket, "dst")
    })
    .await;
    r.get("GET the copy (cached)", Fields::OBJECT, || {
        get_input(&r.bucket, "dst")
    })
    .await;
    r.head("HEAD the copy (body cached)", Fields::OBJECT, || {
        head_input(&r.bucket, "dst")
    })
    .await;
    r.list("LIST after the copy", LIST_SHAPE, || list_input(&r.bucket))
        .await;
}

/// A completed multipart upload reads back as one object whose `ETag` is the multipart
/// `-N` form. What that string is, is the origin's business — it is never hardcoded here,
/// only compared.
#[tokio::test]
async fn multipart_complete_reads_back_identically() {
    // Every part but the last must be at least 5 MiB, so the fixture is one 5 MiB part
    // plus a tail; the cap is set above the assembled size to keep it cacheable.
    const PART: usize = 5 * 1024 * 1024;
    let origin = Origin::start("diff-multipart").await;
    let r = routes(&origin, 8 * 1024 * 1024);
    wait_for_index(&r.proxy, &origin, &r.bucket).await;

    let upload = r
        .proxy
        .create_multipart_upload(request(CreateMultipartUploadInput {
            bucket: r.bucket.clone(),
            key: "assembled".to_owned(),
            content_type: Some("text/x-fixture".to_owned()),
            ..Default::default()
        }))
        .await
        .expect("the upload starts")
        .output
        .upload_id
        .expect("an upload id");

    let mut parts = Vec::new();
    for (number, body) in [vec![b'm'; PART], b"tail".to_vec()].into_iter().enumerate() {
        let part = i32::try_from(number + 1).expect("part number fits");
        parts.push(CompletedPart {
            e_tag: upload_part(&r, &upload, part, body).await,
            part_number: Some(part),
            ..Default::default()
        });
    }
    r.proxy
        .complete_multipart_upload(request(CompleteMultipartUploadInput {
            bucket: r.bucket.clone(),
            key: "assembled".to_owned(),
            upload_id: upload,
            multipart_upload: Some(CompletedMultipartUpload { parts: Some(parts) }),
            ..Default::default()
        }))
        .await
        .expect("the upload completes");

    r.head("HEAD the assembled object", WRITE_SHAPE, || {
        head_input(&r.bucket, "assembled")
    })
    .await;
    r.get("GET the assembled object", Fields::OBJECT, || {
        get_input(&r.bucket, "assembled")
    })
    .await;
    r.get("GET the assembled object (cached)", Fields::OBJECT, || {
        get_input(&r.bucket, "assembled")
    })
    .await;
    r.list("LIST after the upload", LIST_SHAPE, || {
        list_input(&r.bucket)
    })
    .await;
}

/// Upload one part through the proxy and return the `ETag` the origin gave it.
async fn upload_part(r: &Routes, upload: &str, number: i32, body: Vec<u8>) -> Option<ETag> {
    let length = i64::try_from(body.len()).expect("part size fits");
    r.proxy
        .upload_part(request(UploadPartInput {
            bucket: r.bucket.clone(),
            key: "assembled".to_owned(),
            upload_id: upload.to_owned(),
            part_number: number,
            content_length: Some(length),
            body: Some(body_blob(Bytes::from(body))),
            ..Default::default()
        }))
        .await
        .expect("the part uploads")
        .output
        .e_tag
}

/// An object past the per-object cap is never cached: it streams through on every read.
/// The bypass has to be exact too — whole-object and ranged alike.
#[tokio::test]
async fn over_cap_reads_match_the_origin() {
    const CAP: usize = 64;
    let origin = Origin::start("diff-over-cap").await;
    origin.seed("big", &vec![b'x'; CAP * 4]).await;
    let r = routes(&origin, CAP);
    wait_for_index(&r.proxy, &origin, &r.bucket).await;

    let fetched = origin.ops.get();
    r.get("GET over the cap", Fields::OBJECT, || {
        get_input(&r.bucket, "big")
    })
    .await;
    r.get("GET over the cap (again)", Fields::OBJECT, || {
        get_input(&r.bucket, "big")
    })
    .await;
    assert_eq!(
        origin.ops.get(),
        fetched + 2,
        "an over-cap object is fetched every time — this is the bypass path"
    );
    for (what, range) in [
        (
            "bytes=0-0",
            Range::Int {
                first: 0,
                last: Some(0),
            },
        ),
        (
            "bytes=100-200",
            Range::Int {
                first: 100,
                last: Some(200),
            },
        ),
        (
            "bytes=250-",
            Range::Int {
                first: 250,
                last: None,
            },
        ),
        (
            "bytes=999-",
            Range::Int {
                first: 999,
                last: None,
            },
        ),
        ("bytes=-16", Range::Suffix { length: 16 }),
    ] {
        r.get(&format!("GET {what} over the cap"), Fields::OBJECT, || {
            GetObjectInput {
                range: Some(range),
                ..get_input(&r.bucket, "big")
            }
        })
        .await;
    }
    r.head("HEAD over the cap", Fields::OBJECT, || {
        head_input(&r.bucket, "big")
    })
    .await;
}

// ---------------------------------------------------------------- response overrides

/// The `response-*` overrides a client puts on a GET for the origin to apply to that one
/// response — a property of the request, not of the stored object.
fn overriding(bucket: &str, key: &str) -> GetObjectInput {
    GetObjectInput {
        response_content_type: Some("application/x-override".to_owned()),
        response_content_disposition: Some("attachment; filename=\"override.txt\"".to_owned()),
        response_cache_control: Some("max-age=99".to_owned()),
        ..get_input(bucket, key)
    }
}

/// A cached body may not answer a `response-*` override by replaying the headers it was
/// filled with: the overrides belong to the request, and the origin applies them to every
/// response, hit or miss.
///
/// Was, on the second (cache-hit) GET:
/// `content-type: proxy=Some("text/x-fixture") origin=Some("application/x-override")`,
/// `content-disposition: proxy=None origin=Some("attachment; filename=\"override.txt\"")`,
/// `cache-control: proxy=None origin=Some("max-age=99")` — `CachingProxy::get_object`
/// never looked at the `response_*` fields, so an overriding request was
/// served from the cache as if it had asked for nothing. They are now lifted off the
/// request on the way in and applied to the answer on the way out, whichever tier
/// produced it.
#[tokio::test]
async fn response_overrides_are_applied_on_a_cache_hit() {
    let origin = Origin::start("diff-overrides").await;
    origin
        .seed_rich("obj", b"override me", "text/x-fixture", &[])
        .await;
    let r = routes(&origin, 1024 * 1024);
    wait_for_index(&r.proxy, &origin, &r.bucket).await;

    r.get("plain GET (fills the cache)", Fields::OBJECT, || {
        get_input(&r.bucket, "obj")
    })
    .await;
    r.get(
        "GET with response overrides (cache hit)",
        Fields::OBJECT,
        || overriding(&r.bucket, "obj"),
    )
    .await;
}

/// The mirror image: a GET carrying overrides must not leave them in the cache for the
/// next client, who asked for no such thing.
///
/// Was, on the plain GET that follows an overriding one:
/// `content-type: proxy=Some("application/x-override") origin=Some("text/x-fixture")`,
/// `content-disposition: proxy=Some("attachment; filename=\"override.txt\"") origin=None`,
/// `cache-control: proxy=Some("max-age=99") origin=None`. The overriding GET was treated
/// as cacheable, so `CachedObject::from_get` stored the *overridden* headers and every
/// later reader got one client's per-request formatting. Because the overrides are now
/// stripped before the request reaches the origin, the fill is of the object as the
/// origin stores it — an overriding read still warms the cache, and warms it correctly.
#[tokio::test]
async fn a_response_override_does_not_poison_the_cache() {
    let origin = Origin::start("diff-overrides-poison").await;
    origin
        .seed_rich("obj", b"override me", "text/x-fixture", &[])
        .await;
    let r = routes(&origin, 1024 * 1024);
    wait_for_index(&r.proxy, &origin, &r.bucket).await;

    r.get("GET with response overrides (miss)", Fields::OBJECT, || {
        overriding(&r.bucket, "obj")
    })
    .await;
    r.get("plain GET after an overriding one", Fields::OBJECT, || {
        get_input(&r.bucket, "obj")
    })
    .await;
}

// -------------------------------------------------------------- more of the LIST matrix

/// Keys that force `encoding-type=url` to do something: a space, a plus, a literal
/// percent and a delimiter.
const ENCODED_KEYS: &[&str] = &[
    "plain",
    "with space",
    "with+plus",
    "with%pct",
    "dir/with space",
];

/// `encoding-type=url` is a wire-format request: the origin percent-encodes the keys, the
/// prefixes, the delimiter and `start-after` in its answer, and echoes `EncodingType`.
///
/// Was: `keys: proxy=["dir/with space", "plain", "with space", "with%pct", "with+plus"]`
/// versus `origin=["dir/with+space", "plain", "with+space", "with%25pct", "with%2Bplus"]`,
/// and `encoding_type: proxy=None origin=Some("url")` in the paging envelope —
/// `list_objects_v2_from_index` (src/index.rs) never read `inp.encoding_type`, so a
/// client that asked for encoded keys got raw ones and corrupted every key containing a
/// `%` or a `+` when it decoded them.
///
/// The fix is to forward, not to encode: the escaping is the *origin's* wire format and
/// it is not portable. This origin escapes a space as `+` and leaves `/`, `*`, `-`, `_`
/// and `.` alone (`s3ShouldEscape` in `MinIO`'s `cmd/api-utils.go`); Go's own
/// `url.QueryEscape` — the function that code is derived from — escapes `/` and `*`, and
/// AWS and R2 need not agree with either. A proxy that guesses the rule hands back keys
/// that decode to something the bucket does not contain, which is worse than an
/// origin round trip, so an `encoding-type` LIST passes through. The row stays as the
/// referee: it compares the two legs whatever this proxy decides to do with them.
#[tokio::test]
async fn list_with_url_encoding_matches_the_origin() {
    let origin = Origin::start("diff-list-encoding").await;
    for key in ENCODED_KEYS {
        origin.seed(key, key.as_bytes()).await;
    }
    let r = routes(&origin, 1024 * 1024);
    wait_for_index(&r.proxy, &origin, &r.bucket).await;

    let url = || Some(EncodingType::from_static(EncodingType::URL));
    r.list("encoding-type=url", LIST_SHAPE, || ListObjectsV2Input {
        encoding_type: url(),
        ..list_input(&r.bucket)
    })
    .await;
    r.list("encoding-type=url + delimiter", LIST_SHAPE, || {
        ListObjectsV2Input {
            encoding_type: url(),
            delimiter: Some("/".to_owned()),
            ..list_input(&r.bucket)
        }
    })
    .await;
}

/// `max-keys=0` asks for no keys at all — a shape clients use to probe a bucket cheaply.
///
/// Was: `keys: proxy=["a"] origin=[]`, and in the envelope
/// `max_keys: proxy=Some(1) origin=Some(0)`, `key_count: proxy=Some(1) origin=Some(0)` —
/// `list_objects_v2_from_index` (src/index.rs) clamped `max_keys` into `1..=1000`, so a
/// request for nothing was answered with an object, and the echo told the client its
/// request had been rewritten. The floor is now 0.
#[tokio::test]
async fn list_with_max_keys_zero_matches_the_origin() {
    let origin = Origin::start("diff-list-zero").await;
    for key in ["a", "b", "c"] {
        origin.seed(key, key.as_bytes()).await;
    }
    let r = routes(&origin, 1024 * 1024);
    wait_for_index(&r.proxy, &origin, &r.bucket).await;

    r.list("max-keys 0", LIST_SHAPE, || ListObjectsV2Input {
        max_keys: Some(0),
        ..list_input(&r.bucket)
    })
    .await;
}

/// `fetch-owner=true` asks for the per-object `Owner` element.
///
/// Was: `list-owner: proxy=[None, None, None]` versus
/// `origin=[Some(("02d6176db174dc93cb1b899f7c6078f08654445fe8cf1b6ce98d8855f66bdbf4", "minio")), ...]`
/// — `ObjEntry` (src/index.rs) has no owner field and no write path carries one, because
/// the owner is the origin's account, not anything a write says. A `fetch-owner` LIST
/// therefore passes through rather than answering without the element the client
/// explicitly asked for; the row still compares the two legs, and no origin-LIST-counter
/// assertion is made because this one is *meant* to cost a LIST.
#[tokio::test]
async fn list_with_fetch_owner_matches_the_origin() {
    let origin = Origin::start("diff-list-owner").await;
    for key in ["a", "b", "c"] {
        origin.seed(key, key.as_bytes()).await;
    }
    let r = routes(&origin, 1024 * 1024);
    wait_for_index(&r.proxy, &origin, &r.bucket).await;

    r.list("fetch-owner=true", Fields::KEYS | Fields::OWNER, || {
        ListObjectsV2Input {
            fetch_owner: Some(true),
            ..list_input(&r.bucket)
        }
    })
    .await;
}

// ------------------------------------------------------------------- error-shape rows

/// A 416 carries one header that matters: `Content-Range: bytes */<size>`, which is how a
/// client learns the object's real size after guessing wrong.
///
/// The reference leg cannot answer this one — `s3s-aws` drops an error's response headers
/// on the way in (`// TODO: headers?` in its `error.rs`) — so the origin's raw HTTP answer
/// is the reference instead. Note what that reference actually says: `MinIO` answers 416
/// with **no** `Content-Range` at all (observed: `status=416 content-range=None`), where
/// AWS S3 sends `bytes */10`. So the proxy's own header-less 416 matches this origin, and
/// the row cannot pin the AWS behaviour — it does pin that the proxy never *contradicts*
/// the origin, and it starts failing the day the origin sends the header.
#[tokio::test]
async fn an_unsatisfiable_range_reports_content_range_like_the_origin() {
    let origin = Origin::start("diff-416").await;
    origin.seed("obj", b"0123456789").await;
    let r = routes(&origin, 1024 * 1024);
    wait_for_index(&r.proxy, &origin, &r.bucket).await;
    // Cache the body first, so the 416 below is the cache's own verdict rather than a
    // forwarded one.
    r.get("warm the body cache", Fields::OBJECT, || {
        get_input(&r.bucket, "obj")
    })
    .await;

    let refused = origin
        .client()
        .get_object()
        .bucket(&r.bucket)
        .key("obj")
        .range("bytes=99-")
        .send()
        .await;
    let Err(refused) = refused else {
        panic!("the origin must refuse a range that starts past the end of the object");
    };
    let expected = refused
        .raw_response()
        .and_then(|resp| resp.headers().get("content-range"))
        .map(str::to_owned);
    let served = r
        .proxy
        .get_object(request(GetObjectInput {
            range: Some(Range::Int {
                first: 99,
                last: None,
            }),
            ..get_input(&r.bucket, "obj")
        }))
        .await;
    let Err(err) = served else {
        panic!("the proxy must refuse it too");
    };
    let actual = err
        .headers()
        .and_then(|headers| headers.get("content-range"))
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    assert_eq!(
        err.status_code().map(|status| status.as_u16()),
        Some(416),
        "the status is the origin's"
    );
    assert_eq!(actual, expected, "and so is the Content-Range it carries");
}

/// `x-amz-expected-bucket-owner` is a guard: where the origin rejects a request whose
/// named account does not own the bucket, a locally-served answer must reject it too.
///
/// This origin does not implement the guard — a HEAD carrying `000000000000` is answered
/// `200` by `MinIO` itself — so the row cannot prove the refusal, only that the proxy
/// does not contradict the origin. What makes it hold the day the origin (R2, AWS) does
/// enforce it is that a request carrying the header is no longer eligible for a local
/// answer at all: it is in the `origin_only` set on GET, out of `cache_eligible` on
/// HEAD, and out of the index-served LIST path, so the origin evaluates
/// the guard on every one.
#[tokio::test]
async fn expected_bucket_owner_is_honoured_like_the_origin() {
    let origin = Origin::start("diff-owner-guard").await;
    origin.seed("obj", b"guarded").await;
    let r = routes(&origin, 1024 * 1024);
    wait_for_index(&r.proxy, &origin, &r.bucket).await;
    r.get("warm the body cache", Fields::OBJECT, || {
        get_input(&r.bucket, "obj")
    })
    .await;

    let wrong = || Some("000000000000".to_owned());
    let shape = Fields::STATUS | Fields::ERROR_CODE;
    r.head("HEAD with a wrong expected-bucket-owner", shape, || {
        HeadObjectInput {
            expected_bucket_owner: wrong(),
            ..head_input(&r.bucket, "obj")
        }
    })
    .await;
    r.get("GET with a wrong expected-bucket-owner", shape, || {
        GetObjectInput {
            expected_bucket_owner: wrong(),
            ..get_input(&r.bucket, "obj")
        }
    })
    .await;
    r.list(
        "LIST with a wrong expected-bucket-owner",
        shape | Fields::KEYS,
        || ListObjectsV2Input {
            expected_bucket_owner: wrong(),
            ..list_input(&r.bucket)
        },
    )
    .await;
}

/// A batch delete reports per key: the call succeeds while individual keys are refused.
/// The index may only drop the ones the origin actually deleted.
///
/// Was: after the origin refused to delete a version under a legal hold,
/// `keys: proxy=[] origin=["held"]` and the HEAD was `status: proxy=Some(404)
/// origin=Some(200)` — `CachingProxy::delete_objects` recorded a delete
/// for every key in the *request*, never reading the `Errors` half of the response, so a
/// key the origin still held disappeared from LIST and 404ed until the next resync. The
/// applied set now comes off the response (`Deleted`, or requested-minus-`Errors` under
/// `quiet`), and a version-scoped identifier never tombstones the key at all.
#[tokio::test]
async fn a_refused_batch_delete_leaves_the_key_indexed() {
    let origin = Origin::start("diff-batch-delete").await;
    origin.make_bucket("diff-locked", true).await;
    let r = routes_over(&origin, "diff-locked", 1024 * 1024);
    wait_for_index(&r.proxy, &origin, &r.bucket).await;
    put_through(&r, "held", b"under legal hold", None, &[]).await;
    let version = hold_newest_version(&origin, &r.bucket, "held").await;

    let out = r
        .proxy
        .delete_objects(request(batch_delete(&r.bucket, "held", &version)))
        .await
        .expect("the batch call itself succeeds");
    assert!(
        out.output.errors.iter().flatten().next().is_some(),
        "the origin refused the locked version — without that this row proves nothing"
    );

    r.list("LIST after a refused batch delete", LIST_SHAPE, || {
        list_input(&r.bucket)
    })
    .await;
    r.head("HEAD after a refused batch delete", Fields::OBJECT, || {
        head_input(&r.bucket, "held")
    })
    .await;
}

/// Put a legal hold on the newest version of `key` and return that version id — the
/// cheapest way to make an origin refuse one key of a batch delete.
async fn hold_newest_version(origin: &Origin, bucket: &str, key: &str) -> String {
    let version = origin
        .client()
        .head_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .expect("the object exists")
        .version_id()
        .map(str::to_owned)
        .expect("a versioned bucket stamps a version id");
    origin
        .client()
        .put_object_legal_hold()
        .bucket(bucket)
        .key(key)
        .version_id(&version)
        .legal_hold(
            aws_sdk_s3::types::ObjectLockLegalHold::builder()
                .status(aws_sdk_s3::types::ObjectLockLegalHoldStatus::On)
                .build(),
        )
        .send()
        .await
        .expect("the legal hold is set");
    version
}

/// A `DeleteObjects` naming one specific version.
fn batch_delete(bucket: &str, key: &str, version: &str) -> DeleteObjectsInput {
    let object = ObjectIdentifier {
        key: key.to_owned(),
        version_id: Some(version.to_owned()),
        ..Default::default()
    };
    let mut input = DeleteObjectsInput::builder();
    input.set_bucket(bucket.to_owned());
    input.set_delete(Delete {
        objects: vec![object],
        ..Default::default()
    });
    input.build().expect("a complete batch delete")
}

// --------------------------------------------------------------------------- helpers

/// A write through the proxy carrying what a client would set on it.
async fn put_through(
    r: &Routes,
    key: &str,
    body: &[u8],
    content_type: Option<&str>,
    metadata: &[(&str, &str)],
) {
    let bytes = Bytes::copy_from_slice(body);
    let input = PutObjectInput {
        bucket: r.bucket.clone(),
        key: key.to_owned(),
        content_length: Some(i64::try_from(bytes.len()).expect("fixture size fits")),
        body: Some(body_blob(bytes)),
        content_type: content_type.map(str::to_owned),
        metadata: (!metadata.is_empty()).then(|| {
            metadata
                .iter()
                .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
                .collect()
        }),
        ..Default::default()
    };
    r.proxy
        .put_object(request(input))
        .await
        .expect("the write succeeds");
}

/// The index stamps a write with the local clock rather than the origin's mtime, so its
/// `Last-Modified` is not byte-for-byte assertable (see [`WRITE_SHAPE`]). What is
/// assertable — and what a client depends on — is that the two describe the same moment.
async fn assert_same_moment(indexed: &Answer, r: &Routes, key: &str) {
    let origin = common::diff::answer_head(&r.origin, head_input(&r.bucket, key)).await;
    let (proxy_at, origin_at) = (indexed.epoch_secs, origin.epoch_secs);
    let (Some(proxy_at), Some(origin_at)) = (proxy_at, origin_at) else {
        panic!("both legs report a Last-Modified: proxy={proxy_at:?} origin={origin_at:?}");
    };
    assert!(
        (proxy_at - origin_at).abs() <= CLOCK_SLACK,
        "the indexed write time is {} s from the origin's mtime",
        proxy_at - origin_at
    );
}
