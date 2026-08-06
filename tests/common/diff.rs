//! The differential harness: one request, two legs, one comparison.
//!
//! Every row in `tests/differential.rs` issues the same request through the
//! [`CachingProxy`] under test **and** straight at the origin, then asserts the two
//! answers agree on a named set of fields. The reference leg is a plain
//! `s3s_aws::Proxy` over the uncounted client — the very translation layer the cache
//! uses for its own passthrough — so everything both legs share cancels out and a
//! difference can only come from a decision the cache made: a LIST or HEAD answered
//! from the index, a body served out of the tiers, a range sliced locally.
//!
//! Two rules keep the comparison honest:
//!
//! * **Compare what a client can observe.** Fields are extracted into an [`Answer`] in
//!   the shape they reach the wire (`Last-Modified` as its RFC 1123 header string, the
//!   HTTP status a 206-carrying output implies), never as internal DTO state. Two
//!   representations a client cannot tell apart are canonicalised (absent user metadata
//!   and empty user metadata are both "no `x-amz-meta-*` headers").
//! * **Exclusions are named, not silent.** A row states its [`Fields`] mask, and every
//!   field a mask leaves out is either compared by a dedicated test of its own or
//!   documented at the call site.

#![allow(dead_code)] // each test binary drives a different subset of the harness

use std::collections::BTreeMap;

use bytes::Bytes;
use s3cache::cache::proxy::CachingProxy;
use s3cache::tier::buffer_body;
use s3s::dto::{
    GetObjectInput, GetObjectOutput, HeadObjectInput, HeadObjectOutput, ListObjectsV2Input,
    ListObjectsV2Output, Timestamp, TimestampFormat,
};
use s3s::{S3, S3Error, S3Response};

use super::{Origin, request};

/// Which observable fields a row compares. A bitmask rather than a list of names so a
/// row reads as one expression (`Fields::OBJECT.without(Fields::LAST_MODIFIED)`) and
/// adding a matrix row stays a one-liner.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Fields(u32);

impl Fields {
    /// The HTTP status a client sees (200/206 for a hit, the error's status otherwise).
    pub const STATUS: Self = Self(1 << 0);
    /// The S3 error code in the fault body.
    pub const ERROR_CODE: Self = Self(1 << 1);
    /// The response body, byte for byte.
    pub const BODY: Self = Self(1 << 2);
    pub const CONTENT_LENGTH: Self = Self(1 << 3);
    pub const CONTENT_RANGE: Self = Self(1 << 4);
    /// The `ETag`: of an object answer, and of every row of a LIST answer.
    pub const ETAG: Self = Self(1 << 5);
    /// `Last-Modified`: of an object answer, and of every row of a LIST answer.
    pub const LAST_MODIFIED: Self = Self(1 << 6);
    pub const CONTENT_TYPE: Self = Self(1 << 7);
    pub const ACCEPT_RANGES: Self = Self(1 << 8);
    /// User metadata (`x-amz-meta-*`).
    pub const METADATA: Self = Self(1 << 9);
    /// `Content-Disposition` and `Cache-Control` — the other two headers a client can
    /// override per request (`response-content-disposition`, `response-cache-control`).
    pub const RESPONSE_HEADERS: Self = Self(1 << 15);
    /// The owner LIST reports per key (only with `fetch-owner`).
    pub const OWNER: Self = Self(1 << 16);
    /// The LIST key sequence, in order.
    pub const KEYS: Self = Self(1 << 10);
    /// The size LIST reports per key.
    pub const SIZES: Self = Self(1 << 11);
    /// The common prefixes a delimiter rolled up, in order.
    pub const PREFIXES: Self = Self(1 << 12);
    /// The paging envelope: name, prefix, delimiter, start-after, max-keys, key-count,
    /// is-truncated, and whether a continuation token was echoed or handed out.
    pub const PAGING: Self = Self(1 << 13);
    /// The storage class LIST reports per key.
    pub const STORAGE_CLASS: Self = Self(1 << 14);

    /// Everything this harness models.
    pub const ALL: Self = Self((1 << 17) - 1);
    /// Everything a client can observe about a GET or HEAD answer.
    pub const OBJECT: Self = Self(
        Self::STATUS.0
            | Self::ERROR_CODE.0
            | Self::BODY.0
            | Self::CONTENT_LENGTH.0
            | Self::CONTENT_RANGE.0
            | Self::ETAG.0
            | Self::LAST_MODIFIED.0
            | Self::CONTENT_TYPE.0
            | Self::ACCEPT_RANGES.0
            | Self::METADATA.0
            | Self::RESPONSE_HEADERS.0,
    );
    /// Everything a client can observe about a `ListObjectsV2` answer.
    pub const LIST: Self = Self(
        Self::STATUS.0
            | Self::ERROR_CODE.0
            | Self::KEYS.0
            | Self::SIZES.0
            | Self::ETAG.0
            | Self::LAST_MODIFIED.0
            | Self::PREFIXES.0
            | Self::PAGING.0
            | Self::STORAGE_CLASS.0
            | Self::OWNER.0,
    );

    /// This mask minus `other` — how a row names a deliberate exclusion.
    #[must_use]
    pub const fn without(self, other: Self) -> Self {
        Self(self.0 & !other.0)
    }

    #[must_use]
    const fn has(self, other: Self) -> bool {
        self.0 & other.0 != 0
    }
}

impl std::ops::BitOr for Fields {
    type Output = Self;
    fn bitor(self, rhs: Self) -> Self {
        Self(self.0 | rhs.0)
    }
}

/// One LIST entry, as a client reads it off the wire.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Row {
    pub key: String,
    pub size: Option<i64>,
    pub etag: Option<String>,
    /// ISO 8601 with milliseconds — the granularity the LIST XML carries.
    pub last_modified: Option<String>,
    pub storage_class: Option<String>,
    /// `(id, display name)`, which only a `fetch-owner` LIST carries.
    pub owner: Option<(String, String)>,
}

/// The LIST paging envelope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Paging {
    pub name: Option<String>,
    pub prefix: Option<String>,
    pub delimiter: Option<String>,
    pub start_after: Option<String>,
    pub max_keys: Option<i32>,
    pub key_count: Option<i32>,
    pub is_truncated: Option<bool>,
    pub encoding_type: Option<String>,
    /// Whether a token was echoed / handed out. The *values* are opaque by contract
    /// (S3's are base64 blobs, the index's are raw keys), so only presence compares.
    pub echoed_token: bool,
    pub next_token: bool,
}

/// One leg's answer, reduced to what a client can observe.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Answer {
    pub status: Option<u16>,
    pub error_code: Option<String>,
    pub body: Option<Bytes>,
    pub content_length: Option<i64>,
    pub content_range: Option<String>,
    pub etag: Option<String>,
    /// RFC 1123 — the granularity the `Last-Modified` header carries.
    pub last_modified: Option<String>,
    pub content_type: Option<String>,
    pub accept_ranges: Option<String>,
    pub content_disposition: Option<String>,
    pub cache_control: Option<String>,
    pub metadata: Option<BTreeMap<String, String>>,
    pub rows: Vec<Row>,
    pub prefixes: Vec<String>,
    pub paging: Option<Paging>,
    /// The same instant as `last_modified`, in whole epoch seconds — for the one
    /// comparison that is a bound rather than an equality (an index entry stamped by the
    /// local write clock). Never part of a mask.
    pub epoch_secs: Option<i64>,
}

impl Answer {
    /// Whether this leg failed — the shape assertions read better than `status >= 400`.
    #[must_use]
    pub fn is_error(&self) -> bool {
        self.error_code.is_some()
    }

    fn from_error(err: &S3Error) -> Self {
        Self {
            status: err.status_code().map(|status| status.as_u16()),
            error_code: Some(err.code().as_str().to_owned()),
            ..Self::default()
        }
    }

    async fn from_get(mut resp: S3Response<GetObjectOutput>) -> Self {
        let body = match resp.output.body.take() {
            Some(blob) => Some(buffer_body(blob, usize::MAX).await.expect("readable body")),
            None => None,
        };
        let out = &resp.output;
        Self {
            // s3s derives the status from the output: a `Content-Range` makes it a 206
            // (see `GetObject::serialize_http`), unless the response overrides it.
            status: Some(resp.status.map_or_else(
                || {
                    if out.content_range.is_some() {
                        206
                    } else {
                        200
                    }
                },
                |status| status.as_u16(),
            )),
            body,
            content_length: out.content_length,
            content_range: out.content_range.clone(),
            etag: out.e_tag.as_ref().map(|tag| tag.value().to_owned()),
            last_modified: out.last_modified.as_ref().map(http_date),
            content_type: out.content_type.clone(),
            accept_ranges: out.accept_ranges.clone(),
            content_disposition: out.content_disposition.clone(),
            cache_control: out.cache_control.clone(),
            metadata: user_metadata(out.metadata.as_ref()),
            epoch_secs: out.last_modified.as_ref().and_then(epoch_secs),
            ..Self::default()
        }
    }

    fn from_head(resp: &S3Response<HeadObjectOutput>) -> Self {
        let out = &resp.output;
        Self {
            status: Some(resp.status.map_or(200, |status| status.as_u16())),
            content_length: out.content_length,
            content_range: out.content_range.clone(),
            etag: out.e_tag.as_ref().map(|tag| tag.value().to_owned()),
            last_modified: out.last_modified.as_ref().map(http_date),
            content_type: out.content_type.clone(),
            accept_ranges: out.accept_ranges.clone(),
            content_disposition: out.content_disposition.clone(),
            cache_control: out.cache_control.clone(),
            metadata: user_metadata(out.metadata.as_ref()),
            epoch_secs: out.last_modified.as_ref().and_then(epoch_secs),
            ..Self::default()
        }
    }

    fn from_list(resp: &S3Response<ListObjectsV2Output>) -> Self {
        let out = &resp.output;
        Self {
            status: Some(resp.status.map_or(200, |status| status.as_u16())),
            rows: out
                .contents
                .iter()
                .flatten()
                .map(|object| Row {
                    key: object.key.clone().unwrap_or_default(),
                    size: object.size,
                    etag: object.e_tag.as_ref().map(|tag| tag.value().to_owned()),
                    last_modified: object.last_modified.as_ref().map(iso8601),
                    storage_class: object
                        .storage_class
                        .as_ref()
                        .map(|class| class.as_str().to_owned()),
                    owner: object.owner.as_ref().map(|owner| {
                        (
                            owner.id.clone().unwrap_or_default(),
                            owner.display_name.clone().unwrap_or_default(),
                        )
                    }),
                })
                .collect(),
            prefixes: out
                .common_prefixes
                .iter()
                .flatten()
                .filter_map(|common| common.prefix.clone())
                .collect(),
            paging: Some(Paging {
                name: out.name.clone(),
                prefix: out.prefix.clone(),
                delimiter: out.delimiter.clone(),
                start_after: out.start_after.clone(),
                max_keys: out.max_keys,
                key_count: out.key_count,
                is_truncated: out.is_truncated,
                encoding_type: out
                    .encoding_type
                    .as_ref()
                    .map(|encoding| encoding.as_str().to_owned()),
                echoed_token: out.continuation_token.is_some(),
                next_token: out.next_continuation_token.is_some(),
            }),
            ..Self::default()
        }
    }
}

/// `Last-Modified` as the header carries it (RFC 1123, whole seconds).
fn http_date(ts: &Timestamp) -> String {
    format_stamp(ts, TimestampFormat::HttpDate)
}

/// A LIST entry's timestamp as the XML carries it (ISO 8601, milliseconds).
fn iso8601(ts: &Timestamp) -> String {
    format_stamp(ts, TimestampFormat::DateTime)
}

/// The same instant in whole epoch seconds, for the one comparison that is a bound.
fn epoch_secs(ts: &Timestamp) -> Option<i64> {
    format_stamp(ts, TimestampFormat::EpochSeconds)
        .split('.')
        .next()?
        .parse()
        .ok()
}

fn format_stamp(ts: &Timestamp, format: TimestampFormat) -> String {
    let mut buf = Vec::new();
    ts.format(format, &mut buf)
        .expect("a formattable timestamp");
    String::from_utf8(buf).expect("ASCII timestamp")
}

/// User metadata, with "absent" and "empty" folded together: both put zero
/// `x-amz-meta-*` headers on the wire, so a client cannot tell them apart.
fn user_metadata(metadata: Option<&s3s::dto::Metadata>) -> Option<BTreeMap<String, String>> {
    let map: BTreeMap<String, String> = metadata
        .into_iter()
        .flatten()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    (!map.is_empty()).then_some(map)
}

/// Bodies are compared byte for byte but reported by size and head, so a mismatch on a
/// multi-megabyte object stays readable.
fn preview(body: Option<&Bytes>) -> String {
    let Some(body) = body else {
        return "<no body>".to_owned();
    };
    let head = &body[..body.len().min(48)];
    let tail = if body.len() > head.len() { "..." } else { "" };
    format!(
        "{} bytes {:?}{tail}",
        body.len(),
        String::from_utf8_lossy(head)
    )
}

/// Names every field in `fields` on which the two legs disagree, quoting both values.
/// Empty means the proxy is indistinguishable from the origin over that mask.
///
/// One macro drives the whole comparison, so a field is added by naming it once and no
/// row can silently lose a check.
macro_rules! compare {
    ($out:ident, $fields:ident, $name:literal, $bit:ident, $lhs:expr, $rhs:expr) => {
        if $fields.has(Fields::$bit) && $lhs != $rhs {
            $out.push(format!("{}: proxy={:?} origin={:?}", $name, $lhs, $rhs));
        }
    };
}

/// Every field in `fields` on which the two legs disagree, named and quoted. Empty means
/// the proxy is indistinguishable from the origin over that mask.
#[must_use]
pub fn diff(proxy: &Answer, origin: &Answer, fields: Fields) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    diff_object(&mut out, proxy, origin, fields);
    diff_list(&mut out, proxy, origin, fields);
    out
}

/// The GET/HEAD half: status, body and the object headers.
fn diff_object(out: &mut Vec<String>, proxy: &Answer, origin: &Answer, fields: Fields) {
    macro_rules! cmp {
        ($name:literal, $bit:ident, $lhs:expr, $rhs:expr) => {
            compare!(out, fields, $name, $bit, $lhs, $rhs);
        };
    }
    cmp!("status", STATUS, proxy.status, origin.status);
    cmp!(
        "error-code",
        ERROR_CODE,
        proxy.error_code,
        origin.error_code
    );
    if fields.has(Fields::BODY) && proxy.body != origin.body {
        out.push(format!(
            "body: proxy={} origin={}",
            preview(proxy.body.as_ref()),
            preview(origin.body.as_ref())
        ));
    }
    cmp!(
        "content-length",
        CONTENT_LENGTH,
        proxy.content_length,
        origin.content_length
    );
    cmp!(
        "content-range",
        CONTENT_RANGE,
        proxy.content_range,
        origin.content_range
    );
    cmp!("etag", ETAG, proxy.etag, origin.etag);
    cmp!(
        "last-modified",
        LAST_MODIFIED,
        proxy.last_modified,
        origin.last_modified
    );
    cmp!(
        "content-type",
        CONTENT_TYPE,
        proxy.content_type,
        origin.content_type
    );
    cmp!(
        "accept-ranges",
        ACCEPT_RANGES,
        proxy.accept_ranges,
        origin.accept_ranges
    );
    cmp!(
        "content-disposition",
        RESPONSE_HEADERS,
        proxy.content_disposition,
        origin.content_disposition
    );
    cmp!(
        "cache-control",
        RESPONSE_HEADERS,
        proxy.cache_control,
        origin.cache_control
    );
    cmp!("metadata", METADATA, proxy.metadata, origin.metadata);
}

/// The `ListObjectsV2` half: the rows column by column, the common prefixes, the envelope.
fn diff_list(out: &mut Vec<String>, proxy: &Answer, origin: &Answer, fields: Fields) {
    macro_rules! cmp {
        ($name:literal, $bit:ident, $lhs:expr, $rhs:expr) => {
            compare!(out, fields, $name, $bit, $lhs, $rhs);
        };
    }
    cmp!(
        "keys",
        KEYS,
        column(proxy, |r| r.key.clone()),
        column(origin, |r| r.key.clone())
    );
    cmp!(
        "sizes",
        SIZES,
        column(proxy, |r| r.size),
        column(origin, |r| r.size)
    );
    cmp!(
        "list-etags",
        ETAG,
        column(proxy, |r| r.etag.clone()),
        column(origin, |r| r.etag.clone())
    );
    cmp!(
        "list-last-modified",
        LAST_MODIFIED,
        column(proxy, |r| r.last_modified.clone()),
        column(origin, |r| r.last_modified.clone())
    );
    cmp!(
        "list-storage-class",
        STORAGE_CLASS,
        column(proxy, |r| r.storage_class.clone()),
        column(origin, |r| r.storage_class.clone())
    );
    cmp!(
        "list-owner",
        OWNER,
        column(proxy, |r| r.owner.clone()),
        column(origin, |r| r.owner.clone())
    );
    cmp!("common-prefixes", PREFIXES, proxy.prefixes, origin.prefixes);
    cmp!("paging", PAGING, proxy.paging, origin.paging);
}

/// One field of every LIST row, in order — the vector form a key-sequence assertion wants.
fn column<T>(answer: &Answer, field: impl Fn(&Row) -> T) -> Vec<T> {
    answer.rows.iter().map(field).collect()
}

/// Assert the two legs are indistinguishable over `fields`, naming the row.
///
/// # Panics
///
/// When any compared field differs, listing every one with both values.
pub fn assert_same(what: &str, proxy: &Answer, origin: &Answer, fields: Fields) {
    let diffs = diff(proxy, origin, fields);
    assert!(
        diffs.is_empty(),
        "[{what}] the proxy's answer differs from the origin's:\n  - {}",
        diffs.join("\n  - ")
    );
}

/// The two legs of every differential row, over one bucket.
pub struct Routes {
    /// The caching proxy under test.
    pub proxy: CachingProxy,
    /// The origin, reached directly — the reference answer.
    pub origin: s3s_aws::Proxy,
    pub bucket: String,
}

/// Build both legs over `origin`'s bucket and start the proxy's background index sync.
/// Call it *after* seeding, so the warm-up sees the fixtures.
#[must_use]
pub fn routes(origin: &Origin, max_obj_bytes: usize) -> Routes {
    routes_over(origin, origin.bucket(), max_obj_bytes)
}

/// [`routes`], over a bucket the test made for itself.
#[must_use]
pub fn routes_over(origin: &Origin, bucket: &str, max_obj_bytes: usize) -> Routes {
    let proxy = super::proxy_over(&origin.counted_client(), max_obj_bytes, None);
    proxy.spawn_background_sync(vec![bucket.to_owned()]);
    Routes {
        proxy,
        origin: origin.direct_route(),
        bucket: bucket.to_owned(),
    }
}

/// Both legs, with a proxy whose index is never warmed: LIST passes through and no range
/// can be promoted, so the proxy is a pure forwarder. This is the passthrough baseline
/// every locally-served row has to reproduce — and it is deterministic, which "before the
/// background sync finishes" is not.
#[must_use]
pub fn routes_unsynced(origin: &Origin, max_obj_bytes: usize) -> Routes {
    Routes {
        proxy: super::proxy_over(&origin.counted_client(), max_obj_bytes, None),
        origin: origin.direct_route(),
        bucket: origin.bucket().to_owned(),
    }
}

impl Routes {
    /// A GET down both legs, compared over `fields`. The proxy leg goes first, so a row
    /// run twice reads "uncached, then cached". Returns the proxy's answer.
    pub async fn get(
        &self,
        what: &str,
        fields: Fields,
        input: impl Fn() -> GetObjectInput,
    ) -> Answer {
        let proxy = answer_get(&self.proxy, input()).await;
        let origin = answer_get(&self.origin, input()).await;
        assert_same(what, &proxy, &origin, fields);
        proxy
    }

    /// A HEAD down both legs, compared over `fields`.
    pub async fn head(
        &self,
        what: &str,
        fields: Fields,
        input: impl Fn() -> HeadObjectInput,
    ) -> Answer {
        let proxy = answer_head(&self.proxy, input()).await;
        let origin = answer_head(&self.origin, input()).await;
        assert_same(what, &proxy, &origin, fields);
        proxy
    }

    /// A `ListObjectsV2` down both legs, compared over `fields`.
    pub async fn list(
        &self,
        what: &str,
        fields: Fields,
        input: impl Fn() -> ListObjectsV2Input,
    ) -> Answer {
        let proxy = answer_list(&self.proxy, input()).await;
        let origin = answer_list(&self.origin, input()).await;
        assert_same(what, &proxy, &origin, fields);
        proxy
    }

    /// Walk continuation tokens to exhaustion down both legs and assert the *whole*
    /// traversal matches: every key in order and every common prefix in order. Token
    /// values are opaque per leg and are never compared — the sequence they reconstruct
    /// is the contract.
    ///
    /// The round-trip *count* is only bounded, not equated. An origin that cannot see
    /// past the page it just filled marks it truncated and hands out a token that yields
    /// one final empty page (`MinIO` does this; AWS S3 does not), while an index that can
    /// see the whole key set ends the walk on the last non-empty page. Both reconstruct
    /// the identical sequence, and no S3 client can be broken by the shorter walk — so
    /// the assertion is "within one page", which still catches a paging bug that costs a
    /// client round trips.
    ///
    /// # Panics
    ///
    /// When either traversal fails, does not terminate, or the two disagree.
    pub async fn walk(&self, what: &str, input: impl Fn() -> ListObjectsV2Input) {
        let proxy = walk_pages(&self.proxy, &input).await;
        let origin = walk_pages(&self.origin, &input).await;
        assert_eq!(proxy.0, origin.0, "[{what}] paged key sequence");
        assert_eq!(proxy.1, origin.1, "[{what}] paged common prefixes");
        let (pages, reference) = (
            i64::try_from(proxy.2).expect("page count fits"),
            i64::try_from(origin.2).expect("page count fits"),
        );
        assert!(
            (pages - reference).abs() <= 1,
            "[{what}] the walk took {pages} round trips against the origin's {reference}"
        );
    }
}

/// Follow `next_continuation_token` like a real client: every key, every prefix, and how
/// many round trips it took.
async fn walk_pages<R: S3 + ?Sized>(
    route: &R,
    input: &impl Fn() -> ListObjectsV2Input,
) -> (Vec<String>, Vec<String>, usize) {
    let (mut keys, mut prefixes, mut pages) = (Vec::new(), Vec::new(), 0usize);
    let mut token: Option<String> = None;
    loop {
        let page = route
            .list_objects_v2(request(ListObjectsV2Input {
                continuation_token: token.take(),
                ..input()
            }))
            .await
            .expect("a page of the walk");
        pages += 1;
        assert!(pages < 1000, "the walk did not terminate");
        keys.extend(
            page.output
                .contents
                .iter()
                .flatten()
                .filter_map(|o| o.key.clone()),
        );
        prefixes.extend(
            page.output
                .common_prefixes
                .iter()
                .flatten()
                .filter_map(|c| c.prefix.clone()),
        );
        match (
            page.output.is_truncated,
            page.output.next_continuation_token,
        ) {
            (Some(true), Some(next)) => token = Some(next),
            _ => return (keys, prefixes, pages),
        }
    }
}

/// One leg's answer to a GET.
pub async fn answer_get<R: S3 + ?Sized>(route: &R, input: GetObjectInput) -> Answer {
    match route.get_object(request(input)).await {
        Ok(resp) => Answer::from_get(resp).await,
        Err(err) => Answer::from_error(&err),
    }
}

/// One leg's answer to a HEAD.
pub async fn answer_head<R: S3 + ?Sized>(route: &R, input: HeadObjectInput) -> Answer {
    match route.head_object(request(input)).await {
        Ok(resp) => Answer::from_head(&resp),
        Err(err) => Answer::from_error(&err),
    }
}

/// One leg's answer to a `ListObjectsV2`.
pub async fn answer_list<R: S3 + ?Sized>(route: &R, input: ListObjectsV2Input) -> Answer {
    match route.list_objects_v2(request(input)).await {
        Ok(resp) => Answer::from_list(&resp),
        Err(err) => Answer::from_error(&err),
    }
}

/// A plain whole-object GET input, the base every GET row overrides.
#[must_use]
pub fn get_input(bucket: &str, key: &str) -> GetObjectInput {
    GetObjectInput {
        bucket: bucket.to_owned(),
        key: key.to_owned(),
        ..Default::default()
    }
}

/// A plain HEAD input, the base every HEAD row overrides.
#[must_use]
pub fn head_input(bucket: &str, key: &str) -> HeadObjectInput {
    HeadObjectInput {
        bucket: bucket.to_owned(),
        key: key.to_owned(),
        ..Default::default()
    }
}

/// A plain LIST input, the base every LIST row overrides.
#[must_use]
pub fn list_input(bucket: &str) -> ListObjectsV2Input {
    ListObjectsV2Input {
        bucket: bucket.to_owned(),
        ..Default::default()
    }
}
