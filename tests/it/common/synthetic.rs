//! The forwarder's synthetic listing: a production-sized bucket that answers
//! LIST without `MinIO` holding its objects, merged with every object a test
//! writes or deletes through the forwarder, so an origin scan still reports the
//! bucket a client would see.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::ops::Bound;
use std::sync::Mutex;

use bytes::Bytes;
use http::{HeaderMap, Method, Response, StatusCode};
use http_body_util::{BodyExt, Full, combinators::BoxBody};

use super::percent_decoded;

/// Key `n` of a synthetic listing. Fixed width, so lexicographic order is
/// numeric order, and as long as a production document key, so a synthetic
/// row encodes to about the measured production image row.
#[must_use]
pub fn synthetic_key(n: usize) -> String {
    format!("tenants/acme/documents/{n:08}/{:032x}.json", mix(n, 1))
}

/// The `ETag` a synthetic listing reports for key `n`, quoted as on the wire.
fn synthetic_etag(n: usize) -> String {
    format!("\"{:032x}\"", mix(n, 2))
}

fn synthetic_size(n: usize) -> u64 {
    1_024 + u64::try_from(n % 65_536).expect("small size")
}

/// A fixed 128-bit mix of `n`: realistic, uncorrelated hex for keys and tags.
fn mix(n: usize, lane: u64) -> u128 {
    let mut z = u64::try_from(n).expect("row fits u64") ^ lane.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    let mut next = || {
        z = z.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut x = z;
        x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        x ^ (x >> 31)
    };
    (u128::from(next()) << 64) | u128::from(next())
}

/// One listed row: quoted `ETag`, size, and `LastModified` as S3 writes it.
#[derive(Clone)]
struct Row {
    etag: String,
    size: u64,
    modified: String,
}

/// Objects written (`Some`) or deleted (`None`) through the forwarder while a
/// synthetic listing answers LIST. Each entry overrides the synthetic row of
/// the same key.
#[derive(Default)]
pub(super) struct Overlay(Mutex<BTreeMap<String, Option<Row>>>);

impl Overlay {
    /// Fold one successful single-object `PUT` or `DELETE` of `key` into the
    /// listing. Copies, multipart parts and batch deletes are not modelled.
    pub(super) fn observe(
        &self,
        method: &Method,
        key: &str,
        query: &str,
        request: &HeaderMap,
        response: &HeaderMap,
    ) {
        let multipart = query
            .split('&')
            .any(|pair| pair.starts_with("uploadId=") || pair.starts_with("partNumber="));
        let row = match *method {
            Method::PUT if !multipart && !request.contains_key("x-amz-copy-source") => {
                let header = |name: &str| {
                    request
                        .get(name)
                        .and_then(|value| value.to_str().ok())
                        .and_then(|value| value.parse::<u64>().ok())
                };
                Some(Row {
                    etag: response
                        .get("etag")
                        .and_then(|value| value.to_str().ok())
                        .expect("a PUT response carries its ETag")
                        .to_owned(),
                    size: header("x-amz-decoded-content-length")
                        .or_else(|| header("content-length"))
                        .expect("a sized PUT"),
                    modified: now_rfc3339(),
                })
            }
            Method::DELETE if !multipart => None,
            _ => return,
        };
        self.0.lock().unwrap().insert(percent_decoded(key), row);
    }

    /// One `ListObjectsV2` page of `rows` synthetic keys merged with this
    /// overlay. The continuation token is the hex of the last key returned;
    /// `start-after` resumes after its key. Prefix and delimiter listings are
    /// not modelled and fail loudly.
    pub(super) fn list(
        &self,
        bucket: &str,
        query: &str,
        rows: usize,
    ) -> Response<BoxBody<Bytes, std::io::Error>> {
        let Some(ListQuery {
            after,
            max_keys,
            token,
        }) = ListQuery::parse(query)
        else {
            return Response::builder()
                .status(StatusCode::NOT_IMPLEMENTED)
                .body(
                    Full::new(Bytes::from_static(b"synthetic listing: no prefix scans"))
                        .map_err(|never| match never {})
                        .boxed(),
                )
                .expect("a 501 is well-formed");
        };
        let overlay = self.0.lock().unwrap();
        let mut synthetic = after.as_deref().map_or(0, |after| {
            let (mut low, mut high) = (0, rows);
            while low < high {
                let mid = low + (high - low) / 2;
                if synthetic_key(mid).as_str() <= after {
                    low = mid + 1;
                } else {
                    high = mid;
                }
            }
            low
        });
        let lower = after.map_or(Bound::Unbounded, Bound::Excluded);
        let mut written = overlay
            .range::<String, _>((lower, Bound::Unbounded))
            .peekable();
        // The merged listing, one key at a time: the smaller of the next
        // synthetic and the next overlay key, the overlay winning a tie.
        let mut next = || {
            loop {
                let own = (synthetic < rows).then(|| synthetic_key(synthetic));
                let take_overlay = match (own.as_deref(), written.peek()) {
                    (None, None) => return None,
                    (Some(_), None) => false,
                    (None, Some(_)) => true,
                    (Some(own), Some((key, _))) => key.as_str() <= own,
                };
                if take_overlay {
                    let (key, row) = written.next().expect("peeked");
                    if own.as_deref() == Some(key.as_str()) {
                        synthetic += 1;
                    }
                    if let Some(row) = row {
                        return Some((key.clone(), row.clone()));
                    }
                } else {
                    let n = synthetic;
                    synthetic += 1;
                    return Some((
                        own.expect("a synthetic row"),
                        Row {
                            etag: synthetic_etag(n),
                            size: synthetic_size(n),
                            modified: "2026-09-30T00:00:00.000Z".to_owned(),
                        },
                    ));
                }
            }
        };
        let mut page = Vec::with_capacity(max_keys);
        while page.len() < max_keys {
            let Some(row) = next() else { break };
            page.push(row);
        }
        let truncated = next().is_some();
        page_response(bucket, max_keys, token, truncated, &page)
    }
}

/// One `ListObjectsV2` response carrying `page`.
fn page_response(
    bucket: &str,
    max_keys: usize,
    token: Option<String>,
    truncated: bool,
    page: &[(String, Row)],
) -> Response<BoxBody<Bytes, std::io::Error>> {
    let mut xml = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ListBucketResult \
         xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Name>{bucket}</Name><Prefix></Prefix>\
         <KeyCount>{}</KeyCount><MaxKeys>{max_keys}</MaxKeys><IsTruncated>{truncated}</IsTruncated>",
        page.len(),
    );
    if let Some(token) = token {
        write!(xml, "<ContinuationToken>{token}</ContinuationToken>").expect("to a String");
    }
    if truncated && let Some((last, _)) = page.last() {
        write!(
            xml,
            "<NextContinuationToken>{}</NextContinuationToken>",
            hex(last)
        )
        .expect("to a String");
    }
    for (key, row) in page {
        write!(
            xml,
            "<Contents><Key>{key}</Key><LastModified>{}</LastModified>\
             <ETag>{}</ETag><Size>{}</Size><StorageClass>STANDARD</StorageClass></Contents>",
            row.modified,
            row.etag.replace('"', "&quot;"),
            row.size,
        )
        .expect("to a String");
    }
    xml.push_str("</ListBucketResult>");
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/xml")
        .body(
            Full::new(Bytes::from(xml))
                .map_err(|never| match never {})
                .boxed(),
        )
        .expect("a synthetic LIST page is well-formed")
}

/// The `ListObjectsV2` parameters the synthetic listing models.
struct ListQuery {
    /// List keys after this one.
    after: Option<String>,
    max_keys: usize,
    /// The continuation token as the client sent it.
    token: Option<String>,
}

impl ListQuery {
    /// `None` for a prefix or delimiter listing, which is not modelled.
    fn parse(query: &str) -> Option<Self> {
        let mut parsed = Self {
            after: None,
            max_keys: 1_000,
            token: None,
        };
        for pair in query.split('&') {
            let (name, value) = pair.split_once('=').unwrap_or((pair, ""));
            match name {
                "max-keys" => {
                    parsed.max_keys = value.parse::<usize>().expect("max-keys").clamp(1, 1_000);
                }
                "continuation-token" => {
                    parsed.after = Some(unhex(value));
                    parsed.token = Some(value.to_owned());
                }
                "start-after" => parsed.after = Some(percent_decoded(value)),
                "prefix" | "delimiter" if !value.is_empty() => return None,
                _ => {}
            }
        }
        Some(parsed)
    }
}

fn hex(key: &str) -> String {
    key.bytes()
        .fold(String::with_capacity(key.len() * 2), |mut out, byte| {
            write!(out, "{byte:02x}").expect("to a String");
            out
        })
}

fn unhex(token: &str) -> String {
    let bytes = (0..token.len())
        .step_by(2)
        .map(|at| {
            u8::from_str_radix(&token[at..at + 2], 16).expect("a synthetic continuation token")
        })
        .collect();
    String::from_utf8(bytes).expect("a UTF-8 key")
}

/// Now as S3 writes `LastModified`, to the whole second.
fn now_rfc3339() -> String {
    let now = time::OffsetDateTime::now_utc();
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.000Z",
        now.year(),
        u8::from(now.month()),
        now.day(),
        now.hour(),
        now.minute(),
        now.second()
    )
}
