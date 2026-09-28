//! Bounded, exhaustive LIST scans over disjoint lexicographic key ranges.

use std::collections::VecDeque;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures::future::try_join_all;
use groupnet::consistency::volatile_recovery::PublicationPermit;
use s3s::dto::ObjectStorageClass;
use tracing::{info, warn};

use super::{
    KeyIndex, ObjEntry, finish_bucket_sync_generation, standard_class, sync_listing_into_generation,
};

// Limit connection bursts even on large hosts or an excessive operator override.
const MAX_WORKERS: usize = 64;
const MAX_DISCOVERY_REQUESTS: usize = 64;
const MAX_PREFIX_DEPTH: usize = 8;

/// Limits for a full bucket index scan. Discovery uses extra origin LIST calls.
/// A single worker, or a zero discovery budget, uses the serial scan.
#[derive(Clone, Copy, Debug)]
pub struct ScanConfig {
    /// Maximum concurrent full-keyspace LIST chains.
    pub workers: usize,
    /// Maximum additional LIST requests used to find split boundaries.
    pub discovery_budget: usize,
}

impl Default for ScanConfig {
    fn default() -> Self {
        Self {
            workers: std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get),
            discovery_budget: 16,
        }
    }
}

impl ScanConfig {
    fn bounded(self) -> Self {
        Self {
            workers: self.workers.clamp(1, MAX_WORKERS),
            discovery_budget: self.discovery_budget.min(MAX_DISCOVERY_REQUESTS),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct KeyRange {
    lower: Option<String>,
    upper: Option<String>,
}

/// The boundaries are hints, never a list of prefixes to include. These adjacent
/// ranges cover every valid S3 key, including keys without `/` and Unicode keys.
fn ranges(boundaries: &[String]) -> Vec<KeyRange> {
    let mut result = Vec::with_capacity(boundaries.len() + 1);
    let mut lower = None;
    for boundary in boundaries {
        result.push(KeyRange {
            lower: lower.clone(),
            upper: Some(boundary.clone()),
        });
        lower = Some(boundary.clone());
    }
    result.push(KeyRange { lower, upper: None });
    result
}

fn choose_boundaries(prefixes: &[String], workers: usize) -> Vec<String> {
    let range_count = workers.min(prefixes.len() + 1);
    (1..range_count)
        .map(|part| prefixes[part * prefixes.len() / range_count].clone())
        .collect()
}

/// Discover a large set of sibling prefixes without assuming any application key
/// layout. A failed or incomplete discovery only loses parallelism. It never changes
/// the exhaustive ranges used by the final scan.
async fn discover_boundaries(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    config: ScanConfig,
) -> Vec<String> {
    if config.workers == 1 || config.discovery_budget == 0 {
        return Vec::new();
    }
    let mut queue = VecDeque::from([String::new()]);
    let mut best = Vec::new();
    let mut used = 0usize;
    while let Some(prefix) = queue.pop_front() {
        if used >= config.discovery_budget {
            break;
        }
        let mut children = Vec::new();
        let mut token: Option<String> = None;
        let complete = loop {
            if used >= config.discovery_budget {
                break false;
            }
            let mut req = client
                .list_objects_v2()
                .bucket(bucket)
                .prefix(&prefix)
                .delimiter("/")
                .max_keys(1000);
            if let Some(value) = &token {
                req = req.continuation_token(value);
            }
            used += 1;
            let response = match req.send().await {
                Ok(response) => response,
                Err(error) => {
                    warn!(
                        "index split discovery failed for `{bucket}`: {error}; using available ranges"
                    );
                    return choose_boundaries(&best, config.workers);
                }
            };
            children.extend(
                response
                    .common_prefixes()
                    .iter()
                    .filter_map(|child| child.prefix().map(str::to_owned)),
            );
            if children.is_empty() {
                // A flat bucket offers no delimiter split. Do not paginate it
                // again merely to learn that it is flat.
                return choose_boundaries(&best, config.workers);
            }
            if !response.is_truncated().unwrap_or(false) {
                break true;
            }
            token = response.next_continuation_token().map(str::to_owned);
            if token.is_none() {
                break false;
            }
        };
        if !complete {
            break;
        }
        children.sort();
        children.dedup();
        if children.len() > best.len() {
            best = children.clone();
        }
        if best.len() >= config.workers * 8 {
            break;
        }
        if prefix.matches('/').count() < MAX_PREFIX_DEPTH {
            queue.extend(children);
        }
    }
    info!(
        bucket,
        discovery_requests = used,
        candidates = best.len(),
        "planned index split scan"
    );
    choose_boundaries(&best, config.workers)
}

async fn scan_range(
    client: &aws_sdk_s3::Client,
    state: &KeyIndex,
    bucket: &str,
    generation: u64,
    range: KeyRange,
    permit: Option<PublicationPermit>,
) -> anyhow::Result<usize> {
    let mut token: Option<String> = None;
    let mut previous: Option<String> = None;
    let mut found = 0usize;
    let mut empty_pages = 0usize;
    loop {
        let mut req = client.list_objects_v2().bucket(bucket).max_keys(1000);
        if let Some(value) = &token {
            req = req.continuation_token(value);
        } else if let Some(lower) = &range.lower {
            req = req.start_after(lower);
        }
        let response = req.send().await?;
        let response_key_count = response.contents().len();
        let mut rows = Vec::new();
        let mut past_upper = false;
        for obj in response.contents() {
            let key = obj
                .key()
                .ok_or_else(|| anyhow::anyhow!("origin LIST omitted a key"))?;
            if previous.as_deref().is_some_and(|last| key <= last)
                || range.lower.as_deref().is_some_and(|lower| key <= lower)
            {
                anyhow::bail!("origin LIST returned keys out of order");
            }
            previous = Some(key.to_owned());
            if range.upper.as_deref().is_some_and(|upper| key > upper) {
                past_upper = true;
                break;
            }
            let last_modified = obj.last_modified().map_or_else(SystemTime::now, |stamp| {
                u64::try_from(stamp.secs()).map_or_else(
                    |_| SystemTime::now(),
                    |secs| UNIX_EPOCH + Duration::new(secs, stamp.subsec_nanos()),
                )
            });
            rows.push((
                key.to_owned(),
                ObjEntry {
                    size: obj.size(),
                    last_modified,
                    etag: obj.e_tag().and_then(|raw| raw.parse().ok()),
                    storage_class: obj.storage_class().map_or_else(standard_class, |class| {
                        ObjectStorageClass::from(class.as_str().to_owned())
                    }),
                    content_type: None,
                    meta: None,
                },
            ));
        }
        let updated = match &permit {
            Some(permit) => permit
                .publish(|| sync_listing_into_generation(state, bucket, generation, rows))
                .flatten(),
            None => sync_listing_into_generation(state, bucket, generation, rows),
        };
        let Some(page_len) = updated else {
            anyhow::bail!("bucket sync superseded by a newer origin rebuild");
        };
        found += page_len;
        if past_upper || !response.is_truncated().unwrap_or(false) {
            break;
        }
        empty_pages = if response_key_count == 0 {
            empty_pages + 1
        } else {
            0
        };
        if empty_pages > 8 {
            anyhow::bail!("origin LIST returned too many empty truncated pages");
        }
        let next = response.next_continuation_token().ok_or_else(|| {
            anyhow::anyhow!("truncated origin LIST omitted its continuation token")
        })?;
        if token.as_deref() == Some(next) {
            anyhow::bail!("origin LIST repeated a continuation token");
        }
        token = Some(next.to_owned());
    }
    Ok(found)
}

/// Scan the full bucket under one rebuild generation. Every range must succeed
/// before the bucket can answer local LIST or negative HEAD requests.
///
/// # Errors
/// Returns an origin LIST error or a superseded-generation error.
pub(super) async fn sync_bucket_generation(
    client: &aws_sdk_s3::Client,
    state: &KeyIndex,
    bucket: &str,
    generation: u64,
    config: ScanConfig,
    permit: Option<PublicationPermit>,
) -> anyhow::Result<usize> {
    let config = config.bounded();
    let boundaries = discover_boundaries(client, bucket, config).await;
    let range_count = boundaries.len() + 1;
    let scans = ranges(&boundaries)
        .into_iter()
        .map(|range| scan_range(client, state, bucket, generation, range, permit.clone()));
    let counts = try_join_all(scans).await?;
    let found = counts.into_iter().sum();
    let finished = match &permit {
        Some(permit) => permit
            .publish(|| finish_bucket_sync_generation(state, bucket, generation))
            .unwrap_or(false),
        None => finish_bucket_sync_generation(state, bucket, generation),
    };
    if !finished {
        anyhow::bail!("bucket sync superseded by a newer origin rebuild");
    }
    info!(bucket, found, range_count, "synced bucket into index");
    Ok(found)
}

#[cfg(test)]
mod tests {
    use super::{choose_boundaries, ranges};

    #[test]
    fn boundaries_cover_empty_ascii_unicode_and_exact_boundary_keys_once() {
        let prefixes = vec!["ns/a/".to_owned(), "ns/c/".to_owned(), "ns/é/".to_owned()];
        let boundaries = choose_boundaries(&prefixes, 4);
        let partitions = ranges(&boundaries);
        for key in [
            "", "/", "ns/a/", "ns/a/key", "ns/b", "ns/c/", "ns/z", "ns/é/", "雪",
        ] {
            let matches = partitions
                .iter()
                .filter(|range| {
                    range.lower.as_deref().is_none_or(|lower| key > lower)
                        && range.upper.as_deref().is_none_or(|upper| key <= upper)
                })
                .count();
            assert_eq!(matches, 1, "{key:?} must have exactly one range");
        }
    }

    #[test]
    fn no_discovered_prefixes_uses_one_exhaustive_range() {
        assert_eq!(ranges(&choose_boundaries(&[], 8)).len(), 1);
    }
}

#[cfg(test)]
#[path = "scan_probe.rs"]
mod scan_probe;
