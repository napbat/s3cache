//! The in-memory LIST index: per-bucket sorted key sets, the `ListObjectsV2` algorithm
//! over them, and the background full-LIST bootstrap that warms a bucket.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ops::Bound;
use std::sync::RwLock;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use s3s::dto::{CommonPrefix, ListObjectsV2Input, ListObjectsV2Output, Object, Timestamp};
use tracing::info;

/// One indexed key's LIST metadata: its size and last-modified time.
#[derive(Clone)]
pub(crate) struct ObjEntry {
    pub(crate) size: i64,
    pub(crate) last_modified: SystemTime,
}

/// Per-bucket LIST index: the sorted key set, whether its warm-up sync has
/// finished, and delete tombstones so a late-arriving cross-writer put
/// cannot resurrect a deleted key.
#[derive(Default)]
pub(crate) struct BucketState {
    pub(crate) synced: bool,
    pub(crate) keys: BTreeMap<String, ObjEntry>,
    /// Deleted keys and when: consulted by [`apply_put`], pruned amortized.
    pub(crate) gone: BTreeMap<String, SystemTime>,
}

/// How long a delete tombstone shields its key from older, late-arriving
/// cross-writer puts. Conflicts past this window resolve at the next origin
/// sync — the origin stays the authority, this index is a cache of it.
const TOMBSTONE_TTL: Duration = Duration::from_hours(1);

/// Tombstones per bucket before an amortized TTL prune runs.
const TOMBSTONE_PRUNE_LEN: usize = 65_536;

/// Applies an observed put (a local write, a peer's feed event, or a read
/// observation) by per-key last-writer-wins: a strictly newer entry or an
/// equal-or-newer tombstone rejects it; ties between puts fall to
/// last-applied (cross-writer same-millisecond puts are healed by the next
/// origin sync). Returns whether the index changed.
pub(crate) fn apply_put(
    state: &RwLock<HashMap<String, BucketState>>,
    bucket: &str,
    key: &str,
    size: i64,
    ts: SystemTime,
) -> bool {
    let mut g = state.write().unwrap();
    let b = g.entry(bucket.to_owned()).or_default();
    if b.gone.get(key).is_some_and(|dead| *dead >= ts) {
        return false; // deletes win ties: never resurrect
    }
    if b.keys.get(key).is_some_and(|e| e.last_modified > ts) {
        return false; // a newer put is already indexed
    }
    b.keys.insert(
        key.to_owned(),
        ObjEntry {
            size,
            last_modified: ts,
        },
    );
    true
}

/// Applies an observed delete: removes any not-newer entry and records a
/// tombstone (see [`apply_put`]). Returns whether a live entry was removed.
pub(crate) fn apply_del(
    state: &RwLock<HashMap<String, BucketState>>,
    bucket: &str,
    key: &str,
    ts: SystemTime,
) -> bool {
    let mut g = state.write().unwrap();
    let b = g.entry(bucket.to_owned()).or_default();
    if b.gone.len() > TOMBSTONE_PRUNE_LEN
        && let Some(cutoff) = ts.checked_sub(TOMBSTONE_TTL)
    {
        b.gone.retain(|_, dead| *dead >= cutoff);
    }
    let dead = b.gone.entry(key.to_owned()).or_insert(ts);
    if *dead < ts {
        *dead = ts;
    }
    if b.keys.get(key).is_some_and(|e| e.last_modified > ts) {
        return false; // the key was rewritten after this delete
    }
    b.keys.remove(key).is_some()
}

/// The `ListObjectsV2` algorithm over an already-borrowed key index — free-standing so it
/// is unit-testable without a live proxy. Matches S3: sorted keys, prefix filter,
/// delimiter roll-up into common prefixes, max-keys paging with a key continuation token
/// (resumed *inclusively*, since the token is the next key to return), and `start_after`
/// (exclusive, first page only).
pub(crate) fn list_objects_v2_from_index(
    keys: Option<&BTreeMap<String, ObjEntry>>,
    inp: &ListObjectsV2Input,
) -> ListObjectsV2Output {
    let bucket = inp.bucket.as_str();
    let prefix = inp.prefix.clone().unwrap_or_default();
    let delim = inp.delimiter.clone();
    let max = usize::try_from(inp.max_keys.unwrap_or(1000).clamp(1, 1000)).unwrap_or(1000);

    let mut contents: Vec<Object> = Vec::new();
    let mut common: BTreeSet<String> = BTreeSet::new();
    let mut truncated = false;
    let mut next_token = None;

    if let Some(keys) = keys {
        let lower = if let Some(token) = &inp.continuation_token {
            Bound::Included(token.clone())
        } else if let Some(sa) = &inp.start_after {
            Bound::Excluded(sa.clone())
        } else {
            Bound::Unbounded
        };
        for (key, entry) in keys.range((lower, Bound::Unbounded)) {
            if !key.starts_with(&prefix) {
                if key.as_str() > prefix.as_str() {
                    break; // sorted: past the prefix block
                }
                continue;
            }
            let count = contents.len() + common.len();
            if let Some(d) = &delim {
                let rest = &key[prefix.len()..];
                if let Some(idx) = rest.find(d.as_str()) {
                    let cp = format!("{prefix}{}", &rest[..idx + d.len()]);
                    if !common.contains(&cp) {
                        if count >= max {
                            truncated = true;
                            next_token = Some(key.clone());
                            break;
                        }
                        common.insert(cp);
                    }
                    continue;
                }
            }
            if count >= max {
                truncated = true;
                next_token = Some(key.clone());
                break;
            }
            contents.push(Object {
                key: Some(key.clone()),
                size: Some(entry.size),
                last_modified: Some(Timestamp::from(entry.last_modified)),
                ..Default::default()
            });
        }
    }

    let key_count = i32::try_from(contents.len() + common.len()).unwrap_or(i32::MAX);
    ListObjectsV2Output {
        name: Some(bucket.to_owned()),
        prefix: Some(prefix),
        max_keys: Some(i32::try_from(max).unwrap_or(1000)),
        key_count: Some(key_count),
        is_truncated: Some(truncated),
        continuation_token: inp.continuation_token.clone(),
        next_continuation_token: next_token,
        contents: (!contents.is_empty()).then_some(contents),
        common_prefixes: (!common.is_empty()).then(|| {
            common
                .into_iter()
                .map(|p| CommonPrefix { prefix: Some(p) })
                .collect()
        }),
        delimiter: delim,
        start_after: inp.start_after.clone(),
        ..Default::default()
    }
}

/// Full paginated LIST of a bucket into `state`, then mark it synced. Merges (never
/// clears) so a write that raced the sync isn't lost. Free-standing (takes the client +
/// shared index) so the background warm-up task can run it without borrowing the proxy,
/// which the S3 service owns by value.
pub(crate) async fn sync_bucket_into(
    client: &aws_sdk_s3::Client,
    state: &RwLock<HashMap<String, BucketState>>,
    bucket: &str,
) -> anyhow::Result<usize> {
    let mut token: Option<String> = None;
    let mut found = 0usize;
    loop {
        let mut req = client.list_objects_v2().bucket(bucket).max_keys(1000);
        if let Some(t) = &token {
            req = req.continuation_token(t);
        }
        let resp = req.send().await?;
        for obj in resp.contents() {
            if let Some(key) = obj.key() {
                let last_modified = obj
                    .last_modified()
                    .and_then(|d| u64::try_from(d.secs()).ok())
                    .map_or_else(SystemTime::now, |s| UNIX_EPOCH + Duration::from_secs(s));
                let entry = ObjEntry {
                    size: obj.size().unwrap_or(0),
                    last_modified,
                };
                let mut g = state.write().unwrap();
                g.entry(bucket.to_owned())
                    .or_default()
                    .keys
                    .insert(key.to_owned(), entry);
                found += 1;
            }
        }
        if resp.is_truncated().unwrap_or(false) {
            token = resp.next_continuation_token().map(str::to_owned);
            if token.is_none() {
                break;
            }
        } else {
            break;
        }
    }
    state
        .write()
        .unwrap()
        .entry(bucket.to_owned())
        .or_default()
        .synced = true;
    info!("synced bucket `{bucket}` into index: {found} keys");
    Ok(found)
}

#[cfg(test)]
mod tests {
    use super::{ObjEntry, apply_del, apply_put, list_objects_v2_from_index};
    use std::collections::HashMap;
    use std::sync::RwLock;
    use std::time::Duration;

    type Index = RwLock<HashMap<String, super::BucketState>>;

    fn ts(secs: u64) -> std::time::SystemTime {
        UNIX_EPOCH + Duration::from_secs(secs)
    }

    fn size_of(state: &Index, key: &str) -> Option<i64> {
        state
            .read()
            .unwrap()
            .get("b")
            .and_then(|b| b.keys.get(key))
            .map(|e| e.size)
    }

    /// Cross-writer events arrive in any order; per-key LWW must converge:
    /// older puts lose, deletes win ties, and a delete's tombstone blocks a
    /// late older put from resurrecting the key.
    #[test]
    fn lww_applies_out_of_order_events_convergently() {
        let state: Index = RwLock::new(HashMap::new());
        assert!(apply_put(&state, "b", "k", 1, ts(10)));
        assert!(!apply_put(&state, "b", "k", 9, ts(5)), "older put loses");
        assert_eq!(size_of(&state, "k"), Some(1));
        assert!(apply_put(&state, "b", "k", 2, ts(20)), "newer put wins");
        assert_eq!(size_of(&state, "k"), Some(2));

        // Delete at t=30; an older put (t=25) must NOT resurrect the key.
        assert!(apply_del(&state, "b", "k", ts(30)));
        assert!(!apply_put(&state, "b", "k", 3, ts(25)), "tombstoned");
        assert_eq!(size_of(&state, "k"), None);
        // Deletes win timestamp ties, in either arrival order.
        assert!(apply_put(&state, "b", "tie", 1, ts(40)));
        apply_del(&state, "b", "tie", ts(40));
        assert_eq!(size_of(&state, "tie"), None, "delete wins the tie");
        assert!(
            !apply_put(&state, "b", "tie", 2, ts(40)),
            "still tombstoned"
        );

        // A genuinely newer put after a delete brings the key back.
        assert!(apply_put(&state, "b", "k", 4, ts(35)));
        assert_eq!(size_of(&state, "k"), Some(4));
    }

    /// A delete observed before its key's put (cross-writer reorder) still
    /// suppresses the older put.
    #[test]
    fn delete_first_reorder_suppresses_the_put() {
        let state: Index = RwLock::new(HashMap::new());
        assert!(!apply_del(&state, "b", "k", ts(50)), "nothing live yet");
        assert!(
            !apply_put(&state, "b", "k", 1, ts(45)),
            "arrives late, loses"
        );
        assert_eq!(size_of(&state, "k"), None);
    }
    use s3s::dto::{ListObjectsV2Input, ListObjectsV2Output};
    use std::collections::BTreeMap;
    use std::time::UNIX_EPOCH;

    fn index(keys: &[&str]) -> BTreeMap<String, ObjEntry> {
        keys.iter()
            .map(|k| {
                (
                    (*k).to_owned(),
                    ObjEntry {
                        size: 1,
                        last_modified: UNIX_EPOCH,
                    },
                )
            })
            .collect()
    }

    fn list_input(
        max: i32,
        token: Option<&str>,
        prefix: &str,
        delim: Option<&str>,
        start_after: Option<&str>,
    ) -> ListObjectsV2Input {
        ListObjectsV2Input {
            bucket: "b".to_owned(),
            max_keys: Some(max),
            continuation_token: token.map(str::to_owned),
            prefix: (!prefix.is_empty()).then(|| prefix.to_owned()),
            delimiter: delim.map(str::to_owned),
            start_after: start_after.map(str::to_owned),
            ..Default::default()
        }
    }

    fn page_keys(out: &ListObjectsV2Output) -> Vec<String> {
        out.contents
            .iter()
            .flatten()
            .filter_map(|o| o.key.clone())
            .collect()
    }
    fn page_prefixes(out: &ListObjectsV2Output) -> Vec<String> {
        out.common_prefixes
            .iter()
            .flatten()
            .filter_map(|c| c.prefix.clone())
            .collect()
    }

    /// Follow the continuation tokens like a real client and collect every key + prefix.
    fn walk_pages(
        idx: &BTreeMap<String, ObjEntry>,
        max: i32,
        prefix: &str,
        delim: Option<&str>,
    ) -> (Vec<String>, Vec<String>) {
        let (mut keys, mut prefixes) = (Vec::new(), Vec::new());
        let mut token: Option<String> = None;
        for _ in 0..10_000 {
            let inp = list_input(max, token.as_deref(), prefix, delim, None);
            let out = list_objects_v2_from_index(Some(idx), &inp);
            keys.extend(page_keys(&out));
            prefixes.extend(page_prefixes(&out));
            match (out.is_truncated, out.next_continuation_token) {
                (Some(true), Some(t)) => token = Some(t),
                _ => break,
            }
        }
        (keys, prefixes)
    }

    #[test]
    fn list_pagination_loses_nothing() {
        let idx = index(&["a", "b", "c", "d", "e"]);
        // Every page size must reproduce the full ordered key set — no gaps, no dups.
        for max in 1..=6 {
            let (keys, _) = walk_pages(&idx, max, "", None);
            assert_eq!(keys, ["a", "b", "c", "d", "e"], "max_keys={max}");
        }
    }

    #[test]
    fn list_prefix_and_delimiter() {
        let idx = index(&["p/a/1", "p/a/2", "p/b/1", "p/top"]);
        let out =
            list_objects_v2_from_index(Some(&idx), &list_input(1000, None, "p/a/", None, None));
        assert_eq!(page_keys(&out), ["p/a/1", "p/a/2"]);
        let out =
            list_objects_v2_from_index(Some(&idx), &list_input(1000, None, "p/", Some("/"), None));
        assert_eq!(page_prefixes(&out), ["p/a/", "p/b/"]);
        assert_eq!(page_keys(&out), ["p/top"]);
    }

    #[test]
    fn list_delimiter_pagination_no_dup_prefix() {
        let idx = index(&["a/1", "a/2", "b/1", "c/1"]);
        // Each common prefix must appear exactly once across paged results.
        let (keys, prefixes) = walk_pages(&idx, 1, "", Some("/"));
        assert!(keys.is_empty());
        assert_eq!(prefixes, ["a/", "b/", "c/"]);
    }

    #[test]
    fn list_start_after_is_exclusive() {
        let idx = index(&["a", "b", "c", "d"]);
        let out =
            list_objects_v2_from_index(Some(&idx), &list_input(1000, None, "", None, Some("b")));
        assert_eq!(page_keys(&out), ["c", "d"]);
    }

    #[test]
    fn list_empty_bucket() {
        let out = list_objects_v2_from_index(None, &list_input(1000, None, "", None, None));
        assert_eq!(out.is_truncated, Some(false));
        assert!(out.contents.is_none());
        assert_eq!(out.key_count, Some(0));
    }
}
