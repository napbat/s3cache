//! Integration tests for the tiered object-body cache, driven entirely through the
//! library's public surface — the reason `s3cache` is a lib + a thin bin.
//!
//! The properties here are the ones the unit tests can't reach from inside a single tier:
//! that an object evicted from the hot tier is still served (from disk), that the warm
//! tier survives the process, that the per-object cap and the disk budget are enforced
//! without ever failing a read, and that a fill hits the origin exactly once.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use s3cache::metrics::Metrics;
use s3cache::tier::{CacheKey, CachedObject, TieredCache, buffer_body, open_warm};
use s3s::dto::{ETag, GetObjectOutput};

/// Room for every warm tier that isn't the subject of the test.
const AMPLE: u64 = 8 * 1024 * 1024;
/// Per-object cap for every warm tier that isn't testing the cap.
const NO_CAP: usize = 1024 * 1024;

/// A scratch directory for one test's warm tier, removed on the way out (including on a
/// panic, so a failure never leaves the next run a dirty tier to re-index).
struct TempDir(PathBuf);

impl TempDir {
    fn new(tag: &str) -> Self {
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let seq = SEQ.fetch_add(1, Ordering::Relaxed);
        let dir =
            std::env::temp_dir().join(format!("s3cache-it-{tag}-{}-{seq}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        Self(dir)
    }

    fn path(&self) -> PathBuf {
        self.0.clone()
    }

    /// The tier's entries: one file per key, so this is what disk is holding.
    fn files(&self) -> usize {
        entries(&self.0).count()
    }

    /// Total bytes the tier is holding on disk.
    fn bytes(&self) -> u64 {
        entries(&self.0).map(|entry| entry.len()).sum()
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn entries(dir: &Path) -> impl Iterator<Item = std::fs::Metadata> {
    std::fs::read_dir(dir)
        .into_iter()
        .flatten()
        .filter_map(|entry| entry.ok()?.metadata().ok())
        .filter(std::fs::Metadata::is_file)
}

fn key(name: &str) -> CacheKey {
    ("bkt".to_owned(), name.to_owned())
}

/// A `len`-byte body unique to `tag`, so a wrong hit is a failed assertion, not a pass.
fn body(tag: &str, len: usize) -> Bytes {
    tag.bytes().cycle().take(len).collect::<Bytes>()
}

/// A cacheable object carrying enough metadata to prove the whole entry round-tripped.
fn object(tag: &str, len: usize) -> Arc<CachedObject> {
    let out = GetObjectOutput {
        content_length: i64::try_from(len).ok(),
        content_type: Some("application/octet-stream".to_owned()),
        e_tag: Some(ETag::Strong(format!("\"{tag}\""))),
        ..Default::default()
    };
    Arc::new(CachedObject::from_get(&out, body(tag, len)))
}

/// A cached object's body, read back the way the proxy serves it.
async fn served_body(obj: &CachedObject) -> Bytes {
    let out = obj.to_get();
    buffer_body(out.body.expect("a cached GET carries a body"), NO_CAP)
        .await
        .expect("the body is within the cap")
}

fn cache(dir: &TempDir, hot_bytes: u64, disk_bytes: u64, max_obj_bytes: usize) -> TieredCache {
    let metrics = Arc::new(Metrics::default());
    let warm = open_warm(dir.path(), disk_bytes, max_obj_bytes, Arc::clone(&metrics))
        .expect("warm tier opens");
    TieredCache::new(hot_bytes, Some(warm), metrics)
}

/// Inserts `count` distinct `len`-byte objects, `obj-0` first.
async fn fill(cache: &TieredCache, count: usize, len: usize) {
    for i in 0..count {
        let tag = format!("obj-{i}");
        cache.insert(key(&tag), object(&tag, len)).await;
    }
}

/// How many of [`fill`]'s objects the cache can no longer serve.
async fn misses(cache: &TieredCache, count: usize) -> usize {
    let mut misses = 0;
    for i in 0..count {
        if cache.get(&key(&format!("obj-{i}"))).await.is_none() {
            misses += 1;
        }
    }
    misses
}

/// The rollover property: objects the hot tier has dropped are still served, from disk.
///
/// A hot-only control runs the identical workload first, so the test proves the pressure
/// is real rather than assuming it — moka evicts lazily, and an assertion that "everything
/// is still served" means nothing if nothing was ever evicted. Stated this way round it is
/// also not flaky: it never asserts *when* the hot tier gives an object up.
#[tokio::test]
async fn objects_evicted_from_hot_are_still_served_from_the_warm_tier() {
    const HOT_BYTES: u64 = 4 * 1024;
    const BODY: usize = 1024;
    const COUNT: usize = 512; // 512 KiB of bodies against a 4 KiB hot tier

    let hot_only = TieredCache::new(HOT_BYTES, None, Arc::new(Metrics::default()));
    fill(&hot_only, COUNT, BODY).await;
    let evicted = misses(&hot_only, COUNT).await;
    assert!(
        evicted > 0,
        "a {HOT_BYTES}-byte hot tier must drop objects under {COUNT} x {BODY}-byte fills, \
         or this test proves nothing"
    );

    let dir = TempDir::new("rollover");
    let layered = cache(&dir, HOT_BYTES, AMPLE, NO_CAP);
    fill(&layered, COUNT, BODY).await;
    // Warm is inclusive: every fill wrote disk too, so hot is free to evict at will.
    assert_eq!(dir.files(), COUNT, "every insert also filled the warm tier");
    assert_eq!(
        misses(&layered, COUNT).await,
        0,
        "the same pressure that lost {evicted} objects from hot loses none through warm"
    );
    for i in 0..COUNT {
        let tag = format!("obj-{i}");
        let obj = layered.get(&key(&tag)).await.expect("served");
        assert_eq!(
            served_body(&obj).await,
            body(&tag, BODY),
            "{tag} served intact"
        );
    }
}

/// A fresh cache over the same directory comes up warm: the disk tier re-indexes its
/// files, so a restarted node serves from disk instead of stampeding the origin.
#[tokio::test]
async fn the_warm_tier_survives_a_restart_into_a_cold_hot_tier() {
    let dir = TempDir::new("restart");
    {
        let cache = cache(&dir, 1024 * 1024, AMPLE, NO_CAP);
        cache.insert(key("survivor"), object("survivor", 512)).await;
        assert!(
            cache.get(&key("survivor")).await.is_some(),
            "cached before the restart"
        );
    } // the cache — hot tier included — is gone here

    let restarted = cache(&dir, 1024 * 1024, AMPLE, NO_CAP);
    let obj = restarted
        .get(&key("survivor"))
        .await
        .expect("re-indexed from disk after the restart");
    assert_eq!(served_body(&obj).await, body("survivor", 512));
    assert_eq!(
        obj.to_head().e_tag,
        Some(ETag::Strong("\"survivor\"".to_owned())),
        "the response metadata rode along with the body"
    );
}

/// `S3CACHE_MAX_OBJECT_BYTES` is a disk-fill policy, not a read failure: an object whose
/// encoding is over the cap is skipped by the warm tier and still served from hot.
#[tokio::test]
async fn oversize_objects_skip_the_disk_fill_and_still_serve() {
    const MAX_OBJ: usize = 512;
    const BODY: usize = 4096;

    let dir = TempDir::new("cap");
    {
        let cache = cache(&dir, 1024 * 1024, AMPLE, MAX_OBJ);
        cache.insert(key("too-big"), object("too-big", BODY)).await;

        let obj = cache
            .get(&key("too-big"))
            .await
            .expect("the disk rejection never blocks the hot fill");
        assert_eq!(served_body(&obj).await, body("too-big", BODY));
        assert_eq!(dir.files(), 0, "nothing over the cap reached disk");
    }

    // With the hot tier gone there is nothing to fall back on: it was never persisted.
    let restarted = cache(&dir, 1024 * 1024, AMPLE, MAX_OBJ);
    assert!(
        restarted.get(&key("too-big")).await.is_none(),
        "an over-cap object is absent after a restart"
    );
}

/// The warm tier stays inside `S3CACHE_DISK_CACHE_BYTES`, FIFO-evicting to get there.
#[tokio::test]
async fn the_warm_tier_stays_inside_its_disk_budget() {
    const DISK_BYTES: u64 = 16 * 1024;
    const BODY: usize = 2048;
    const COUNT: usize = 40; // 80 KiB of bodies against a 16 KiB budget

    let dir = TempDir::new("budget");
    {
        let cache = cache(&dir, 1024 * 1024, DISK_BYTES, NO_CAP);
        for i in 0..COUNT {
            cache
                .insert(key(&format!("obj-{i}")), object(&format!("obj-{i}"), BODY))
                .await;
        }
        assert!(
            dir.bytes() <= DISK_BYTES,
            "on-disk bytes ({}) must stay inside the {DISK_BYTES}-byte budget",
            dir.bytes()
        );
        assert!(
            dir.files() < COUNT,
            "the budget must have evicted something"
        );
    }

    // Read back with a cold hot tier, so `get` reflects disk alone: the oldest key was
    // evicted to make room, the newest is still there.
    let restarted = cache(&dir, 1024 * 1024, DISK_BYTES, NO_CAP);
    assert!(
        restarted.get(&key("obj-0")).await.is_none(),
        "the earliest key was evicted to stay inside the budget"
    );
    let newest = format!("obj-{}", COUNT - 1);
    let obj = restarted
        .get(&key(&newest))
        .await
        .expect("the newest key is still on disk");
    assert_eq!(served_body(&obj).await, body(&newest, BODY));
}

/// A cached key never reaches the origin again: the second fetch is served locally, with
/// an origin future that fails the test if it is ever polled.
#[tokio::test]
async fn get_or_fetch_consults_the_origin_once_per_key() {
    let dir = TempDir::new("fetch");
    let cache = cache(&dir, 1024 * 1024, AMPLE, NO_CAP);
    let calls = Arc::new(AtomicU64::new(0));

    let fetched = cache
        .get_or_fetch(&key("fetched"), {
            let calls = Arc::clone(&calls);
            async move {
                calls.fetch_add(1, Ordering::Relaxed);
                Ok(object("fetched", 256))
            }
        })
        .await
        .expect("the first call fetches from the origin");
    assert_eq!(served_body(&fetched).await, body("fetched", 256));

    let served = cache
        .get_or_fetch(&key("fetched"), async {
            Err("origin must not be called".to_owned())
        })
        .await
        .expect("the second call is served from the cache");
    assert_eq!(served_body(&served).await, body("fetched", 256));
    assert_eq!(
        calls.load(Ordering::Relaxed),
        1,
        "one origin round-trip in total"
    );
}

/// Concurrent misses for one key share a single origin round-trip: the follower waits on
/// the fill gate and is served the leader's result rather than fetching its own.
#[tokio::test]
async fn concurrent_misses_share_one_origin_fetch() {
    let dir = TempDir::new("singleflight");
    let cache = cache(&dir, 1024 * 1024, AMPLE, NO_CAP);
    let calls = Arc::new(AtomicU64::new(0));

    let origin = || {
        let calls = Arc::clone(&calls);
        async move {
            calls.fetch_add(1, Ordering::Relaxed);
            // Long enough that the other caller is provably parked on the gate.
            tokio::time::sleep(Duration::from_millis(50)).await;
            Ok(object("shared", 256))
        }
    };
    let shared = key("shared");
    let (first, second) = tokio::join!(
        cache.get_or_fetch(&shared, origin()),
        cache.get_or_fetch(&shared, origin()),
    );

    assert_eq!(
        served_body(&first.expect("leader served")).await,
        body("shared", 256)
    );
    assert_eq!(
        served_body(&second.expect("follower served")).await,
        body("shared", 256)
    );
    assert_eq!(
        calls.load(Ordering::Relaxed),
        1,
        "the fetch was singleflighted"
    );
}
