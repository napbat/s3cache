//! Criterion-free warm-hit microbenchmark. Run with
//! `cargo bench --bench warm_decode`.

use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use s3cache::metrics::Metrics;
use s3cache::tier::{CacheKey, CachedObject, WarmTier, open_warm};
use s3s::dto::GetObjectOutput;
use tierstore::{TierRead, TierWrite};

const BODY_BYTES: usize = 16 * 1024 * 1024;
const READS: usize = 10_000;

fn main() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(run());
}

async fn run() {
    let dir =
        std::env::temp_dir().join(format!("s3cache-warm-decode-bench-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let metrics = Arc::new(Metrics::default());
    let (warm, disk) = open_warm(dir.clone(), 64 * 1024 * 1024, 32 * 1024 * 1024, metrics)
        .expect("open warm tier");
    let current_key = ("bench".to_owned(), "current".to_owned());
    warm.put(current_key.clone(), object())
        .await
        .expect("seed current warm record");

    let legacy_key = ("bench".to_owned(), "legacy".to_owned());
    let legacy_object = object();
    let legacy = Bytes::from(
        bincode::serialize(&(&legacy_key, legacy_object.as_ref())).expect("encode legacy record"),
    );
    disk.put(disk_key(&legacy_key), legacy)
        .await
        .expect("seed legacy warm record");

    measure(&warm, &current_key, "current").await;
    measure(&warm, &legacy_key, "legacy").await;
    println!("{:?}", disk.stats());

    drop(warm);
    drop(disk);
    let _ = std::fs::remove_dir_all(dir);
}

fn object() -> Arc<CachedObject> {
    Arc::new(CachedObject::from_get(
        &GetObjectOutput {
            content_length: i64::try_from(BODY_BYTES).ok(),
            ..Default::default()
        },
        Bytes::from(vec![0x5a; BODY_BYTES]),
    ))
}

fn disk_key(key: &CacheKey) -> String {
    let mut hash = blake3::Hasher::new();
    hash.update(key.0.as_bytes());
    hash.update(&[0]);
    hash.update(key.1.as_bytes());
    hash.finalize().to_hex().to_string()
}

async fn measure(warm: &WarmTier, key: &CacheKey, format: &str) {
    let start = Instant::now();
    for _ in 0..READS {
        let object = warm.get(key).await.expect("read").expect("warm hit");
        std::hint::black_box(object);
    }
    let elapsed = start.elapsed();
    let seconds = elapsed.as_secs_f64();
    let reads = f64::from(u32::try_from(READS).expect("READS fits u32"));
    let body_bytes = f64::from(u32::try_from(BODY_BYTES).expect("BODY_BYTES fits u32"));
    let ops = reads / seconds;
    let logical_gib = (body_bytes * reads) / (1024.0 * 1024.0 * 1024.0);
    println!(
        "{format}: {READS} mmap-backed {BODY_BYTES}-byte warm decodes in {elapsed:.3?}: \
         {ops:.0} ops/s, {:.1} logical GiB/s without body copies",
        logical_gib / seconds
    );
}
