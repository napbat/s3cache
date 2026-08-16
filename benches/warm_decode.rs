//! Criterion-free warm-hit microbenchmark. Run with
//! `cargo bench --bench warm_decode`.

use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use s3cache::metrics::Metrics;
use s3cache::tier::{CachedObject, open_warm};
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
    let key = ("bench".to_owned(), "body".to_owned());
    let object = Arc::new(CachedObject::from_get(
        &GetObjectOutput {
            content_length: i64::try_from(BODY_BYTES).ok(),
            ..Default::default()
        },
        Bytes::from(vec![0x5a; BODY_BYTES]),
    ));
    warm.put(key.clone(), object).await.expect("seed warm tier");

    let start = Instant::now();
    for _ in 0..READS {
        let object = warm.get(&key).await.expect("read").expect("warm hit");
        std::hint::black_box(object);
    }
    let elapsed = start.elapsed();
    let seconds = elapsed.as_secs_f64();
    let reads = f64::from(u32::try_from(READS).expect("READS fits u32"));
    let body_bytes = f64::from(u32::try_from(BODY_BYTES).expect("BODY_BYTES fits u32"));
    let ops = reads / seconds;
    let logical_gib = (body_bytes * reads) / (1024.0 * 1024.0 * 1024.0);
    println!(
        "{READS} mmap-backed {BODY_BYTES}-byte warm decodes in {elapsed:.3?}: \
         {ops:.0} ops/s, {:.1} logical GiB/s without body copies; {:?}",
        logical_gib / seconds,
        disk.stats()
    );

    drop(warm);
    drop(disk);
    let _ = std::fs::remove_dir_all(dir);
}
