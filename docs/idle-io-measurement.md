# Idle and post-rollout measurements

## Controlled local origin

Run `cargo test --locked --test idle-origin -j 2 -- --ignored --nocapture`
after building the pinned MinIO image. This test runs locally. CI does not
run the MinIO tests.

The fixture used two proxies in strong mode with two-second leases. Each
proxy had a 2 GiB hot-cache limit and a 32 MiB object-size limit. The disk
tier was disabled. MinIO held 2,048 seeded object prefixes. Seed writes
bypassed the origin counter.

After index warmup and a five-second settle period, three consecutive
20-second windows each served 640 bounded LIST calls on each proxy.
All 3,840 LIST calls used the local index. The counted origin received zero
LIST, GET, HEAD, PUT, COPY, DELETE, or other requests. No feed gap, read
licence bypass, or freshness timeout occurred in those windows.

This measures the cache with an inventory workload. It does not measure
Docres writer leases or prove that every production idle interval costs zero.

## Live deployment on 2026-09-28 UTC

Both proxies ran `14062d036ca80a34ccf640aa2f3aa90a486084fb`. Docres still
ran `3ab5356ddaf9192bcb5538eaa43a1755d77202d2`. The new Docres engine was
not deployed during this sample.

Pod 0 completed its directory at 00:41:37 UTC with 758,745 listed keys.
Pod 1 completed at 00:43:48 UTC with 760,408 listed keys. Pod 1 had a feed
gap during startup that caused another scan. Its first scan started before
pod 0 joined. The chart's TCP readiness check admitted traffic before
these scans completed.

The following deltas cover 231.776 seconds after both indexes completed.
Neither pod was replaced during the interval. Docres ingested 11,699
documents in the same interval. This was an active workload, not idle.

| Counter | Pod 0 | Pod 1 | Total |
| --- | ---: | ---: | ---: |
| LIST from index | 449 | 3,497 | 3,946 |
| LIST passthrough | 0 | 59 | 59 |
| Whole GET hit | 73 | 4,548 | 4,621 |
| Whole GET miss | 42 | 793 | 835 |
| GET bypass | 4 | 282 | 286 |
| HEAD miss | 53 | 142 | 195 |
| Range hit | 153 | 4,799 | 4,952 |
| Range promotion | 76 | 590 | 666 |
| Body revalidation success | 413 | 6,915 | 7,328 |
| Body rejected: timestamp | 3 | 721 | 724 |
| Body rejected: ETag | 8 | 67 | 75 |
| Body rejected: absent key | 0 | 1 | 1 |
| Body rejected: missing identity | 0 | 0 | 0 |
| Read licence bypass | 0 | 128 | 128 |
| Read freshness timeout | 0 | 0 | 0 |
| Feed gap | 0 | 0 | 0 |
| Full resync after lease lapse | 0 | 0 | 0 |

The proxies answered 98.5% of these LIST calls locally. The timestamp
counter prompted a check of read observations: the old path inserted an
unknown key with the observation time instead of its origin modification
time. This can reject an unchanged disk-cached body. The counters alone
do not prove that every timestamp rejection used that path.

The raw live result is `/tmp/s3cache-steady-result.json` on the cluster host.
The before and after metric snapshots are `/tmp/s3cache-steady-before.json`
and `/tmp/s3cache-steady-after.json` on that host.
