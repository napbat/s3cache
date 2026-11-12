use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::common::faultnet::Link;
use crate::common::{body_blob, request};
use bytes::Bytes;
use s3cache::cache::proxy::CachingProxy;
use s3cache::sync::coherence::DEFAULT_LEASE_MS;
use s3cache::tier::buffer_body;
use s3s::dto::{
    DeleteObjectInput, ETag, ETagCondition, GetObjectInput, HeadObjectInput, ListObjectsV2Input,
    PutObjectInput, Range,
};
use s3s::{S3, S3Error, S3ErrorCode};

use super::{
    CAS, Cluster, Cond, LEASE, POINTERS, RUN, Read, ReadKind, Seen, Value, WriteStatus, version_of,
};

pub(super) async fn put(
    proxy: &CachingProxy,
    bucket: &str,
    key: &str,
    payload: Bytes,
    if_match: Option<ETagCondition>,
    if_none_match: Option<ETagCondition>,
) -> Result<Option<String>, S3Error> {
    let version = version_of(&payload).unwrap_or_default();
    // Every fourth version names no Content-Type: the proxy indexes it skeletally
    // and keeps no copy, so its first HEAD and GET are the origin's to answer.
    let content_type =
        (!version.is_multiple_of(4)).then(|| "application/x-s3cache-check".to_owned());
    let input = PutObjectInput {
        bucket: bucket.to_owned(),
        key: key.to_owned(),
        content_length: Some(i64::try_from(payload.len()).expect("small")),
        body: Some(body_blob(payload)),
        content_type,
        metadata: Some([("version".to_owned(), version.to_string())].into()),
        if_match,
        if_none_match,
        ..Default::default()
    };
    let out = proxy.put_object(request(input)).await?.output;
    Ok(out.e_tag.map(ETag::into_value))
}

pub(super) async fn delete(proxy: &CachingProxy, bucket: &str, key: &str) -> Result<(), S3Error> {
    let input = DeleteObjectInput {
        bucket: bucket.to_owned(),
        key: key.to_owned(),
        ..Default::default()
    };
    proxy.delete_object(request(input)).await?;
    Ok(())
}

pub(super) async fn get(
    proxy: &CachingProxy,
    bucket: &str,
    key: &str,
    range: Option<(u64, u64)>,
) -> Result<Value, S3Error> {
    let input = GetObjectInput {
        bucket: bucket.to_owned(),
        key: key.to_owned(),
        range: range.map(|(first, last)| Range::Int {
            first,
            last: Some(last),
        }),
        ..Default::default()
    };
    match proxy.get_object(request(input)).await {
        Ok(mut out) => {
            let blob = out.output.body.take().expect("a GET carries a body");
            let bytes = buffer_body(blob, usize::MAX).await.expect("readable body");
            Ok(version_of(&bytes).map_or(Value::Absent, Value::V))
        }
        Err(error) if error.code() == &S3ErrorCode::NoSuchKey => Ok(Value::Absent),
        Err(error) => Err(error),
    }
}

pub(super) async fn head(proxy: &CachingProxy, bucket: &str, key: &str) -> Result<Seen, S3Error> {
    let input = HeadObjectInput {
        bucket: bucket.to_owned(),
        key: key.to_owned(),
        ..Default::default()
    };
    match proxy.head_object(request(input)).await {
        Ok(out) => {
            let out = out.output;
            Ok(Seen::Head {
                etag: out.e_tag.map(ETag::into_value),
                meta: out
                    .metadata
                    .as_ref()
                    .and_then(|meta| meta.get("version"))
                    .and_then(|version| version.parse().ok()),
                length: out.content_length,
            })
        }
        Err(error)
            if error.code() == &S3ErrorCode::NoSuchKey
                || error.status_code() == Some(http::StatusCode::NOT_FOUND) =>
        {
            Ok(Seen::Value(Value::Absent))
        }
        Err(error) => Err(error),
    }
}

pub(super) type Rows = BTreeMap<String, (Option<String>, Option<i64>)>;

pub(super) async fn list(
    proxy: &CachingProxy,
    bucket: &str,
    prefix: &str,
) -> Result<Rows, S3Error> {
    let out = proxy
        .list_objects_v2(request(ListObjectsV2Input {
            bucket: bucket.to_owned(),
            prefix: Some(prefix.to_owned()),
            ..Default::default()
        }))
        .await?
        .output;
    Ok(out
        .contents
        .into_iter()
        .flatten()
        .filter_map(|object| {
            Some((
                object.key?,
                (object.e_tag.map(ETag::into_value), object.size),
            ))
        })
        .collect())
}

/// A seeded splitmix64 stream per workload task.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound
    }

    fn node(&mut self) -> usize {
        usize::from(self.next() % 2 == 1)
    }
}

/// The other node, unless it is down: then the same one.
fn other(cluster: &Cluster, via: usize) -> usize {
    if cluster.node(1 - via).is_some() {
        1 - via
    } else {
        via
    }
}

/// A node that is up, preferring `via`.
fn up(cluster: &Cluster, via: usize) -> usize {
    if cluster.node(via).is_some() {
        via
    } else {
        1 - via
    }
}

fn read_kind(rng: &mut Rng) -> ReadKind {
    match rng.below(3) {
        0 => ReadKind::Get,
        1 => ReadKind::Range,
        _ => ReadKind::Head,
    }
}

/// Overwrites of the pointer keys through alternating nodes, each followed by a
/// read through the other node.
async fn pointer_writer(cluster: Arc<Cluster>, seed: u64, until: Instant) {
    let mut rng = Rng(seed);
    let mut version = 0;
    while Instant::now() < until {
        version += 1;
        let key = POINTERS[usize::try_from(version % 2).expect("small")];
        let via = up(&cluster, rng.node());
        if cluster.write(via, key, Value::V(version), Cond::None).await == WriteStatus::Acked {
            let reader = other(&cluster, via);
            cluster.read(reader, key, read_kind(&mut rng)).await;
            if rng.below(4) == 0 {
                cluster.list(reader, "ptr/").await;
            }
        }
    }
}

/// An `If-Match` chain: HEAD through one node, conditional overwrite through the
/// other, the way a log writer advances its pointer.
async fn cas_writer(cluster: Arc<Cluster>, seed: u64, until: Instant) {
    let mut rng = Rng(seed);
    let mut version = 1_000_000;
    while Instant::now() < until {
        let reader = up(&cluster, rng.node());
        let (bucket, key) = (cluster.bucket.clone(), CAS.to_owned());
        let invoked = Instant::now();
        let answer = cluster
            .on(reader, move |proxy| async move {
                head(&proxy, &bucket, &key).await
            })
            .await
            .and_then(Result::ok);
        let done = Instant::now();
        let Some(current) = answer else { continue };
        cluster.history.lock().unwrap().reads.push(Read {
            key: CAS.to_owned(),
            op: "HEAD (CAS)",
            via: reader,
            invoked,
            done,
            seen: current.clone(),
        });
        version += 1;
        let cond = match current {
            Seen::Head {
                etag: Some(etag), ..
            } => Cond::IfMatch(etag),
            _ => Cond::IfNoneMatch,
        };
        let writer = other(&cluster, reader);
        cluster.write(writer, CAS, Value::V(version), cond).await;
    }
}

/// Log slots: create once (a contested duplicate through the other node must
/// lose), read and list through the other node, truncate old slots behind.
async fn slot_writer(cluster: Arc<Cluster>, seed: u64, until: Instant) {
    let mut rng = Rng(seed);
    let mut slot = 0_u64;
    let mut version = 2_000_000;
    while Instant::now() < until {
        slot += 1;
        let key = format!("log/{slot:04}");
        let via = up(&cluster, rng.node());
        version += 1;
        if cluster
            .write(via, &key, Value::V(version), Cond::IfNoneMatch)
            .await
            != WriteStatus::Acked
        {
            continue;
        }
        let reader = other(&cluster, via);
        cluster.read(reader, &key, read_kind(&mut rng)).await;
        version += 1;
        cluster
            .write(reader, &key, Value::V(version), Cond::IfNoneMatch)
            .await;
        cluster.list(reader, "log/").await;
        if slot > 3 {
            let old = format!("log/{:04}", slot - 3);
            let deleter = up(&cluster, rng.node());
            if cluster
                .write(deleter, &old, Value::Absent, Cond::None)
                .await
                == WriteStatus::Acked
            {
                let reader = other(&cluster, deleter);
                cluster.read(reader, &old, read_kind(&mut rng)).await;
                cluster.list(reader, "log/").await;
            }
        }
    }
}

/// Reads of every key through either node, at random.
async fn random_reader(cluster: Arc<Cluster>, seed: u64, until: Instant) {
    let mut rng = Rng(seed);
    while Instant::now() < until {
        let via = up(&cluster, rng.node());
        let pick = rng.below(6);
        if pick == 5 {
            let prefix = ["ptr/", "cas/", "log/"][usize::try_from(rng.below(3)).expect("small")];
            cluster.list(via, prefix).await;
            continue;
        }
        let key = match pick {
            0 | 1 => POINTERS[usize::try_from(pick).expect("small")].to_owned(),
            2 => CAS.to_owned(),
            _ => {
                let slots = cluster
                    .history
                    .lock()
                    .unwrap()
                    .writes
                    .iter()
                    .filter(|write| write.key.starts_with("log/"))
                    .map(|write| write.key.clone())
                    .next_back();
                let Some(last) = slots else { continue };
                last
            }
        };
        cluster.read(via, &key, read_kind(&mut rng)).await;
        tokio::time::sleep(Duration::from_millis(rng.below(15))).await;
    }
}

/// Run the workload for [`RUN`] while `faults` plays (it is handed the workload's
/// deadline, and may run workload of its own), heal, let the pair serve locally again,
/// read every key through both nodes, and assert the history honours the contract and
/// both nodes agree with the origin.
async fn scenario<F, Fut>(label: &'static str, names: [&'static str; 2], seed: u64, faults: F)
where
    F: FnOnce(Arc<Cluster>, Instant) -> Fut,
    Fut: Future<Output = ()> + Send + 'static,
{
    // `RUST_LOG` opts a run into the nodes' decisions, each under its pod's thread.
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "off".into()),
        )
        .with_thread_names(true)
        .with_test_writer()
        .try_init();
    let cluster = Cluster::new(label, names, seed).await;
    cluster.note("workload starts");
    let until = Instant::now() + RUN;
    let tasks = vec![
        tokio::spawn(pointer_writer(Arc::clone(&cluster), seed ^ 1, until)),
        tokio::spawn(cas_writer(Arc::clone(&cluster), seed ^ 2, until)),
        tokio::spawn(slot_writer(Arc::clone(&cluster), seed ^ 3, until)),
        tokio::spawn(random_reader(Arc::clone(&cluster), seed ^ 4, until)),
        tokio::spawn(random_reader(Arc::clone(&cluster), seed ^ 5, until)),
        tokio::spawn(faults(Arc::clone(&cluster), until)),
    ];
    for task in tasks {
        task.await.expect("workload task");
    }
    cluster.net.heal();
    cluster.origin.jitter_object_responses(Duration::ZERO);
    cluster.origin.fail_applied_writes(0);
    cluster.note("workload ends; faults healed");
    for index in 0..2 {
        if cluster.node(index).is_none() {
            cluster.boot(index).await;
        }
    }
    cluster.await_local("the run").await;
    cluster.settle_abandoned().await;
    let keys: Vec<String> = {
        let history = cluster.history.lock().unwrap();
        let mut keys: Vec<String> = history
            .writes
            .iter()
            .map(|write| write.key.clone())
            .collect();
        keys.sort();
        keys.dedup();
        keys
    };
    let mut prefixes: Vec<String> = keys
        .iter()
        .filter_map(|key| key.split_once('/').map(|(head, _)| format!("{head}/")))
        .collect();
    prefixes.dedup();
    for via in 0..2 {
        for key in &keys {
            for kind in [ReadKind::Get, ReadKind::Range, ReadKind::Head] {
                cluster.read(via, key, kind).await;
            }
        }
        for prefix in &prefixes {
            cluster.list(via, prefix).await;
        }
    }
    let mut violations = cluster.violations();
    violations.extend(cluster.divergence(&keys).await);
    let report = cluster.report(&violations);
    println!("{report}");
    assert!(violations.is_empty(), "{report}");
}

/// A cut link for longer than the lease: the writer's wait ends on a lapse.
fn cut() -> Link {
    Link {
        cut: true,
        ..Link::default()
    }
}

/// How long a cut must last for each side to reap the other and then keep serving
/// past a stalled write's whole wait: the binary tunes `dead_timeout_ms` to the lease
/// `D` and reaps a member `2 × dead_timeout_ms` past its `Dead` verdict, which follows
/// the silence by a detection window plus the suspect timeout (well inside one more
/// `D`), and a write waits at most `D` plus a second of slack.
const PAST_THE_REAP_HORIZON: Duration = Duration::from_millis(5 * DEFAULT_LEASE_MS);

/// Steady state, with origin answers arriving out of order.
#[tokio::test(flavor = "multi_thread")]
async fn steady_state_with_origin_jitter() {
    scenario(
        "rw-steady",
        ["rw-steady-a", "rw-steady-b"],
        11,
        |cluster, _| async move {
            cluster
                .origin
                .jitter_object_responses(Duration::from_millis(40));
            cluster.note("origin jitter 0-40ms");
        },
    )
    .await;
}

/// Gossip delayed and lossy in both directions, below the lease.
#[tokio::test(flavor = "multi_thread")]
async fn feed_delay_and_gossip_loss() {
    scenario(
        "rw-lossy",
        ["rw-lossy-a", "rw-lossy-b"],
        12,
        |cluster, _| async move {
            let [a, b] = cluster.names;
            cluster.net.both(
                a,
                b,
                Link {
                    delay: Duration::from_millis(150),
                    loss_per_mille: 300,
                    ..Link::default()
                },
            );
            cluster.note("both directions: 150ms delay, 30% loss");
            cluster
                .origin
                .jitter_object_responses(Duration::from_millis(20));
        },
    )
    .await;
}

/// Full partitions longer than the lease, then heals: writes complete on lapses,
/// and the lapsed side recovers.
#[tokio::test(flavor = "multi_thread")]
async fn partitions_longer_than_the_lease() {
    scenario(
        "rw-part",
        ["rw-part-a", "rw-part-b"],
        13,
        |cluster, _| async move {
            let [a, b] = cluster.names;
            for _ in 0..2 {
                tokio::time::sleep(Duration::from_secs(2)).await;
                cluster.net.both(a, b, cut());
                cluster.note("partition a<->b");
                tokio::time::sleep(LEASE + Duration::from_millis(1500)).await;
                cluster.net.heal();
                cluster.note("heal");
            }
        },
    )
    .await;
}

/// One direction cut at a time, longer than the lease.
#[tokio::test(flavor = "multi_thread")]
async fn asymmetric_partitions() {
    scenario(
        "rw-asym",
        ["rw-asym-a", "rw-asym-b"],
        14,
        |cluster, _| async move {
            let [a, b] = cluster.names;
            tokio::time::sleep(Duration::from_secs(2)).await;
            cluster.net.set(a, b, cut());
            cluster.note("cut a->b");
            tokio::time::sleep(LEASE + Duration::from_millis(1500)).await;
            cluster.net.heal();
            cluster.note("heal");
            tokio::time::sleep(Duration::from_secs(2)).await;
            cluster.net.set(b, a, cut());
            cluster.note("cut b->a");
            tokio::time::sleep(LEASE + Duration::from_millis(1500)).await;
            cluster.net.heal();
            cluster.note("heal");
        },
    )
    .await;
}

/// One direction cut for longer than the reap horizon: the reader that hears nothing
/// from the writer stops counting it as a granter while the writer still counts the
/// reader — the asymmetric partition behind a stalled acknowledgement.
#[tokio::test(flavor = "multi_thread")]
async fn an_asymmetric_partition_past_the_reap_horizon() {
    scenario(
        "rw-asym-reap",
        ["rw-asym-reap-a", "rw-asym-reap-b"],
        21,
        |cluster, _| async move {
            let [a, b] = cluster.names;
            tokio::time::sleep(Duration::from_secs(1)).await;
            cluster.net.set(a, b, cut());
            cluster.note("cut a->b past the reap horizon");
            tokio::time::sleep(PAST_THE_REAP_HORIZON).await;
            cluster.net.heal();
            cluster.note("heal");
        },
    )
    .await;
}

/// A pod whose only worker stalls past the lease: its lease lapses under writes.
#[tokio::test(flavor = "multi_thread")]
async fn a_stalled_pod_lapses() {
    scenario(
        "rw-stall",
        ["rw-stall-a", "rw-stall-b"],
        15,
        |cluster, _| async move {
            for index in [1, 0] {
                tokio::time::sleep(Duration::from_secs(3)).await;
                if let Some(pod) = cluster.pods.lock().unwrap()[index].as_ref() {
                    pod.stall(LEASE + Duration::from_millis(800));
                }
                cluster.note(format!("{} stalls", cluster.names[index]));
            }
        },
    )
    .await;
}

/// A pod crashes mid-workload and comes back on its warm tier.
#[tokio::test(flavor = "multi_thread")]
async fn a_crashed_pod_restarts_on_its_warm_tier() {
    scenario(
        "rw-crash",
        ["rw-crash-a", "rw-crash-b"],
        16,
        |cluster, _| async move {
            tokio::time::sleep(Duration::from_secs(3)).await;
            cluster.crash(1).await;
            tokio::time::sleep(Duration::from_secs(3)).await;
            cluster.boot(1).await;
        },
    )
    .await;
}

/// A rolling update: each pod in turn takes the binary's planned stop and
/// restarts on its warm tier while the other serves.
#[tokio::test(flavor = "multi_thread")]
async fn a_rolling_update() {
    scenario(
        "rw-roll",
        ["rw-roll-a", "rw-roll-b"],
        17,
        |cluster, _| async move {
            for index in [1, 0] {
                tokio::time::sleep(Duration::from_secs(1)).await;
                cluster.planned_stop(index).await;
                tokio::time::sleep(Duration::from_secs(1)).await;
                cluster.boot(index).await;
            }
        },
    )
    .await;
}

/// The origin commits some writes and then fails their responses.
#[tokio::test(flavor = "multi_thread")]
async fn ambiguous_write_outcomes() {
    scenario(
        "rw-ambig",
        ["rw-ambig-a", "rw-ambig-b"],
        18,
        |cluster, _| async move {
            cluster.origin.fail_applied_writes(150);
            cluster
                .origin
                .jitter_object_responses(Duration::from_millis(20));
            cluster.note("15% of applied writes answered 500; jitter 0-20ms");
        },
    )
    .await;
}

/// Clients that hang up mid-write while the origin answers late: an abandoned DELETE
/// or PUT still lands in every node's view of the key.
#[tokio::test(flavor = "multi_thread")]
async fn clients_that_hang_up_mid_write() {
    scenario(
        "rw-hangup",
        ["rw-hangup-a", "rw-hangup-b"],
        19,
        |cluster, until| async move {
            cluster
                .origin
                .jitter_object_responses(Duration::from_millis(60));
            cluster.note("origin jitter 0-60ms; clients give up after 0-40ms");
            let mut rng = Rng(19);
            let mut n = 3_000_000;
            while Instant::now() < until {
                n += 1;
                let key = format!("cancel/{n}");
                let via = up(&cluster, rng.node());
                if cluster.write(via, &key, Value::V(n), Cond::None).await != WriteStatus::Acked {
                    continue;
                }
                let patience = Duration::from_millis(rng.below(40));
                let other = other(&cluster, via);
                cluster
                    .write_within(other, &key, Value::Absent, Cond::None, Some(patience))
                    .await;
            }
        },
    )
    .await;
}

/// Two clients overwrite one key through different nodes at once: the origin
/// decides the order, and both nodes must end up answering what it kept.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_overwrites_through_both_nodes() {
    scenario(
        "rw-race",
        ["rw-race-a", "rw-race-b"],
        20,
        |cluster, until| async move {
            cluster
                .origin
                .jitter_object_responses(Duration::from_millis(30));
            let racers = (0..2).map(|via| {
                let cluster = Arc::clone(&cluster);
                tokio::spawn(async move {
                    let mut n = 4_000_000 + via as u64 * 1_000_000;
                    while Instant::now() < until {
                        n += 1;
                        cluster
                            .write(up(&cluster, via), "race/k", Value::V(n), Cond::None)
                            .await;
                    }
                })
            });
            for racer in racers.collect::<Vec<_>>() {
                racer.await.expect("racer");
            }
        },
    )
    .await;
}
