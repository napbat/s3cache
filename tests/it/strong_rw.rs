//! `strong`'s read-after-acknowledged-write contract, checked adversarially.
//!
//! The contract (README, "Consistency"): once a PUT, DELETE or conditional PUT has
//! been acknowledged to its client through either node, every GET (whole or ranged),
//! HEAD and LIST *invoked after that acknowledgement*, through either node, reflects
//! that write or a newer one: never the old body, a stale 404, a LIST that misses the
//! key or one that still shows a deleted key. Where a node cannot prove its local
//! state current it must answer from the origin. A write whose response was an error
//! but which the origin applied is held to the same rule from the moment that error
//! reached its client: the proxy could not prove the key's state then, so nothing
//! local may contradict the origin afterwards (reported as `unacked`).
//!
//! Two production-wired nodes (strong leases, fleet bootstrap, a warm tier behind a
//! tiny hot tier, so bodies live on disk and must revalidate) gossip over a
//! [`FaultNet`] in front of a real `MinIO` whose responses the test can delay or
//! fault. A seeded workload writes through one node and reads through the other:
//! overwrites of one key through alternating nodes (the `SEQ`/`CURRENT` pattern),
//! an `If-Match` chain, `If-None-Match` slot creates with a contested duplicate,
//! deletes followed by GET and HEAD, and LIST after create or delete — while a
//! scenario breaks the fabric, stalls or restarts a pod, or delays and faults the
//! origin. Every answer is checked against the recorded history, and every
//! violation is reported with the fault timeline that produced it.

use crate::common;

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use bytes::Bytes;
use common::faultnet::{FaultNet, Link};
use common::fleet::{CAP, Pod, SERIAL_SCAN, fleet_config_bound, free_tcp_port, serves_locally};
use common::{Origin, WarmDir, body_blob, request};
use s3cache::cache::proxy::{CacheConfig, CachingProxy};
use s3cache::metrics::Metrics;
use s3cache::sync::coherence::{Consistency, DEFAULT_LEASE_MS, WriteSync};
use s3cache::tier::{buffer_body, open_warm};
use s3s::dto::{
    DeleteObjectInput, ETag, ETagCondition, GetObjectInput, HeadObjectInput, ListObjectsV2Input,
    PutObjectInput, Range,
};
use s3s::{S3, S3Error, S3ErrorCode};

/// A hot tier smaller than the workload's bodies together, so most reads find
/// their copy on the warm tier and have to prove it.
const HOT: u64 = 2 * 1024;
const WARM: u64 = 8 * 1024 * 1024;
/// How long a scenario's workload runs while its faults play.
const RUN: Duration = Duration::from_secs(14);
/// The bound on any wait for the pair to serve locally.
const SETTLE: Duration = Duration::from_secs(90);
/// The binary's drained-stop seal wait (`SEAL_WAIT` in `main.rs`).
const SEAL_WAIT: Duration = Duration::from_secs(5);
/// The lease duration every node runs, as the binary defaults it.
const LEASE: Duration = Duration::from_millis(DEFAULT_LEASE_MS);

const POINTERS: [&str; 2] = ["ptr/seq", "ptr/current"];
/// Keys written concurrently through both nodes: judged only by convergence.
const UNORDERED: &str = "race/";
const CAS: &str = "cas/epoch";

/// The bytes version `n` of `key` holds: the version first, so a ranged read
/// of the first eight bytes names it, and a length that varies with it.
fn body(key: &str, n: u64) -> Bytes {
    let mut text = format!("{n:08}|{key}|");
    text.extend(std::iter::repeat_n(
        'x',
        usize::try_from(n % 13).expect("small") * 7,
    ));
    Bytes::from(text)
}

fn version_of(bytes: &[u8]) -> Option<u64> {
    std::str::from_utf8(bytes.get(..8)?).ok()?.parse().ok()
}

/// What a key held after a write, or what a read saw.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Value {
    Absent,
    V(u64),
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Absent => f.write_str("absent"),
            Self::V(n) => write!(f, "v{n}"),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WriteStatus {
    /// The client got a success.
    Acked,
    /// The client got a 412.
    Refused,
    /// The client got another error, or its pod died under the request.
    Failed,
    /// The client hung up before an answer; the write may still be in flight.
    Abandoned,
}

#[derive(Clone, Debug)]
struct Write {
    key: String,
    value: Value,
    op: &'static str,
    via: usize,
    invoked: Instant,
    done: Instant,
    status: WriteStatus,
    /// Whether the origin holds this write's effect, read straight from the
    /// origin right after a non-success.
    applied: bool,
    etag: Option<String>,
    /// What a non-success said, for the report.
    error: Option<String>,
}

/// What one read reported about one key.
#[derive(Clone, Debug)]
enum Seen {
    /// A body (whole or the leading range) naming its version, or a 404.
    Value(Value),
    /// A HEAD: its `ETag`, the version its metadata names, and its length.
    Head {
        etag: Option<String>,
        meta: Option<u64>,
        length: Option<i64>,
    },
    /// A LIST row's `ETag` and size, or no row.
    Row(Option<(Option<String>, Option<i64>)>),
}

#[derive(Clone, Debug)]
struct Read {
    key: String,
    op: &'static str,
    via: usize,
    invoked: Instant,
    done: Instant,
    seen: Seen,
}

#[derive(Default)]
struct History {
    writes: Vec<Write>,
    reads: Vec<Read>,
}

/// One answer the contract forbids.
struct Violation {
    class: &'static str,
    text: String,
}

/// The two pods, the fabric between them, and what happened to them.
struct Cluster {
    label: &'static str,
    origin: Arc<Origin>,
    bucket: String,
    net: FaultNet,
    names: [&'static str; 2],
    binds: [u16; 2],
    warm: [WarmDir; 2],
    pods: Mutex<[Option<Pod>; 2]>,
    nodes: RwLock<[Option<NodeHandle>; 2]>,
    started: Instant,
    timeline: Mutex<Vec<(Duration, String)>>,
    history: Mutex<History>,
}

/// What a client needs to reach one node: its proxy and the runtime its requests
/// run on, as an inbound connection's task runs inside its pod.
#[derive(Clone)]
struct NodeHandle {
    proxy: CachingProxy,
    sync: Arc<WriteSync>,
    metrics: Arc<Metrics>,
    runtime: tokio::runtime::Handle,
}

impl Cluster {
    async fn new(label: &'static str, names: [&'static str; 2], seed: u64) -> Arc<Self> {
        let origin = Origin::start(label).await;
        let bucket = origin.bucket().to_owned();
        let cluster = Arc::new(Self {
            label,
            origin,
            bucket,
            net: FaultNet::new(seed),
            names,
            binds: [free_tcp_port(), free_tcp_port()],
            warm: [WarmDir::new(names[0]), WarmDir::new(names[1])],
            pods: Mutex::new([None, None]),
            nodes: RwLock::new([None, None]),
            started: Instant::now(),
            timeline: Mutex::new(Vec::new()),
            history: Mutex::new(History::default()),
        });
        cluster.boot(0).await;
        cluster.boot(1).await;
        cluster.await_local("cold start").await;
        cluster.settle().await;
        cluster
    }

    fn at(&self, instant: Instant) -> f64 {
        instant
            .saturating_duration_since(self.started)
            .as_secs_f64()
    }

    fn note(&self, event: impl Into<String>) {
        let event = event.into();
        self.timeline
            .lock()
            .unwrap()
            .push((self.started.elapsed(), event));
    }

    /// Start node `index` on a fresh pod over its persisted warm tier, as the
    /// binary starts: fleet bootstrap, strong lease, the binary's recovery.
    async fn boot(&self, index: usize) {
        let pod = Pod::new(self.names[index]);
        let metrics = Arc::new(Metrics::default());
        let name = self.names[index];
        let peer = self.names[1 - index];
        let book = [
            (self.names[0], self.binds[0]),
            (self.names[1], self.binds[1]),
        ];
        let fleet = fleet_config_bound(&self.origin, &self.bucket, name, self.binds[index], &book);
        let transport = self.net.endpoint(name);
        let warm = open_warm(self.warm[index].path(), WARM, CAP, Arc::clone(&metrics))
            .expect("the warm tier opens");
        let client = self.origin.counted_client();
        let bucket = self.bucket.clone();
        let node_metrics = Arc::clone(&metrics);
        let (sync, proxy) = pod
            .handle()
            .spawn(async move {
                let sync = Arc::new(WriteSync::over_transport(
                    transport,
                    name,
                    &[peer],
                    Consistency::Strong,
                    DEFAULT_LEASE_MS,
                ));
                let proxy = CachingProxy::new(
                    &client,
                    CacheConfig {
                        cache_bytes: HOT,
                        max_obj_bytes: CAP,
                    },
                    Some(warm),
                    Some(Arc::clone(&sync)),
                    node_metrics,
                )
                .with_index_scan(SERIAL_SCAN)
                .with_fleet_config(fleet);
                proxy
                    .start_fleet_coherence(std::slice::from_ref(&bucket))
                    .await;
                (sync, proxy)
            })
            .await
            .expect("node construction");
        let runtime = pod.handle();
        self.pods.lock().unwrap()[index] = Some(pod);
        self.nodes.write().unwrap()[index] = Some(NodeHandle {
            proxy,
            sync,
            metrics,
            runtime,
        });
        self.note(format!("{name} booted"));
    }

    fn node(&self, index: usize) -> Option<NodeHandle> {
        self.nodes.read().unwrap()[index].clone()
    }

    /// Kill node `index` where it stands.
    async fn crash(&self, index: usize) {
        self.nodes.write().unwrap()[index] = None;
        let pod = self.pods.lock().unwrap()[index].take();
        self.note(format!("{} crashed", self.names[index]));
        tokio::task::spawn_blocking(move || drop(pod))
            .await
            .expect("pod shutdown");
    }

    /// The binary's `SIGTERM` path: retract the lease, drain, seal the feed, stop.
    async fn planned_stop(&self, index: usize) {
        let Some(node) = self.node(index) else { return };
        self.note(format!("{} stopping (leave + seal)", self.names[index]));
        let (sync, proxy) = (Arc::clone(&node.sync), node.proxy.clone());
        let sealed = node
            .runtime
            .spawn(async move {
                sync.leave();
                proxy.seal_writes(SEAL_WAIT).await
            })
            .await
            .expect("planned stop");
        self.nodes.write().unwrap()[index] = None;
        let pod = self.pods.lock().unwrap()[index].take();
        self.note(format!("{} stopped; seal {sealed:?}", self.names[index]));
        tokio::task::spawn_blocking(move || drop(pod))
            .await
            .expect("pod shutdown");
    }

    /// Whether both nodes are up and serve locally.
    fn both_local(&self) -> bool {
        (0..2).all(|index| {
            self.node(index)
                .is_some_and(|node| serves_locally(&node.proxy, &self.bucket))
        })
    }

    async fn await_local(&self, what: &str) {
        let deadline = Instant::now() + SETTLE;
        while !self.both_local() {
            assert!(
                Instant::now() < deadline,
                "{}: both nodes serve locally after {what} within {SETTLE:?}",
                self.label
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        self.note(format!("both serve locally after {what}"));
    }

    /// Until each node's write is in the other's index by the time it returns —
    /// membership, leases and feeds have all converged.
    async fn settle(&self) {
        let deadline = Instant::now() + SETTLE;
        let mut probe = 0;
        loop {
            probe += 1;
            let mut seen = true;
            for writer in 0..2 {
                let key = format!("settle/{probe}-{writer}");
                let payload = body(&key, 0);
                let (put_key, bucket) = (key.clone(), self.bucket.clone());
                let wrote = self
                    .on(writer, move |proxy| async move {
                        put(&proxy, &bucket, &put_key, payload, None, None).await
                    })
                    .await;
                let (list_key, bucket) = (key.clone(), self.bucket.clone());
                let listed = self
                    .on(1 - writer, move |proxy| async move {
                        list(&proxy, &bucket, &list_key).await
                    })
                    .await;
                seen &= matches!(wrote, Some(Ok(_)))
                    && matches!(listed, Some(Ok(rows)) if rows.contains_key(&key));
            }
            if seen {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "{}: the pair converges within {SETTLE:?}",
                self.label
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        self.note("cluster settled");
    }

    /// Run `op` against node `via` on its own runtime. `None` when the node is down
    /// or died under the request.
    async fn on<T, F, Fut>(&self, via: usize, op: F) -> Option<T>
    where
        T: Send + 'static,
        F: FnOnce(CachingProxy) -> Fut,
        Fut: Future<Output = T> + Send + 'static,
    {
        let node = self.node(via)?;
        node.runtime.spawn(op(node.proxy.clone())).await.ok()
    }

    /// One write, recorded, with its effect read back from the origin when the
    /// client did not get a success.
    async fn write(&self, via: usize, key: &str, value: Value, cond: Cond) -> WriteStatus {
        self.write_within(via, key, value, cond, None).await
    }

    /// [`write`](Self::write), whose client hangs up after `give_up` — the request
    /// future is dropped, as a server drops it when its connection closes.
    async fn write_within(
        &self,
        via: usize,
        key: &str,
        value: Value,
        cond: Cond,
        give_up: Option<Duration>,
    ) -> WriteStatus {
        let invoked = Instant::now();
        let (bucket, owned) = (self.bucket.clone(), key.to_owned());
        let op = cond.label(value);
        let patience = give_up.unwrap_or(Duration::MAX);
        let outcome = match value {
            Value::V(n) => {
                let payload = body(key, n);
                let (if_match, if_none_match) = cond.conditions();
                self.on(via, move |proxy| async move {
                    tokio::time::timeout(
                        patience,
                        put(&proxy, &bucket, &owned, payload, if_match, if_none_match),
                    )
                    .await
                    .ok()
                })
                .await
            }
            Value::Absent => {
                self.on(via, move |proxy| async move {
                    tokio::time::timeout(patience, delete(&proxy, &bucket, &owned))
                        .await
                        .ok()
                        .map(|result| result.map(|()| None))
                })
                .await
            }
        };
        let done = Instant::now();
        let (status, etag, error) = match outcome {
            Some(Some(Ok(etag))) => (WriteStatus::Acked, etag, None),
            Some(Some(Err(error))) if error.code() == &S3ErrorCode::PreconditionFailed => {
                (WriteStatus::Refused, None, None)
            }
            Some(Some(Err(error))) => (
                WriteStatus::Failed,
                None,
                Some(format!(
                    "{:?}: {}",
                    error.code(),
                    error.message().unwrap_or("")
                )),
            ),
            Some(None) => (
                WriteStatus::Abandoned,
                None,
                Some("client gave up".to_owned()),
            ),
            None => (WriteStatus::Failed, None, Some("pod gone".to_owned())),
        };
        // An abandoned write may still be in flight: its effect is read only once the
        // run is quiet (see `settle_abandoned`).
        let (applied, etag) = match status {
            WriteStatus::Acked => (true, etag),
            WriteStatus::Abandoned => (false, None),
            WriteStatus::Refused | WriteStatus::Failed => {
                let (current, etag) = self.origin_state(key).await;
                (current == value, etag)
            }
        };
        self.history.lock().unwrap().writes.push(Write {
            key: key.to_owned(),
            value,
            op,
            via,
            invoked,
            done,
            status,
            applied,
            etag,
            error,
        });
        status
    }

    /// Resolve every abandoned write once nothing is in flight: the last write of its
    /// key applied iff the origin now holds its effect.
    async fn settle_abandoned(&self) {
        let abandoned: Vec<(usize, String, Value)> = {
            let history = self.history.lock().unwrap();
            history
                .writes
                .iter()
                .enumerate()
                .filter(|(_, write)| write.status == WriteStatus::Abandoned)
                .filter(|(index, write)| {
                    !history.writes[index + 1..]
                        .iter()
                        .any(|later| later.key == write.key)
                })
                .map(|(index, write)| (index, write.key.clone(), write.value))
                .collect()
        };
        for (index, key, value) in abandoned {
            let (current, etag) = self.origin_state(&key).await;
            let mut history = self.history.lock().unwrap();
            history.writes[index].applied = current == value;
            history.writes[index].etag = etag;
        }
    }

    /// What the origin holds for `key`, read around the proxy.
    async fn origin_state(&self, key: &str) -> (Value, Option<String>) {
        match self
            .origin
            .client()
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
        {
            Ok(out) => {
                let etag = out.e_tag().map(|tag| tag.trim_matches('"').to_owned());
                let bytes = out.body.collect().await.expect("origin body").into_bytes();
                (version_of(&bytes).map_or(Value::Absent, Value::V), etag)
            }
            Err(_) => (Value::Absent, None),
        }
    }

    async fn read(&self, via: usize, key: &str, kind: ReadKind) {
        let invoked = Instant::now();
        let (bucket, owned) = (self.bucket.clone(), key.to_owned());
        let seen = match kind {
            ReadKind::Get => self
                .on(via, move |proxy| async move {
                    get(&proxy, &bucket, &owned, None).await
                })
                .await
                .and_then(Result::ok)
                .map(Seen::Value),
            ReadKind::Range => self
                .on(via, move |proxy| async move {
                    get(&proxy, &bucket, &owned, Some((0, 7))).await
                })
                .await
                .and_then(Result::ok)
                .map(Seen::Value),
            ReadKind::Head => self
                .on(via, move |proxy| async move {
                    head(&proxy, &bucket, &owned).await
                })
                .await
                .and_then(Result::ok),
        };
        let done = Instant::now();
        if let Some(seen) = seen {
            self.history.lock().unwrap().reads.push(Read {
                key: key.to_owned(),
                op: kind.label(),
                via,
                invoked,
                done,
                seen,
            });
        }
    }

    /// A LIST of `prefix` through `via`: one row-or-absence per key the workload
    /// ever wrote under it.
    async fn list(&self, via: usize, prefix: &str) {
        let invoked = Instant::now();
        let (bucket, owned) = (self.bucket.clone(), prefix.to_owned());
        let rows = self
            .on(via, move |proxy| async move {
                list(&proxy, &bucket, &owned).await
            })
            .await
            .and_then(Result::ok);
        let done = Instant::now();
        let Some(rows) = rows else { return };
        let mut history = self.history.lock().unwrap();
        let mut keys: Vec<String> = history
            .writes
            .iter()
            .filter(|write| write.key.starts_with(prefix))
            .map(|write| write.key.clone())
            .collect();
        keys.extend(
            rows.keys()
                .filter(|key| !key.starts_with("settle/"))
                .cloned(),
        );
        keys.sort();
        keys.dedup();
        for key in keys {
            let row = rows.get(&key).cloned();
            history.reads.push(Read {
                key,
                op: "LIST",
                via,
                invoked,
                done,
                seen: Seen::Row(row),
            });
        }
    }

    /// The version `read` reported, named through the written `ETag`s; a HEAD whose
    /// `ETag`, metadata and length describe different versions is reported as `mixed`.
    fn observed(
        &self,
        read: &Read,
        etags: &BTreeMap<(String, String), u64>,
        lengths: &BTreeMap<(String, u64), i64>,
        found: &mut Vec<Violation>,
    ) -> Option<Value> {
        match &read.seen {
            Seen::Value(value) => Some(*value),
            Seen::Head { etag, meta, length } => {
                let by_etag = etag
                    .as_ref()
                    .and_then(|etag| etags.get(&(read.key.clone(), etag.clone())))
                    .copied();
                let consistent = by_etag.is_none_or(|n| {
                    meta.is_none_or(|meta| meta == n)
                        && *length == lengths.get(&(read.key.clone(), n)).copied()
                });
                if !consistent {
                    found.push(Violation {
                        class: "mixed",
                        text: format!(
                            "HEAD via {} of {} at +{:.3}s..+{:.3}s mixed versions: \
                             etag names v{}, metadata names {meta:?}, length {length:?}",
                            self.names[read.via],
                            read.key,
                            self.at(read.invoked),
                            self.at(read.done),
                            by_etag.unwrap_or_default(),
                        ),
                    });
                }
                by_etag.map(Value::V).or_else(|| meta.map(Value::V))
            }
            Seen::Row(None) => Some(Value::Absent),
            Seen::Row(Some((etag, _))) => etag
                .as_ref()
                .and_then(|etag| etags.get(&(read.key.clone(), etag.clone())))
                .map(|n| Value::V(*n)),
        }
    }

    /// Every check, rendered; empty when the history honours the contract.
    fn violations(&self) -> Vec<Violation> {
        let history = self.history.lock().unwrap();
        let mut etags: BTreeMap<(String, String), u64> = BTreeMap::new();
        let mut lengths: BTreeMap<(String, u64), i64> = BTreeMap::new();
        for write in &history.writes {
            if let (Value::V(n), Some(etag)) = (write.value, &write.etag)
                && write.applied
            {
                etags.insert((write.key.clone(), etag.clone()), n);
            }
            if let Value::V(n) = write.value {
                lengths.insert(
                    (write.key.clone(), n),
                    i64::try_from(body(&write.key, n).len()).expect("small"),
                );
            }
        }
        let mut found = Vec::new();
        for read in &history.reads {
            // Keys written concurrently through both nodes have no issue order to check
            // against; the final convergence check judges them (see `divergence`).
            if read.key.starts_with(UNORDERED) {
                continue;
            }
            let observed = self.observed(read, &etags, &lengths, &mut found);
            let writes: Vec<&Write> = history
                .writes
                .iter()
                .filter(|write| write.key == read.key)
                .collect();
            let Some(observed) = observed else {
                found.push(Violation {
                    class: "unknown",
                    text: format!(
                        "{} via {} of {} at +{:.3}s reported a version no write produced: {:?}",
                        read.op,
                        self.names[read.via],
                        read.key,
                        self.at(read.invoked),
                        read.seen
                    ),
                });
                continue;
            };
            // The newest effect that had reached its client before this read began. An
            // abandoned write's effect has no known time, so it never sets the floor.
            let floor = writes.iter().rposition(|write| {
                write.applied && write.status != WriteStatus::Abandoned && write.done < read.invoked
            });
            let allowed: Vec<usize> = writes
                .iter()
                .enumerate()
                .filter(|(index, write)| {
                    floor.is_none_or(|floor| *index >= floor)
                        && write.applied
                        && write.invoked < read.done
                })
                .map(|(index, _)| index)
                .collect();
            let initial_allowed = floor.is_none() && observed == Value::Absent;
            let explained =
                initial_allowed || allowed.iter().any(|index| writes[*index].value == observed);
            if explained {
                continue;
            }
            let mut text = format!(
                "{} via {} of {} at +{:.3}s..+{:.3}s returned {observed}",
                read.op,
                self.names[read.via],
                read.key,
                self.at(read.invoked),
                self.at(read.done),
            );
            let class = if let Some(floor) = floor {
                let write = writes[floor];
                let _ = write!(
                    text,
                    "; {} {} via {} (invoked +{:.3}s, {:?} at +{:.3}s) had already completed",
                    write.op,
                    write.value,
                    self.names[write.via],
                    self.at(write.invoked),
                    write.status,
                    self.at(write.done),
                );
                if write.status == WriteStatus::Acked {
                    "acked"
                } else {
                    "unacked"
                }
            } else {
                "phantom"
            };
            let options: Vec<String> = allowed
                .iter()
                .map(|index| writes[*index].value.to_string())
                .collect();
            let _ = write!(text, "; allowed {options:?}");
            found.push(Violation { class, text });
        }
        found
    }

    /// Once the run is quiet, every node must answer every key exactly as the origin
    /// does: a GET's version, a HEAD's `ETag`, and LIST's row.
    async fn divergence(&self, keys: &[String]) -> Vec<Violation> {
        let mut found = Vec::new();
        for key in keys {
            let (origin, origin_etag) = self.origin_state(key).await;
            for via in 0..2 {
                let (bucket, owned) = (self.bucket.clone(), key.clone());
                let answers = self
                    .on(via, move |proxy| async move {
                        (
                            get(&proxy, &bucket, &owned, None).await.ok(),
                            head(&proxy, &bucket, &owned).await.ok(),
                            list(&proxy, &bucket, &owned).await.ok(),
                        )
                    })
                    .await;
                let Some((got, headed, listed)) = answers else {
                    continue;
                };
                let headed = headed.map(|seen| match seen {
                    Seen::Head { etag, .. } => etag,
                    _ => None,
                });
                let listed = listed.map(|rows| rows.get(key).and_then(|(etag, _)| etag.clone()));
                let agrees = got == Some(origin)
                    && headed.as_ref() == Some(&origin_etag)
                    && listed.as_ref() == Some(&origin_etag);
                if !agrees {
                    found.push(Violation {
                        class: "diverged",
                        text: format!(
                            "{} answers {key} as GET {got:?}, HEAD etag {headed:?}, LIST etag \
                             {listed:?}; the origin holds {origin} ({origin_etag:?})",
                            self.names[via]
                        ),
                    });
                }
            }
        }
        found
    }

    fn report(&self, violations: &[Violation]) -> String {
        let history = self.history.lock().unwrap();
        let acked = history
            .writes
            .iter()
            .filter(|write| write.status == WriteStatus::Acked)
            .count();
        let mut text = format!(
            "{}: {} writes ({acked} acknowledged), {} read observations, {} violations\n",
            self.label,
            history.writes.len(),
            history.reads.len(),
            violations.len()
        );
        let mut classes: BTreeMap<&str, usize> = BTreeMap::new();
        for violation in violations {
            *classes.entry(violation.class).or_default() += 1;
        }
        let _ = writeln!(text, "by class: {classes:?}");
        let mut failures: BTreeMap<&str, usize> = BTreeMap::new();
        for write in &history.writes {
            if let Some(error) = &write.error {
                *failures.entry(error.as_str()).or_default() += 1;
            }
        }
        let _ = writeln!(text, "write failures: {failures:?}");
        for violation in violations.iter().take(40) {
            let _ = writeln!(text, "  [{}] {}", violation.class, violation.text);
        }
        let _ = writeln!(text, "timeline:");
        for (at, event) in self.timeline.lock().unwrap().iter() {
            let _ = writeln!(text, "  +{:.3}s {event}", at.as_secs_f64());
        }
        for index in 0..2 {
            if let Some(node) = self.node(index) {
                let _ = writeln!(
                    text,
                    "{} counters: {}",
                    self.names[index],
                    counters(&node.metrics)
                );
            }
        }
        text
    }
}

fn counters(metrics: &Metrics) -> String {
    let text = metrics.prometheus_text();
    [
        "ack_timeouts",
        "write_lease_lapses",
        "feed_gaps",
        "feed_renewals",
        "recovery_origin_scans",
        "recovery_fallbacks",
        "read_licence_bypasses",
        "read_absent_granter_bypasses",
        "read_freshness_bypasses",
        "body_revalidations",
        "body_revalidation_evictions",
        "get_hit",
        "head_index",
        "list_from_index",
    ]
    .iter()
    .filter_map(|name| {
        let prefix = format!("s3cache_{name} ");
        text.lines()
            .find_map(|line| line.strip_prefix(&prefix))
            .map(|value| format!("{name}={value}"))
    })
    .collect::<Vec<_>>()
    .join(" ")
}

#[derive(Clone, Copy)]
enum ReadKind {
    Get,
    Range,
    Head,
}

impl ReadKind {
    fn label(self) -> &'static str {
        match self {
            Self::Get => "GET",
            Self::Range => "GET range",
            Self::Head => "HEAD",
        }
    }
}

/// A write's precondition.
#[derive(Clone)]
enum Cond {
    None,
    IfMatch(String),
    IfNoneMatch,
}

impl Cond {
    fn conditions(&self) -> (Option<ETagCondition>, Option<ETagCondition>) {
        match self {
            Self::None => (None, None),
            Self::IfMatch(etag) => (Some(ETagCondition::ETag(ETag::Strong(etag.clone()))), None),
            Self::IfNoneMatch => (None, Some(ETagCondition::Any)),
        }
    }

    fn label(&self, value: Value) -> &'static str {
        match (self, value) {
            (_, Value::Absent) => "DELETE",
            (Self::None, _) => "PUT",
            (Self::IfMatch(_), _) => "PUT If-Match",
            (Self::IfNoneMatch, _) => "PUT If-None-Match",
        }
    }
}

async fn put(
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

async fn delete(proxy: &CachingProxy, bucket: &str, key: &str) -> Result<(), S3Error> {
    let input = DeleteObjectInput {
        bucket: bucket.to_owned(),
        key: key.to_owned(),
        ..Default::default()
    };
    proxy.delete_object(request(input)).await?;
    Ok(())
}

async fn get(
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

async fn head(proxy: &CachingProxy, bucket: &str, key: &str) -> Result<Seen, S3Error> {
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

type Rows = BTreeMap<String, (Option<String>, Option<i64>)>;

async fn list(proxy: &CachingProxy, bucket: &str, prefix: &str) -> Result<Rows, S3Error> {
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
