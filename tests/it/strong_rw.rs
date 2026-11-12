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

mod workload;
use workload::{delete, get, head, list, put};

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use bytes::Bytes;
use common::faultnet::FaultNet;
use common::fleet::{CAP, Pod, SERIAL_SCAN, fleet_config_bound, free_tcp_port, serves_locally};
use common::{Origin, WarmDir};
use s3cache::cache::proxy::{CacheConfig, CachingProxy};
use s3cache::metrics::Metrics;
use s3cache::sync::coherence::{Consistency, DEFAULT_LEASE_MS, WriteSync};
use s3cache::tier::open_warm;
use s3s::S3ErrorCode;
use s3s::dto::{ETag, ETagCondition};

/// A hot tier smaller than the workload's bodies together, so most reads find
/// their copy on the warm tier and have to prove it.
const HOT: u64 = 2 * 1024;
const WARM: u64 = 8 * 1024 * 1024;
/// How long a scenario's workload runs while its faults play.
const RUN: Duration = Duration::from_secs(14);
/// The bound on any wait for the pair to serve locally.
const SETTLE: Duration = Duration::from_secs(90);
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
        let proxy = pod
            .handle()
            .spawn(async move {
                let sync = Arc::new(
                    WriteSync::over_link(
                        groupnet::transport::link::BoundLink::new(
                            transport,
                            groupnet::transport::link::LinkConfig::new(vec![
                                groupnet::core::NodeId::new(peer),
                            ]),
                        ),
                        name,
                        &[peer],
                        Consistency::Strong,
                        DEFAULT_LEASE_MS,
                    )
                    .await
                    .expect("managed fault link starts"),
                );
                let proxy = CachingProxy::new(
                    &client,
                    CacheConfig {
                        cache_bytes: HOT,
                        max_obj_bytes: CAP,
                    },
                    Some(warm),
                    Some(sync),
                    node_metrics,
                )
                .with_index_scan(SERIAL_SCAN)
                .with_fleet_config(fleet);
                proxy
                    .start_fleet_coherence(std::slice::from_ref(&bucket))
                    .await;
                proxy
            })
            .await
            .expect("node construction");
        let runtime = pod.handle();
        self.pods.lock().unwrap()[index] = Some(pod);
        self.nodes.write().unwrap()[index] = Some(NodeHandle {
            proxy,
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

    /// The binary's `SIGTERM` path ([`CachingProxy::stop`]): retract the lease, close
    /// mutation admission, seal the feed, stop. No request is in flight to drain.
    async fn planned_stop(&self, index: usize) {
        let Some(node) = self.node(index) else { return };
        self.note(format!("{} stopping (leave + seal)", self.names[index]));
        let proxy = node.proxy.clone();
        let stopped = node
            .runtime
            .spawn(async move { proxy.stop(std::future::ready(())).await })
            .await
            .expect("planned stop");
        let sealed = stopped.sealed;
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
