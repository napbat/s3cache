//! The production fleet scenarios, sized and paced like production: two nodes
//! gossiping on the in-memory transport, each on a one-worker runtime of its
//! own with the binary's recovery, claim and lease configuration, over an
//! origin whose bucket lists 790,000 synthetic rows at production's page pace
//! (objects written or deleted through the proxy are merged into that
//! listing). Every scenario records its timings.
//!
//! * a cold start makes one origin scan, and the follower lists nothing;
//! * a planned stop, by the binary's own `SIGTERM` path, seals the restarting
//!   node's feed: the survivor keeps its index with no gap and no scan, and the
//!   rejoiner installs the survivor's image without one origin LIST;
//! * a crash leaves the dead life's tail unknown: the pair makes exactly one
//!   scan, and the other node follows it;
//! * a follower serves HEAD, GET, PUT and DELETE traffic while it bootstraps,
//!   and installs its donor's image without one origin LIST;
//! * `StatefulSet` rolling updates ([`rolling`]) restart both pods in turn,
//!   the first update and every later one, whose donors hold images they
//!   installed from a peer: no restart makes an origin scan or falls back.

use crate::common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use common::fleet::{Counts, Pod, free_tcp_port, mem_pod_node, serves_locally, trace_decisions};
use common::{Origin, counter, delete, get, head, put, request};
use groupnet::transport::mem::Network;
use s3cache::cache::proxy::CachingProxy;
use s3cache::cache::stop::Stopped;
use s3cache::metrics::Metrics;
use s3cache::sync::coherence::WriteSync;
use s3cache::sync::stop::SealOutcome;
use s3s::S3;
use s3s::dto::ListObjectsV2Input;
use tokio::task::JoinHandle;

mod rolling;

/// About the production bucket's 793,000 rows.
const ROWS: usize = 790_000;
/// Production lists about 1.5 pages a second; one scan runs over three minutes.
const PAGE: Duration = Duration::from_millis(250);
/// Room for two paced scans and a transfer.
const DEADLINE: Duration = Duration::from_mins(20);
/// Writes the restarting node makes before it stops.
const WRITTEN: usize = 4;

/// A key the restarting node writes. It sorts after every synthetic row.
fn written(n: usize) -> String {
    format!("zz-rejoin-{n}")
}

/// A real object a serving follower reads, overwrites or deletes.
fn served(n: usize) -> String {
    format!("zz-served-{n}")
}

/// One node on its own pod, and what the test holds of it.
struct Node {
    pod: Pod,
    sync: Arc<WriteSync>,
    proxy: CachingProxy,
    metrics: Arc<Metrics>,
}

impl Node {
    async fn build(
        net: &Network,
        origin: &Arc<Origin>,
        (name, bind): (&'static str, u16),
        peer: &'static str,
        book: [(&'static str, u16); 2],
    ) -> Self {
        let pod = Pod::new(name);
        let metrics = Arc::new(Metrics::default());
        let (sync, proxy) =
            mem_pod_node(&pod, net, origin, (name, bind), peer, book, &metrics).await;
        Self {
            pod,
            sync,
            proxy,
            metrics,
        }
    }

    /// Start fleet coherence inside the node's pod, as the binary does.
    async fn start(&self, bucket: &str) {
        let (node, bucket) = (self.proxy.clone(), bucket.to_owned());
        self.pod
            .handle()
            .spawn(async move {
                node.start_fleet_coherence(std::slice::from_ref(&bucket))
                    .await;
            })
            .await
            .expect("fleet coherence started");
    }

    fn count(&self, name: &str) -> u64 {
        counter(&self.metrics, name)
    }

    fn serves(&self, bucket: &str) -> bool {
        serves_locally(&self.proxy, bucket)
    }

    fn life(&self) -> Life {
        Life {
            scans: self.count("recovery_origin_scans"),
            fallbacks: self.count("recovery_fallbacks"),
        }
    }
}

/// What a node's recovery has cost so far.
#[derive(Clone, Copy, Debug)]
struct Life {
    scans: u64,
    fallbacks: u64,
}

impl Life {
    fn since(self, before: Self) -> Self {
        Self {
            scans: self.scans - before.scans,
            fallbacks: self.fallbacks - before.fallbacks,
        }
    }
}

/// The pair's names, bulk ports and fleet book.
struct Fleet {
    net: Network,
    origin: Arc<Origin>,
    bucket: String,
    names: [&'static str; 2],
    binds: [u16; 2],
}

impl Fleet {
    async fn new(label: &str, names: [&'static str; 2]) -> Self {
        trace_decisions();
        let origin = Origin::start(label).await;
        origin.serve_synthetic_listing(ROWS);
        origin.delay_lists(PAGE);
        let bucket = origin.bucket().to_owned();
        Self {
            net: Network::new(),
            origin,
            bucket,
            names,
            binds: [free_tcp_port(), free_tcp_port()],
        }
    }

    fn book(&self) -> [(&'static str, u16); 2] {
        [
            (self.names[0], self.binds[0]),
            (self.names[1], self.binds[1]),
        ]
    }

    async fn node(&self, index: usize) -> Node {
        Node::build(
            &self.net,
            &self.origin,
            (self.names[index], self.binds[index]),
            self.names[1 - index],
            self.book(),
        )
        .await
    }

    fn counts(&self) -> Counts {
        Counts::take(&self.origin)
    }
}

async fn wait_until(what: &str, mut done: impl FnMut() -> bool) -> Duration {
    let started = Instant::now();
    tokio::time::timeout(DEADLINE, async {
        while !done() {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{what} within {DEADLINE:?}"));
    started.elapsed()
}

async fn mutual_alive(nodes: [&Node; 2], names: [&str; 2]) {
    wait_until("the pair sees each other alive", || {
        nodes[0].sync.peer_alive(names[1]) && nodes[1].sync.peer_alive(names[0])
    })
    .await;
}

/// Whether a LIST through `proxy` reports `key`, asked by exact prefix so a
/// node serving locally answers from its index.
async fn lists(proxy: &CachingProxy, bucket: &str, key: &str) -> bool {
    proxy
        .list_objects_v2(request(ListObjectsV2Input {
            bucket: bucket.to_owned(),
            prefix: Some(key.to_owned()),
            ..Default::default()
        }))
        .await
        .expect("list succeeds")
        .output
        .contents
        .into_iter()
        .flatten()
        .any(|object| object.key.as_deref() == Some(key))
}

/// A cold pair started together, until both serve locally: the milestones of
/// the builder and follower, and which one built.
struct ColdStart {
    nodes: [Node; 2],
    builder: usize,
    built: Duration,
    installed: Duration,
    cost: Counts,
}

async fn cold_start(fleet: &Fleet) -> ColdStart {
    let nodes = [fleet.node(0).await, fleet.node(1).await];
    mutual_alive([&nodes[0], &nodes[1]], fleet.names).await;
    let before = fleet.counts();
    let started = Instant::now();
    nodes[0].start(&fleet.bucket).await;
    nodes[1].start(&fleet.bucket).await;
    let mut ready = [None, None];
    wait_until("both nodes serve locally after a cold start", || {
        for (index, node) in nodes.iter().enumerate() {
            if ready[index].is_none() && node.serves(&fleet.bucket) {
                ready[index] = Some(started.elapsed());
            }
        }
        ready.iter().all(Option::is_some)
    })
    .await;
    let cost = fleet.counts().since(before);
    let scans = nodes
        .each_ref()
        .map(|node| node.count("recovery_origin_scans"));
    assert_eq!(
        scans.iter().sum::<u64>(),
        1,
        "exactly one origin scan across the pair: scans={scans:?} {cost:?}"
    );
    let builder = usize::from(scans[1] == 1);
    ColdStart {
        builder,
        built: ready[builder].expect("the builder served"),
        installed: ready[1 - builder].expect("the follower served"),
        cost,
        nodes,
    }
}

/// (a) Two nodes start together. One scans the origin, the other waits for
/// it and installs its image: one pass of LIST pages, none by the follower.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "local MinIO and minutes of production-paced origin scans; run with --ignored --test-threads=1"]
async fn cold_start_scans_once_and_the_follower_lists_nothing() {
    let fleet = Fleet::new("fleet-prod-cold", ["prod-cold-a", "prod-cold-b"]).await;
    let cold = cold_start(&fleet).await;
    let follower = &cold.nodes[1 - cold.builder];
    let indexed = cold
        .nodes
        .each_ref()
        .map(|node| node.count("index_objects"));
    println!(
        "fleet_production cold_start builder={} built_ms={} follower_installed_ms={} \
         indexed={indexed:?} {:?}",
        fleet.names[cold.builder],
        cold.built.as_millis(),
        cold.installed.as_millis(),
        cold.cost
    );
    assert_eq!(
        cold.cost.list,
        ROWS.div_ceil(1000) as u64,
        "one pass of LIST pages; the follower listed nothing: {:?}",
        cold.cost
    );
    assert_eq!(follower.count("recovery_origin_scans"), 0);
    assert!(
        cold.installed >= cold.built,
        "the follower followed the scan"
    );
    assert_eq!(
        indexed, [ROWS as u64; 2],
        "the follower installed the image"
    );
    cold.cost.assert_no_writes();
}

/// How the restarting node goes away.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Stop {
    /// The binary's `SIGTERM` path after a completed drain: retract the lease,
    /// then seal the write feed and wait for the peer to acknowledge it.
    Planned,
    /// The pod dies where it stands.
    Crash,
}

/// Which node of the cold-started pair restarts.
#[derive(Clone, Copy, Debug)]
enum Restart {
    /// The node whose origin scan built the index.
    Builder,
    /// The node that installed the builder's image.
    Follower,
}

/// What a restart cost, from the stop until both nodes serve locally.
#[derive(Debug)]
struct Rejoin {
    cost: Counts,
    survivor_scans: u64,
    rejoiner_scans: u64,
    survivor_gaps: u64,
    survivor_renewals: u64,
    survivor_left_local: bool,
    survivor_origin_heads: usize,
}

/// Write [`WRITTEN`] keys from `first` through `writer` and wait until `reader`
/// lists them.
async fn write_through(writer: &Node, reader: &Node, bucket: &str, first: usize) {
    let keys = first..first + WRITTEN;
    let (proxy, write_bucket, written_keys) =
        (writer.proxy.clone(), bucket.to_owned(), keys.clone());
    writer
        .pod
        .handle()
        .spawn(async move {
            for n in written_keys {
                put(
                    &proxy,
                    &write_bucket,
                    &written(n),
                    b"written before the restart",
                )
                .await;
            }
        })
        .await
        .expect("writes through the restarting node");
    for _ in 0..400 {
        let mut all = true;
        for n in keys.clone() {
            all &= lists(&reader.proxy, bucket, &written(n)).await;
        }
        if all {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("the survivor applied the restarting node's writes");
}

/// The binary's `SIGTERM` path ([`CachingProxy::stop`]): retract the lease, drain
/// the node's in-flight requests — `in_flight`, a client request the pod is still
/// serving, if any — and seal the write feed. The pod is gone when this returns;
/// what its life cost is returned, with how the stop ended.
async fn planned_stop(node: Node, in_flight: Option<JoinHandle<()>>) -> (Life, Stopped) {
    let proxy = node.proxy.clone();
    let stopped = node
        .pod
        .handle()
        .spawn(async move {
            proxy
                .stop(async move {
                    if let Some(request) = in_flight {
                        let _ = request.await;
                    }
                })
                .await
        })
        .await
        .expect("planned stop");
    (node.life(), stopped)
}

/// Cold-start the pair, write through the `restart` node, stop it by `stop`,
/// and restart it under the same name and bulk port until both serve locally.
async fn rejoin(label: &str, names: [&'static str; 2], stop: Stop, restart: Restart) -> Rejoin {
    let fleet = Fleet::new(label, names).await;
    let cold = cold_start(&fleet).await;
    println!(
        "fleet_production {label} cold_start built_ms={} installed_ms={}",
        cold.built.as_millis(),
        cold.installed.as_millis()
    );
    let index = match restart {
        Restart::Builder => cold.builder,
        Restart::Follower => 1 - cold.builder,
    };
    let [first, second] = cold.nodes;
    let (survivor, restarting) = if index == 0 {
        (second, first)
    } else {
        (first, second)
    };
    let bucket = fleet.bucket.clone();
    write_through(&restarting, &survivor, &bucket, 0).await;

    let scans_before = survivor.count("recovery_origin_scans");
    let gaps_before = survivor.count("feed_gaps");
    let renewals_before = survivor.count("feed_renewals");
    let before = fleet.counts();
    if stop == Stop::Planned {
        let (_, stopped) = planned_stop(restarting, None).await;
        assert_eq!(
            stopped.sealed,
            Some(SealOutcome::Observed),
            "the survivor acknowledged the seal: {stopped:?}"
        );
    } else {
        drop(restarting);
    }

    let rejoined = fleet.node(index).await;
    let started = Instant::now();
    rejoined.start(&bucket).await;
    let mut survivor_left_local = false;
    let mut survivor_origin_heads = 0;
    loop {
        let survivor_local = survivor.serves(&bucket);
        if survivor_local && rejoined.serves(&bucket) {
            break;
        }
        if !survivor_local {
            survivor_left_local = true;
            // The survivor keeps answering, from the origin, while it recovers.
            head(&survivor.proxy, &bucket, &written(0))
                .await
                .expect("the survivor answers a HEAD while it recovers");
            survivor_origin_heads += 1;
        }
        assert!(
            started.elapsed() < DEADLINE,
            "both nodes serve locally again within {DEADLINE:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let ready_ms = started.elapsed().as_millis();
    let cost = fleet.counts().since(before);
    let outcome = Rejoin {
        cost,
        survivor_scans: survivor.count("recovery_origin_scans") - scans_before,
        rejoiner_scans: rejoined.count("recovery_origin_scans"),
        survivor_gaps: survivor.count("feed_gaps") - gaps_before,
        survivor_renewals: survivor.count("feed_renewals") - renewals_before,
        survivor_left_local,
        survivor_origin_heads,
    };
    println!(
        "fleet_production {label} stop={stop:?} restart={restart:?} rejoin_ready_ms={ready_ms} {outcome:?}"
    );
    let before_check = fleet.counts();
    for n in 0..WRITTEN {
        assert!(
            lists(&rejoined.proxy, &bucket, &written(n)).await,
            "the rejoiner lists its previous life's write {n}"
        );
    }
    assert_eq!(
        fleet.counts().since(before_check).list,
        0,
        "the rejoiner answers from its own index"
    );
    assert_eq!(
        rejoined.count("index_objects"),
        (ROWS + WRITTEN) as u64,
        "the rejoiner indexes the whole bucket"
    );
    outcome
}

/// (b) The production rolling restart: the pod is stopped by `SIGTERM` while
/// its peer is Ready. Its sealed feed lets the survivor cross the restart
/// with no gap and no scan, and the rejoiner installs the survivor's image
/// without one origin LIST, whichever node built the index.
async fn planned_rejoin(label: &str, names: [&'static str; 2], restart: Restart) {
    let rejoin = rejoin(label, names, Stop::Planned, restart).await;
    assert_eq!(
        rejoin.cost.list, 0,
        "the rejoin listed the origin: {rejoin:?}"
    );
    assert_eq!(rejoin.rejoiner_scans, 0, "{rejoin:?}");
    assert_eq!(rejoin.survivor_scans, 0, "{rejoin:?}");
    assert_eq!(
        rejoin.survivor_gaps, 0,
        "a sealed life leaves no gap: {rejoin:?}"
    );
    assert_eq!(rejoin.survivor_renewals, 1, "{rejoin:?}");
}

/// (b) The builder restarts; the survivor installed its image at the cold
/// start, and now donates it.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "local MinIO and minutes of production-paced origin scans; run with --ignored --test-threads=1"]
async fn planned_rejoin_of_the_builder_lists_nothing() {
    planned_rejoin(
        "fleet-prod-planned-builder",
        ["prod-pb-a", "prod-pb-b"],
        Restart::Builder,
    )
    .await;
}

/// (b) The follower restarts; the survivor built the index, and donates it.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "local MinIO and minutes of production-paced origin scans; run with --ignored --test-threads=1"]
async fn planned_rejoin_of_the_follower_lists_nothing() {
    planned_rejoin(
        "fleet-prod-planned-follower",
        ["prod-pf-a", "prod-pf-b"],
        Restart::Follower,
    )
    .await;
}

/// (c) A crash leaves the dead life's tail unknown: the survivor takes the
/// gap and answers from the origin while it recovers, and the pair makes
/// exactly one origin scan, which the other node follows.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "local MinIO and minutes of production-paced origin scans; run with --ignored --test-threads=1"]
async fn crash_rejoin_scans_once_and_the_other_node_follows() {
    let rejoin = rejoin(
        "fleet-prod-crash",
        ["prod-crash-a", "prod-crash-b"],
        Stop::Crash,
        Restart::Builder,
    )
    .await;
    assert!(
        rejoin.survivor_gaps >= 1,
        "the survivor took the gap: {rejoin:?}"
    );
    assert_eq!(rejoin.survivor_renewals, 0, "{rejoin:?}");
    assert!(rejoin.survivor_left_local, "{rejoin:?}");
    assert!(rejoin.survivor_origin_heads > 0, "{rejoin:?}");
    assert_eq!(
        rejoin.survivor_scans + rejoin.rejoiner_scans,
        1,
        "exactly one origin scan, the other node following it: {rejoin:?}"
    );
    assert_eq!(
        rejoin.cost.list,
        (ROWS + WRITTEN).div_ceil(1000) as u64,
        "one pass of LIST pages: {rejoin:?}"
    );
}

/// Real objects the serving follower reads.
const SERVED: usize = 6;

/// (d) A node joins a Ready peer and serves HEAD, GET, PUT and DELETE while
/// it bootstraps, from the origin. It installs its donor's image without one
/// origin LIST, and its index then reflects its own writes and deletes.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "local MinIO and minutes of production-paced origin scans; run with --ignored --test-threads=1"]
async fn follower_serves_traffic_while_it_bootstraps_and_lists_nothing() {
    let fleet = Fleet::new("fleet-prod-serving", ["prod-serving-a", "prod-serving-b"]).await;
    let bucket = fleet.bucket.clone();
    // Real objects, written through the forwarder so the listing merges them.
    let client = fleet.origin.counted_client();
    for n in 0..SERVED {
        client
            .put_object()
            .bucket(&bucket)
            .key(served(n))
            .body(format!("served {n}").into_bytes().into())
            .send()
            .await
            .expect("seed a served object");
    }
    let donor = fleet.node(0).await;
    let started = Instant::now();
    donor.start(&bucket).await;
    let built = wait_until("the donor serves locally", || donor.serves(&bucket)).await;
    let follower = fleet.node(1).await;
    mutual_alive([&donor, &follower], fleet.names).await;

    let before = fleet.counts();
    let join_started = Instant::now();
    follower.start(&bucket).await;
    let (proxy, traffic_bucket) = (follower.proxy.clone(), bucket.clone());
    let traffic = follower
        .pod
        .handle()
        .spawn(async move {
            for n in 0..SERVED {
                head(&proxy, &traffic_bucket, &served(n))
                    .await
                    .expect("origin HEAD of a served object");
            }
            for n in 2..SERVED {
                assert_eq!(
                    get(&proxy, &traffic_bucket, &served(n)).await,
                    format!("served {n}").as_bytes()
                );
            }
            delete(&proxy, &traffic_bucket, &served(0)).await;
            put(&proxy, &traffic_bucket, &served(1), b"rewritten").await;
            put(&proxy, &traffic_bucket, "zz-new", b"new object").await;
        })
        .await;
    traffic.expect("the follower served the traffic");
    let traffic_ms = join_started.elapsed().as_millis();
    assert!(
        !follower.serves(&bucket),
        "the traffic preceded the follower's install"
    );
    let installed = wait_until("the follower serves locally", || follower.serves(&bucket)).await;
    let join = fleet.counts().since(before);
    println!(
        "fleet_production serving donor_built_ms={} traffic_ms={traffic_ms} \
         follower_installed_ms={} join_ms={} {join:?}",
        built.as_millis(),
        installed.as_millis(),
        join_started.elapsed().as_millis()
    );
    assert_eq!(
        join.list, 0,
        "the follower installed the donor image without an origin LIST: {join:?}"
    );
    assert_eq!(follower.count("recovery_origin_scans"), 0);
    assert_eq!(donor.count("recovery_origin_scans"), 1);
    let before_check = fleet.counts();
    assert!(
        !lists(&follower.proxy, &bucket, &served(0)).await,
        "the delete"
    );
    assert!(lists(&follower.proxy, &bucket, &served(1)).await);
    assert!(
        lists(&follower.proxy, &bucket, "zz-new").await,
        "the new object"
    );
    assert!(
        lists(&donor.proxy, &bucket, "zz-new").await,
        "the donor applied it"
    );
    assert!(!lists(&donor.proxy, &bucket, &served(0)).await);
    assert_eq!(
        fleet.counts().since(before_check).list,
        0,
        "both nodes list from their own index"
    );
    assert_eq!(
        follower.count("index_objects"),
        donor.count("index_objects"),
        "the follower's index is the donor's"
    );
    assert_eq!(
        follower.count("index_objects"),
        (ROWS + SERVED) as u64,
        "one delete and one new object"
    );
    println!(
        "fleet_production serving total_ms={}",
        started.elapsed().as_millis()
    );
}
