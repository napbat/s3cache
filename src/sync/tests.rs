use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use groupnet::consistency::volatile_recovery::{RecoveryStage, RecoveryStatus};
use groupnet::consistency::{CAP_ACKS, CAP_LEASE, LeaseConfig};
use groupnet::core::volatile_recovery::RecoveryState;
use groupnet::core::{Config, NodeId, Status};
use groupnet::runtime::{Group, NamedSeeds, Node, ResolveFuture, SeedResolver, SystemResolver};
use groupnet::transport::mem::{MemTransport, Network};
use s3s::dto::GetObjectOutput;

use crate::index::{KeyIndex, ObjEntry, standard_class};
use crate::metrics::Metrics;
use crate::sync::coherence::{
    CAP_BOUNDED, Consistency, DEFAULT_LEASE_MS, KeyReconcile, WriteReceipt, WriteSync, WriteWait,
    recovery_generation_permits, waits_on, waits_on_unleased,
};
use crate::sync::config::{SyncConfig, parse_lease_ms, parse_seeds};
use crate::sync::wire::{
    IndexEvent, IndexOp, WIRE_MAGIC, decode_event, encode_event, from_micros, to_micros, wire_stamp,
};
use crate::tier::{CachedObject, TieredCache, open_warm};

type Index = Arc<KeyIndex>;

/// A lease short enough to watch lapse inside a test, and still comfortably inside
/// its own envelope: `D = 300ms`, renewed every 100ms, 5ms of rate margin.
const TEST_LEASE: Duration = Duration::from_millis(300);

fn test_lease() -> LeaseConfig {
    LeaseConfig::for_duration(TEST_LEASE)
}

/// Index `entry` as this writer's own put into a scratch index and advertise it.
async fn own_put(sync: &WriteSync, key: &str, entry: ObjEntry, metrics: &Metrics) -> WriteReceipt {
    sync.index_put(&KeyIndex::default(), "bkt", key, entry, metrics)
        .1
        .await
}

#[test]
fn a_reopened_recovery_generation_cannot_authorize_an_old_local_answer() {
    let status = |generation, may_serve, stage| RecoveryStatus {
        state: RecoveryState {
            generation,
            stage,
            recovered: may_serve,
            covered_lapses: 0,
        },
        may_serve,
    };
    let old = Some(7);
    assert!(recovery_generation_permits(
        Some(status(7, true, RecoveryStage::Ready)),
        old
    ));
    assert!(!recovery_generation_permits(
        Some(status(8, false, RecoveryStage::Rebuilding)),
        old
    ));
    assert!(!recovery_generation_permits(
        Some(status(8, true, RecoveryStage::Ready)),
        old
    ));
}

/// Test timings: the shipped tuning in miniature. `dead_timeout_ms` tracks the lease
/// duration exactly as [`WriteSync::new`] makes it, so what a test observes about
/// reap-bounded behaviour is the same shape production gets, only faster.
fn brisk() -> Config {
    Config {
        gossip_interval_ms: 10,
        probe_interval_ms: 20,
        probe_timeout_ms: 10,
        suspect_timeout_ms: 50,
        dead_timeout_ms: 300,
        anti_entropy_interval_ms: 25,
        ..Config::default()
    }
}

/// The entry a write path hands [`WriteSync::publish_put`]: a size, an `ETag` the
/// origin gave it, and the local write clock at the wire's precision.
fn written(size: i64) -> ObjEntry {
    ObjEntry {
        size: Some(size),
        last_modified: wire_stamp(SystemTime::now()),
        etag: Some(s3s::dto::ETag::Strong("deadbeef".to_owned())),
        storage_class: standard_class(),
        content_type: Some("text/x-fixture".to_owned()),
        meta: None,
    }
}

#[test]
fn only_acknowledged_strong_waits_retire_feed_history() {
    for consistency in [Consistency::Strong, Consistency::StrongAcks] {
        assert!(WriteWait::Applied.retires_feed(consistency));
        assert!(WriteWait::Lapsed(Vec::new()).retires_feed(consistency));
        assert!(
            !(WriteWait::Stalled {
                waiting_on: Vec::new(),
                lapsed: Vec::new(),
            })
            .retires_feed(consistency)
        );
    }
    assert!(
        !WriteWait::Applied.retires_feed(Consistency::Bounded),
        "bounded Applied is immediate, not an all-readers watermark"
    );
    assert!(!WriteWait::Lapsed(Vec::new()).retires_feed(Consistency::Bounded));
}

fn spawn_node(net: &Network, id: &str, peer: &str) -> (NodeId, Node<MemTransport>, Group) {
    let me = NodeId::new(id);
    let node = Node::builder(me.clone(), net.endpoint(me.clone()))
        .seed(NodeId::new(peer))
        .config(brisk())
        .spawn();
    let group = node.join_group("s3cache");
    (me, node, group)
}

/// A [`WriteSync`] on `group`, leased at [`TEST_LEASE`] unless the mode says
/// otherwise.
fn attach(group: Group, me: NodeId, consistency: Consistency) -> WriteSync {
    WriteSync::attach(group, me, consistency, test_lease(), None)
}

fn cached(body: &'static [u8]) -> Arc<CachedObject> {
    Arc::new(CachedObject::from_get(
        &GetObjectOutput::default(),
        bytes::Bytes::from_static(body),
    ))
}

fn indexed_size(state: &Index, bucket: &str, key: &str) -> Option<i64> {
    state
        .read()
        .unwrap()
        .get(bucket)
        .and_then(|b| b.keys.get(key))
        .and_then(|e| e.size)
}

/// One indexed entry, cloned out for the assertions that look past its size.
fn indexed(state: &Index, bucket: &str, key: &str) -> Option<ObjEntry> {
    state
        .read()
        .unwrap()
        .get(bucket)
        .and_then(|b| b.keys.get(key))
        .cloned()
}

/// A fully-wired pair: node A publishes, node B applies into `state` and
/// its local cache. Returns A's publisher, B's sync, and B's state and cache.
fn wired_pair(net: &Network) -> (WriteSync, Arc<WriteSync>, Index, TieredCache) {
    wired_pair_named(net, ("sync-a", "sync-b"), Consistency::Strong)
}

/// [`wired_pair`] with the gossip identities and the mode spelled out — unique ids
/// per test, so parallel tests never share one.
///
/// B comes back as an [`Arc`]: it is the node whose apply loop runs, and
/// [`WriteSync::start_apply`] hands the lapse watch a [`Weak`] back to it.
fn wired_pair_named(
    net: &Network,
    ids: (&str, &str),
    consistency: Consistency,
) -> (WriteSync, Arc<WriteSync>, Index, TieredCache) {
    let (a_id, _a_node, a_group) = spawn_node(net, ids.0, ids.1);
    let (b_id, _b_node, b_group) = spawn_node(net, ids.1, ids.0);
    let metrics = Arc::new(Metrics::default());
    let cache = TieredCache::new(1024 * 1024, None, metrics.clone());
    let state = Arc::new(KeyIndex::default());
    let sync_b = Arc::new(attach(b_group, b_id, consistency));
    sync_b.start_apply(cache.local(), state.clone(), metrics, no_reconcile());
    let sync_a = attach(a_group, a_id, consistency);
    (sync_a, sync_b, state, cache)
}

async fn eventually(mut cond: impl FnMut() -> bool, what: &str) {
    for _ in 0..300 {
        if cond() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("timed out waiting for: {what}");
}

/// An apply loop for tests whose peers write nothing they cannot describe.
fn no_reconcile() -> KeyReconcile {
    Arc::new(|_: &str, _: &str, _: u64| {})
}

/// The env spellings [`WriteSync::new`] is configured through: seeds split on the
/// first `=` (host:port may not contain one), blanks and malformed entries drop,
/// and an unknown consistency mode falls back to the safe one.
#[test]
fn env_spellings_parse_into_a_config() {
    assert_eq!(
        parse_seeds("a=host-a:1, b=host-b:2 ,,"),
        [
            ("a".to_owned(), "host-a:1".to_owned()),
            ("b".to_owned(), "host-b:2".to_owned())
        ]
    );
    assert_eq!(parse_seeds(""), [] as [(String, String); 0]);
    assert!(parse_seeds("no-equals-sign").is_empty(), "malformed drops");
    assert!(Consistency::parse("") == Consistency::Strong);
    assert!(Consistency::parse(" Bounded ") == Consistency::Bounded);
    assert!(
        Consistency::parse(" Strong-Acks ") == Consistency::StrongAcks,
        "the ack-only spelling is its own mode, not a synonym for strong"
    );
    assert!(
        Consistency::parse("eventual") == Consistency::Strong,
        "an unknown mode falls back to strong"
    );

    assert_eq!(
        parse_lease_ms(None),
        DEFAULT_LEASE_MS,
        "unset is the default"
    );
    assert_eq!(parse_lease_ms(Some(" 750 ")), 750);
    assert_eq!(
        parse_lease_ms(Some("0")),
        DEFAULT_LEASE_MS,
        "zero is the engine's `never expires` — the stale claim the tier prevents"
    );
    assert_eq!(parse_lease_ms(Some("soon")), DEFAULT_LEASE_MS);
}

/// The seed cadence the resolution tests run on: fast enough to watch several
/// rounds, slow enough not to spin.
const TEST_SEED_CADENCE: Duration = Duration::from_millis(20);

/// Failed lookups before the test's unresolvable seed is reported.
const TEST_STARTUP_ATTEMPTS: u32 = 3;

/// How long a loopback gossip pair may take to (re)meet.
const TEST_REJOIN: Duration = Duration::from_secs(20);

/// A seed name whose address the test moves at will; `None` does not resolve.
#[derive(Clone, Default)]
struct MovableSeed(Arc<std::sync::Mutex<Option<std::net::SocketAddr>>>);

impl MovableSeed {
    fn point_at(&self, addr: Option<std::net::SocketAddr>) {
        *self.0.lock().expect("seed address") = addr;
    }
}

impl SeedResolver for MovableSeed {
    fn resolve<'a>(&'a self, name: &'a str) -> ResolveFuture<'a> {
        let addr = *self.0.lock().expect("seed address");
        Box::pin(async move {
            addr.ok_or_else(|| std::io::Error::new(std::io::ErrorKind::NotFound, name.to_owned()))
        })
    }
}

fn loopback() -> std::net::SocketAddr {
    std::net::UdpSocket::bind("127.0.0.1:0")
        .and_then(|socket| socket.local_addr())
        .expect("a free loopback UDP port")
}

fn seed_config(id: &str, bind: std::net::SocketAddr, seeds: &[(&str, &str)]) -> SyncConfig {
    SyncConfig {
        bind: bind.to_string(),
        advertise: None,
        seeds: seeds
            .iter()
            .map(|(id, addr)| ((*id).to_owned(), (*addr).to_owned()))
            .collect(),
        node_id: id.to_owned(),
        consistency: Consistency::Strong,
        lease_ms: DEFAULT_LEASE_MS,
    }
}

fn sees_alive(sync: &WriteSync, peer: &str) -> bool {
    sync.group().member_status(&NodeId::new(peer)) == Some(Status::Alive)
}

async fn within(limit: Duration, what: &str, mut cond: impl FnMut() -> bool) {
    let deadline = Instant::now() + limit;
    while !cond() {
        assert!(Instant::now() < deadline, "timed out waiting for: {what}");
        tokio::time::sleep(TEST_SEED_CADENCE).await;
    }
}

/// A seed whose name never resolves stops holding readiness once its startup
/// window is spent, instead of keeping the pod out of the Service forever.
#[tokio::test(flavor = "multi_thread")]
async fn an_unresolvable_seed_releases_readiness_after_its_window() {
    let resolution = NamedSeeds::new(MovableSeed::default())
        .retry_interval(TEST_SEED_CADENCE)
        .startup_attempts(TEST_STARTUP_ATTEMPTS)
        .refresh_interval(TEST_SEED_CADENCE);
    let sync = WriteSync::bind(
        seed_config("seed-lone", loopback(), &[("seed-ghost", "ghost:1")]),
        resolution,
    )
    .await
    .expect("binds loopback");
    assert!(
        sync.peer_may_hold_index(),
        "an unmet seed may hold an index"
    );
    within(TEST_REJOIN, "the unresolvable seed is given up", || {
        !sync.peer_may_hold_index()
    })
    .await;
}

/// A seed that comes back at a new address is relearned by re-resolution, and
/// this node reaches the new life — which, seeding nobody, would otherwise
/// never hear from it.
#[tokio::test(flavor = "multi_thread")]
async fn a_moved_seed_is_relearned_and_rejoins() {
    let (a, b_first, b_moved) = (loopback(), loopback(), loopback());
    let name_b = MovableSeed::default();
    name_b.point_at(Some(b_first));
    let resolution = NamedSeeds::new(name_b.clone())
        .retry_interval(TEST_SEED_CADENCE)
        .refresh_interval(TEST_SEED_CADENCE);
    let node_a = WriteSync::bind(seed_config("seed-a", a, &[("seed-b", "b:1")]), resolution)
        .await
        .expect("binds loopback");
    let peer = |bind| {
        WriteSync::bind(
            seed_config("seed-b", bind, &[]),
            NamedSeeds::new(SystemResolver),
        )
    };
    // The first life runs on a runtime of its own, so stopping it is a crash.
    let pod = tokio::runtime::Runtime::new().expect("a runtime for the first life");
    let first = pod
        .spawn(peer(b_first))
        .await
        .expect("the first life starts")
        .expect("binds loopback");
    within(TEST_REJOIN, "the pair meets", || {
        sees_alive(&node_a, "seed-b") && sees_alive(&first, "seed-a")
    })
    .await;

    drop(first);
    pod.shutdown_background();
    within(TEST_REJOIN, "the old life is gone", || {
        !sees_alive(&node_a, "seed-b")
    })
    .await;
    name_b.point_at(Some(b_moved));
    let moved = peer(b_moved).await.expect("binds loopback");
    within(TEST_REJOIN, "the moved seed rejoins", || {
        sees_alive(&node_a, "seed-b") && sees_alive(&moved, "seed-a")
    })
    .await;
}

/// Only the leased mode advertises [`CAP_LEASE`], because only it constructs a
/// lease set — a node advertising it without a running granter freezes every other
/// reader's confirmation until it grants or departs.
#[test]
fn only_a_mode_that_grants_leases_advertises_that_it_does() {
    assert_eq!(Consistency::Strong.capabilities(), [CAP_ACKS, CAP_LEASE]);
    assert_eq!(Consistency::StrongAcks.capabilities(), [CAP_ACKS]);
    assert_eq!(Consistency::Bounded.capabilities(), [CAP_BOUNDED]);
    for mode in [
        Consistency::Strong,
        Consistency::StrongAcks,
        Consistency::Bounded,
    ] {
        assert_eq!(
            mode.leases(),
            mode.capabilities().contains(&CAP_LEASE),
            "the advertisement and the granter are one decision, in both directions"
        );
    }
}

/// Which peers a strong writer's ack wait includes. The rule is a
/// three-way one, and the third case is the one that matters: a peer that
/// has advertised nothing is *unknown*, not absent, so it is waited for.
#[tokio::test]
async fn the_ack_wait_covers_advertisers_and_unknowns_but_not_declared_bounded_peers() {
    let net = Network::new();
    let (_a_id, _a_node, a_group) = spawn_node(&net, "cap-a", "cap-b");
    let (b_id, _b_node, b_group) = spawn_node(&net, "cap-b", "cap-a");
    let (c_id, _c_node, c_group) = spawn_node(&net, "cap-c", "cap-a");
    // D runs, joins, and never advertises — a node from before this change.
    let (d_id, _d_node, _d_group) = spawn_node(&net, "cap-d", "cap-a");
    b_group
        .advertise_capabilities([CAP_ACKS])
        .expect("B advertises the ack tier");
    c_group
        .advertise_capabilities([CAP_BOUNDED])
        .expect("C declares itself bounded");

    eventually(
        || !waits_on(&a_group, &c_id),
        "C's bounded declaration to converge at the writer",
    )
    .await;
    assert!(
        waits_on(&a_group, &b_id),
        "a CAP_ACKS advertiser is waited for"
    );
    assert!(
        waits_on(&a_group, &d_id),
        "so is a peer that has never advertised: absence is not non-participation"
    );
}

/// The **transition** set a leased writer's second wait covers: exactly the peers
/// the lease wait cannot, because they publish no `~lease` entry for it to wait on
/// or to expire. A uniform leased fleet makes it empty, which is what keeps the
/// insurance from costing a healthy cluster a redundant ack round per write.
#[tokio::test]
async fn the_transition_wait_covers_exactly_what_the_lease_cannot() {
    let net = Network::new();
    let (_a_id, _a_node, a_group) = spawn_node(&net, "mix-a", "mix-b");
    let (b_id, _b_node, b_group) = spawn_node(&net, "mix-b", "mix-a");
    let (c_id, _c_node, c_group) = spawn_node(&net, "mix-c", "mix-a");
    // D runs, joins, and never advertises — a node from before any of this.
    let (d_id, _d_node, _d_group) = spawn_node(&net, "mix-d", "mix-a");
    let (e_id, _e_node, e_group) = spawn_node(&net, "mix-e", "mix-a");
    b_group
        .advertise_capabilities(Consistency::StrongAcks.capabilities().iter().copied())
        .expect("B pins the ack tier");
    c_group
        .advertise_capabilities(Consistency::Bounded.capabilities().iter().copied())
        .expect("C declares itself bounded");
    e_group
        .advertise_capabilities(Consistency::Strong.capabilities().iter().copied())
        .expect("E runs the lease tier");

    eventually(
        || !waits_on_unleased(&a_group, &c_id) && !waits_on_unleased(&a_group, &e_id),
        "C's and E's declarations to converge at the writer",
    )
    .await;
    assert!(
        waits_on_unleased(&a_group, &b_id),
        "a strong-acks peer holds no lease, so only this wait can cover it"
    );
    assert!(
        waits_on_unleased(&a_group, &d_id),
        "and so does a peer that has never advertised — the whole point of the set"
    );
    assert!(
        waits_on(&a_group, &e_id) && !waits_on_unleased(&a_group, &e_id),
        "a lease advertiser is the coherence wait's business, not this one's"
    );
}

#[test]
fn event_codec_round_trips_and_rejects_garbage() {
    let event = IndexEvent {
        op: IndexOp::Put {
            size: Some(42),
            etag: Some("\"deadbeef\"".to_owned()),
            content_type: Some("text/x-fixture".to_owned()),
            storage_class: Some("STANDARD".to_owned()),
        },
        bucket: "bucket-1".to_owned(),
        key: "a/b weird\0key".to_owned(),
        ts_us: 1_700_000_000_000_000,
    };
    let encoded = encode_event(&event);
    assert_eq!(
        encoded.first(),
        Some(&WIRE_MAGIC),
        "the sender prefixes the current event format"
    );
    let mut retired = vec![0xFF];
    retired.extend(bincode::serialize(&event).expect("the event shape serializes"));
    assert!(
        decode_event(&retired).is_none(),
        "the decoder rejects the retired bincode envelope"
    );
    let back = decode_event(&encoded).expect("round trip");
    let IndexOp::Put {
        size,
        etag,
        content_type,
        storage_class,
    } = back.op
    else {
        panic!("a put decodes as a put");
    };
    assert_eq!(size, Some(42));
    assert_eq!(etag.as_deref(), Some("\"deadbeef\""));
    assert_eq!(content_type.as_deref(), Some("text/x-fixture"));
    assert_eq!(storage_class.as_deref(), Some("STANDARD"));
    assert_eq!(back.bucket, event.bucket);
    assert_eq!(back.key, event.key);
    assert_eq!(back.ts_us, event.ts_us);
    assert!(decode_event(b"\xff\xff").is_none());
}

/// The LWW clock is only comparable if both sides carry the same precision: a local
/// stamp is truncated to what the wire holds, so a peer's event for the same instant
/// ties (and deletes win ties) instead of always losing to the finer local clock.
#[test]
fn local_stamps_are_truncated_to_the_wire_precision() {
    let now = SystemTime::now();
    let stamped = wire_stamp(now);
    assert!(stamped <= now);
    assert_eq!(
        from_micros(to_micros(stamped)),
        stamped,
        "a stamped time survives the wire unchanged"
    );
    assert!(now.duration_since(stamped).expect("not in the future") < Duration::from_micros(1));
}

/// The full coherence story on one writer: put indexes + invalidates on
/// the peer, delete removes — in the writer's order.
#[tokio::test]
async fn peer_events_fold_into_index_and_invalidate() {
    let net = Network::new();
    let (sync_a, _sync_b, state, cache) = wired_pair(&net);
    let metrics = Metrics::default();

    // B holds a soon-stale body copy.
    let ckey = ("bkt".to_owned(), "obj".to_owned());
    cache.insert(ckey.clone(), cached(b"stale")).await;

    own_put(&sync_a, "obj", written(42), &metrics).await;
    eventually(
        || indexed_size(&state, "bkt", "obj") == Some(42),
        "put reaches the peer index",
    )
    .await;
    assert_eq!(state.stats().objects, 1);
    assert_eq!(state.stats().logical_bytes, 42);
    let entry = indexed(&state, "bkt", "obj").expect("the peer indexed the write");
    assert_eq!(
        entry.etag.as_ref().map(s3s::dto::ETag::value),
        Some("deadbeef"),
        "the event envelope carries the origin's ETag to peers"
    );
    assert_eq!(entry.content_type.as_deref(), Some("text/x-fixture"));
    assert!(
        entry.meta.is_none(),
        "no user metadata rides the feed, so the entry stays skeletal"
    );
    let mut invalidated = false;
    for _ in 0..300 {
        if cache.get(&ckey).await.is_none() {
            invalidated = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(invalidated, "put invalidates the peer's body copy");

    sync_a
        .index_del(
            &KeyIndex::default(),
            "bkt",
            "obj",
            SystemTime::now(),
            &metrics,
        )
        .await;
    eventually(
        || indexed_size(&state, "bkt", "obj").is_none(),
        "delete reaches the peer index",
    )
    .await;
    assert_eq!(state.stats().objects, 0);
    assert_eq!(state.stats().logical_bytes, 0);
}

/// A peer's delete retires this node's warm disk copy too, not just the hot one.
/// The index no longer names the key, so the file could only ever sit in the disk
/// budget crowding out live bodies until LRU reached it.
#[tokio::test]
async fn peer_delete_retires_the_warm_copy() {
    let net = Network::new();
    let (a_id, _a_node, a_group) = spawn_node(&net, "warm-a", "warm-b");
    let (b_id, _b_node, b_group) = spawn_node(&net, "warm-b", "warm-a");
    let metrics = Arc::new(Metrics::default());
    let dir = std::env::temp_dir().join(format!("s3cache-peer-warm-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let warm = open_warm(dir.clone(), 1024 * 1024, 64 * 1024, metrics.clone()).unwrap();
    let cache = TieredCache::new(1024 * 1024, Some(warm), metrics.clone());
    let state = Arc::new(KeyIndex::default());
    let sync_b = Arc::new(attach(b_group, b_id, Consistency::Strong));
    sync_b.start_apply(
        cache.local(),
        state.clone(),
        metrics.clone(),
        no_reconcile(),
    );
    let sync_a = attach(a_group, a_id, Consistency::Strong);

    own_put(&sync_a, "gone", written(4), &metrics).await;
    eventually(
        || indexed_size(&state, "bkt", "gone") == Some(4),
        "put reaches the peer index",
    )
    .await;
    let ckey = ("bkt".to_owned(), "gone".to_owned());
    cache.insert(ckey.clone(), cached(b"body")).await;
    assert!(cache.get(&ckey).await.is_some(), "the body is cached");

    sync_a
        .index_del(
            &KeyIndex::default(),
            "bkt",
            "gone",
            SystemTime::now(),
            &metrics,
        )
        .await;
    let mut retired = false;
    for _ in 0..300 {
        // A lookup that misses hot promotes from warm, so `None` means the disk
        // copy is gone as well.
        if indexed_size(&state, "bkt", "gone").is_none() && cache.get(&ckey).await.is_none() {
            retired = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(retired, "the peer delete retired the warm copy");
    drop(cache);
    let _ = std::fs::remove_dir_all(dir);
}

/// A peer's write it could not describe (the origin failed it after perhaps applying
/// it) fences the key here before this node acknowledges it: the hot copy goes, the
/// key reads from the origin, and the fence's token goes to the origin
/// reconciliation. The writer's next describable write clears the fence.
#[tokio::test]
async fn an_undescribed_peer_write_fences_the_key_before_it_is_acknowledged() {
    let net = Network::new();
    let (a_id, _a_node, a_group) = spawn_node(&net, "unknown-a", "unknown-b");
    let (b_id, _b_node, b_group) = spawn_node(&net, "unknown-b", "unknown-a");
    let metrics = Arc::new(Metrics::default());
    let cache = TieredCache::new(1024 * 1024, None, metrics.clone());
    let state = Arc::new(KeyIndex::default());
    let reconciling = Arc::new(std::sync::Mutex::new(Vec::new()));
    let sync_b = Arc::new(attach(b_group, b_id, Consistency::Strong));
    let handed = Arc::clone(&reconciling);
    sync_b.start_apply(
        cache.local(),
        state.clone(),
        metrics.clone(),
        Arc::new(move |bucket: &str, key: &str, token: u64| {
            handed
                .lock()
                .unwrap()
                .push((bucket.to_owned(), key.to_owned(), token));
        }),
    );
    let sync_a = attach(a_group, a_id, Consistency::Strong);

    let first = own_put(&sync_a, "ptr", written(4), &metrics).await;
    assert!(
        sync_b
            .reached_token(&first.header, Duration::from_secs(5))
            .await
    );
    let ckey = ("bkt".to_owned(), "ptr".to_owned());
    cache.insert(ckey.clone(), cached(b"old")).await;

    let (_, publishing) = sync_a.index_unknown(&KeyIndex::default(), "bkt", "ptr", &metrics);
    let unknown = publishing.await;
    assert!(
        sync_b
            .reached_token(&unknown.header, Duration::from_secs(5))
            .await
    );
    let token = state.read().unwrap()["bkt"]
        .uncertain_keys
        .get("ptr")
        .copied();
    assert!(token.is_some(), "the applied event left the key fenced");
    assert_eq!(
        *reconciling.lock().unwrap(),
        [("bkt".to_owned(), "ptr".to_owned(), token.unwrap())],
        "the fence went to the origin reconciliation"
    );
    assert!(
        cache.get(&ckey).await.is_none(),
        "the hot copy is gone before the acknowledgement"
    );

    let later = own_put(&sync_a, "ptr", written(5), &metrics).await;
    assert!(
        sync_b
            .reached_token(&later.header, Duration::from_secs(5))
            .await
    );
    assert!(state.read().unwrap()["bkt"].uncertain_keys.is_empty());
    assert_eq!(indexed_size(&state, "bkt", "ptr"), Some(5));
}

/// The strict-LIST barrier: after a publish, `await_fresh` on the peer
/// returns true only once the event is actually applied.
#[tokio::test]
async fn await_fresh_reflects_the_publishers_head() {
    let net = Network::new();
    let (sync_a, sync_b, state, _cache) = wired_pair(&net);
    let metrics = Metrics::default();

    own_put(&sync_a, "fresh", written(7), &metrics).await;
    // Wait until the peer has applied the write, then barrier: a caught-up
    // node must pass promptly, and a passed barrier implies the applied
    // index reflects every head the barrier saw.
    eventually(
        || indexed_size(&state, "bkt", "fresh") == Some(7),
        "apply loop catches the write",
    )
    .await;
    let caught_up = sync_b.await_fresh(Duration::from_secs(5)).await;
    assert!(
        caught_up,
        "barrier must pass once the apply loop is caught up"
    );
}

/// Session tokens: the issuer satisfies its own instantly, a peer only
/// once the write is applied, and garbage never blocks a read.
#[tokio::test]
async fn write_tokens_upgrade_reads_to_strict() {
    let net = Network::new();
    let (sync_a, sync_b, state, _cache) = wired_pair(&net);
    let metrics = Metrics::default();

    let receipt = own_put(&sync_a, "tok", written(1), &metrics).await;
    assert!(
        sync_a
            .reached_token(&receipt.header, Duration::from_millis(50))
            .await,
        "the issuer's own token is trivially satisfied"
    );
    assert!(
        sync_b
            .reached_token(&receipt.header, Duration::from_secs(5))
            .await,
        "a peer satisfies the token once the write is applied"
    );
    assert!(
        matches!(
            sync_a
                .wait_cluster_applied(receipt.token, Duration::from_secs(5))
                .await,
            WriteWait::Applied
        ),
        "the write-ack wait resolves once every alive peer applied"
    );
    assert_eq!(indexed_size(&state, "bkt", "tok"), Some(1));
    assert!(
        sync_b
            .reached_token("not-a-token", Duration::from_millis(10))
            .await,
        "garbage tokens never block"
    );
    assert!(
        !sync_b
            .reached_token("ghost:9:9", Duration::from_millis(200))
            .await,
        "an unsatisfiable foreign token must fail closed"
    );
    assert!(
        !sync_a
            .reached_token("ghost:1:1", Duration::from_millis(10))
            .await,
        "without an apply loop a foreign token is unverifiable"
    );
}

/// The pre-lease mode's contract, kept exactly as it was for the one release it
/// survives: the write-ack wait has to actually fire when a peer does not
/// acknowledge — the counter operators watch is only worth watching if it moves —
/// the stalled outcome must not retire feed history the peer still needs, and it is
/// reported to the caller as carrying no guarantee, so the client gets a 503 rather
/// than a success.
#[tokio::test]
async fn in_strong_acks_an_unacked_write_is_counted_and_not_retired() {
    let net = Network::new();
    let (_sync_a, sync_b, _state, _cache) =
        wired_pair_named(&net, ("acks-a", "acks-b"), Consistency::StrongAcks);
    let metrics = Metrics::default();
    // A was attached without an apply loop, so it publishes no ack ledger — but it
    // is alive, so B has to wait for it and then give up.
    eventually(
        || {
            sync_b
                .group
                .statuses()
                .iter()
                .any(|(id, status)| id.as_str() == "acks-a" && *status == Status::Alive)
        },
        "A to appear alive in B's membership view",
    )
    .await;

    let receipt = own_put(&sync_b, "unacked", written(1), &metrics).await;
    let guaranteed = sync_b
        .ack_write(
            receipt.token,
            Duration::from_millis(200),
            "bkt",
            "unacked",
            &metrics,
        )
        .await;
    assert!(!guaranteed, "a stalled wait must never be acknowledged");
    assert!(
        metrics
            .prometheus_text()
            .contains("\ns3cache_ack_timeouts 1\n"),
        "the ack timeout is counted"
    );
    assert!(
        !(WriteWait::Stalled {
            waiting_on: Vec::new(),
            lapsed: Vec::new(),
        })
        .retires_feed(Consistency::StrongAcks),
        "the timed-out write remains advertised for a lagging peer"
    );
}

/// The fast path: a leased write is an ack round and nothing more. When it returns,
/// the peer already holds the write — asserted with no polling in between, which is
/// the whole claim `strong` makes to a client.
#[tokio::test]
async fn a_leased_write_resolves_on_acks_and_the_peer_already_has_it() {
    let net = Network::new();
    let (sync_a, _sync_b, state, _cache) =
        wired_pair_named(&net, ("fast-a", "fast-b"), Consistency::Strong);
    let metrics = Metrics::default();
    // Nothing about a *fast* path is provable until there is somebody to be fast
    // against: an empty wait set resolves immediately and would pass this vacuously.
    eventually(
        || sync_a.lease_holders().len() == 1,
        "A to adopt B's serve-lease",
    )
    .await;

    let started = Instant::now();
    let receipt = own_put(&sync_a, "fast", written(9), &metrics).await;
    let outcome = sync_a
        .wait_cluster_applied(receipt.token, Duration::from_secs(5))
        .await;

    assert!(
        matches!(outcome, WriteWait::Applied),
        "a healthy leased write ends on acknowledgement, not on a lapse"
    );
    assert_eq!(
        indexed_size(&state, "bkt", "fast"),
        Some(9),
        "and B had applied it before the write returned"
    );
    assert!(
        started.elapsed() < TEST_LEASE,
        "an ack round costs well under one lease duration ({:?})",
        started.elapsed()
    );
}

/// **The lapse bound**, which is the whole reason this tier exists: a peer that stops
/// renewing does not tax writes forever waiting for it to *learn* it should stand
/// down. Its serve-lease expires on the writer's own engine, the write completes with
/// the guarantee intact — the straggler serves nothing cached until it
/// re-synchronizes — and the writes after it are free again.
#[tokio::test]
async fn a_dropped_peers_lease_lapses_and_the_write_completes_inside_one_duration() {
    let net = Network::new();
    let (a_id, _a_node, a_group) = spawn_node(&net, "lapse-a", "lapse-b");
    let (b_id, _b_node, b_group) = spawn_node(&net, "lapse-b", "lapse-a");
    let sync_a = attach(a_group, a_id, Consistency::Strong);
    // B renews and grants but never applies: no apply loop, so no ack ledger. It is
    // the fail-slow reader, which is exactly the peer a lapse has to rescue.
    let sync_b = attach(b_group, b_id.clone(), Consistency::Strong);
    let metrics = Metrics::default();
    // Both halves of B's participation have to have converged: the lease A will
    // wait on, and the advertisement that keeps B out of the transitional ack wait.
    // Otherwise this measures gossip convergence rather than the lapse.
    eventually(
        || sync_a.lease_holders() == [b_id.clone()] && !waits_on_unleased(&sync_a.group, &b_id),
        "A to adopt B's serve-lease and its lease advertisement",
    )
    .await;

    // A drop is the shape of the process dying: renewals stop, and the entry is
    // deliberately *not* retracted — it has to lapse on the writer's clock, which is
    // the bound under test.
    drop(sync_b);

    let started = Instant::now();
    let receipt = own_put(&sync_a, "lapsed", written(1), &metrics).await;
    let guaranteed = sync_a
        .ack_write(
            receipt.token,
            Duration::from_millis(200),
            "bkt",
            "lapsed",
            &metrics,
        )
        .await;
    assert!(guaranteed, "a proven lapse carries the guarantee");
    let stalled = started.elapsed();
    assert!(
        stalled < TEST_LEASE * 3,
        "the write ends at the lapse (~one lease duration), not at its own deadline ({stalled:?})"
    );
    let text = metrics.prometheus_text();
    assert!(
        text.contains("\ns3cache_write_lease_lapses 1\n"),
        "the lapse is counted as the guarantee it is"
    );
    assert!(
        text.contains("\ns3cache_ack_timeouts 0\n"),
        "and never as the alarm it is not"
    );

    // And the cost is not recurring: the next write carries the guarantee without
    // waiting out B again. B is either out of the wait set (Applied) or, while its
    // entry lingers, excused at once by the lapse already proven (Lapsed).
    let started = Instant::now();
    let receipt = own_put(&sync_a, "after", written(2), &metrics).await;
    assert!(matches!(
        sync_a
            .wait_cluster_applied(receipt.token, Duration::from_secs(5))
            .await,
        WriteWait::Applied | WriteWait::Lapsed(_)
    ));
    assert!(
        started.elapsed() < TEST_LEASE,
        "a lapsed peer is waited out once, not once per write"
    );
}

/// The availability price of a roster that only grows, made visible: a granter that dies
/// without departing stays counted — through suspect, dead and reap — so the survivor
/// serves from the origin until it returns, and names it as the reason.
#[tokio::test]
async fn a_granter_that_crashed_without_departing_is_named_until_it_returns() {
    let net = Network::new();
    let (a_id, _a_node, a_group) = spawn_node(&net, "absent-a", "absent-b");
    let (b_id, b_node, b_group) = spawn_node(&net, "absent-b", "absent-a");
    let sync_a = attach(a_group, a_id, Consistency::Strong);
    let sync_b = attach(b_group, b_id.clone(), Consistency::Strong);
    eventually(
        || sync_a.lease_granted_by(&b_id).is_some(),
        "B to grant A's serve-lease",
    )
    .await;
    assert!(
        sync_a.absent_granters().is_empty(),
        "B is alive and granting"
    );

    // A crash: no departure, the node simply stops. Dropping the handles is not enough
    // on its own — the receive loop keeps the node ticking until its endpoint is
    // evicted, which registering the id again does.
    drop(sync_b);
    drop(b_node);
    drop(net.endpoint(b_id.clone()));
    eventually(
        || sync_a.absent_granters() == [b_id.clone()],
        "A to name B as the counted granter it is waiting on",
    )
    .await;
    // Past the reap horizon B is still counted, and A still cannot serve locally.
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    assert_eq!(sync_a.absent_granters(), std::slice::from_ref(&b_id));
    assert!(!sync_a.may_serve_local());

    // B returns under the same id (a StatefulSet pod) and is no longer named.
    let (b_id, _b_node, b_group) = spawn_node(&net, "absent-b", "absent-a");
    let _sync_b = attach(b_group, b_id, Consistency::Strong);
    eventually(
        || sync_a.absent_granters().is_empty(),
        "A to see the returned granter alive",
    )
    .await;
}

#[tokio::test]
async fn flush_drops_every_local_copy() {
    let cache = TieredCache::new(1024 * 1024, None, Arc::new(Metrics::default()));
    let key = ("bkt".to_owned(), "obj".to_owned());
    cache.insert(key.clone(), cached(b"body")).await;
    assert!(cache.get(&key).await.is_some());
    cache.local().flush().await;
    assert!(
        cache.get(&key).await.is_none(),
        "flush empties the hot tier"
    );
}
