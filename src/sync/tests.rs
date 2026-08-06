use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime};

use groupnet::consistency::{CAP_ACKS, CAP_LEASE, LeaseConfig, WriteFeed};
use groupnet::core::{Config, NodeId, Status};
use groupnet::runtime::{Group, Node};
use groupnet::transport::mem::{MemTransport, Network};
use groupnet::transport::{Inbound, Transport};
use s3s::dto::GetObjectOutput;

use crate::index::{BucketState, ObjEntry, standard_class};
use crate::metrics::Metrics;
use crate::sync::coherence::{
    AFFIRM_POLL, CAP_BOUNDED, Consistency, DEFAULT_LEASE_MS, FEED_CAPACITY, WriteSync, WriteWait,
    waits_on, waits_on_unleased,
};
use crate::sync::config::{parse_lease_ms, parse_seeds};
use crate::sync::recovery::{lapse_poll, settle_window};
use crate::sync::wire::{
    IndexEvent, IndexOp, WIRE_MAGIC, decode_event, encode_event, from_micros, to_micros, wire_stamp,
};
use crate::tier::{CachedObject, LocalCache, TieredCache};

type Index = Arc<RwLock<HashMap<String, BucketState>>>;

/// A lease short enough to watch lapse inside a test, and still comfortably inside
/// its own envelope: `D = 300ms`, renewed every 100ms, 5ms of rate margin.
const TEST_LEASE: Duration = Duration::from_millis(300);

fn test_lease() -> LeaseConfig {
    LeaseConfig::for_duration(TEST_LEASE)
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

fn spawn_node(net: &Network, id: &str, peer: &str) -> (NodeId, Node<MemTransport>, Group) {
    let me = NodeId::new(id);
    let node = Node::builder(me.clone(), net.endpoint(me.clone()))
        .seed(NodeId::new(peer))
        .config(brisk())
        .spawn();
    let group = node.join_group("s3cache");
    (me, node, group)
}

/// A [`MemTransport`] endpoint with a kill switch — the only way to make a peer
/// actually *die* in-process. Dropping a [`Node`] does not stop it (its receive loop
/// holds its own handle), and a peer that keeps gossiping is never reaped, so nothing
/// else can produce the input the lapse watcher exists for: a lease-granting peer that
/// goes silent for good. Unplugged, this neither sends nor delivers, which is what a
/// dead process looks like from the outside.
struct Unpluggable {
    inner: MemTransport,
    plugged: Arc<AtomicBool>,
}

impl Transport for Unpluggable {
    type Error = <MemTransport as Transport>::Error;

    async fn send(&self, to: &NodeId, msg: &[u8]) -> Result<(), Self::Error> {
        if self.plugged.load(Ordering::Relaxed) {
            self.inner.send(to, msg).await
        } else {
            Ok(()) // best-effort transports drop, they do not error
        }
    }

    async fn recv(&self) -> Result<Inbound, Self::Error> {
        loop {
            let inbound = self.inner.recv().await?;
            if self.plugged.load(Ordering::Relaxed) {
                return Ok(inbound);
            }
        }
    }
}

/// [`spawn_node`] with the kill switch; storing `false` in the returned flag is the
/// peer dying.
fn spawn_killable(
    net: &Network,
    id: &str,
    peer: &str,
) -> (NodeId, Node<Unpluggable>, Group, Arc<AtomicBool>) {
    let me = NodeId::new(id);
    let plugged = Arc::new(AtomicBool::new(true));
    let transport = Unpluggable {
        inner: net.endpoint(me.clone()),
        plugged: Arc::clone(&plugged),
    };
    let node = Node::builder(me.clone(), transport)
        .seed(NodeId::new(peer))
        .config(brisk())
        .spawn();
    let group = node.join_group("s3cache");
    (me, node, group, plugged)
}

/// The resync handle production hands the apply loop, in miniature: it counts the
/// origin re-LIST it stands in for, and then — the half that matters — **owns the
/// affirmation**, read at call time and answered for that generation, exactly as
/// `CachingProxy::gap_resync_handle` does. A remediation that did not affirm would
/// leave the node correct and permanently out of service, which is the very bug the
/// watcher exists to prevent.
fn counting_resync(sync: &Arc<WriteSync>, count: &Arc<AtomicU64>) -> Arc<dyn Fn() + Send + Sync> {
    // Weak, so the handle the apply loop holds forever does not keep the `WriteSync`
    // (which owns that loop) alive in a cycle.
    let (sync, count) = (Arc::downgrade(sync), Arc::clone(count));
    Arc::new(move || {
        count.fetch_add(1, Ordering::Relaxed);
        let Some(sync) = sync.upgrade() else {
            return;
        };
        let generation = sync.resync_gen();
        tokio::spawn(async move { sync.affirm_resynced(generation).await });
    })
}

/// A leased node with its apply loop (and so its lapse watcher) running, its own
/// tiers, and a counter for the remediations its resync handle ran.
fn leased_reader(
    group: Group,
    me: NodeId,
    metrics: &Arc<Metrics>,
) -> (Arc<WriteSync>, TieredCache, Arc<AtomicU64>) {
    let cache = TieredCache::new(1024 * 1024, None, Arc::clone(metrics));
    let sync = Arc::new(attach(group, me, Consistency::Strong));
    let resyncs = Arc::new(AtomicU64::new(0));
    (sync, cache, resyncs)
}

/// Starts the apply loop of a [`leased_reader`] — separately, because one test has to
/// decide a race by starting the watcher after it.
fn start_watching(
    sync: &Arc<WriteSync>,
    cache: &TieredCache,
    resyncs: &Arc<AtomicU64>,
    metrics: &Arc<Metrics>,
) -> Index {
    let state: Index = Arc::new(RwLock::new(HashMap::new()));
    let local: LocalCache = cache.local();
    sync.start_apply(
        local,
        Arc::clone(&state),
        counting_resync(sync, resyncs),
        Arc::clone(metrics),
    );
    state
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

fn ckey(key: &str) -> (String, String) {
    ("bkt".to_owned(), key.to_owned())
}

/// Cache `body` under `key` the way a fill does: stamped current, so it is served
/// with no index consulted (see `CachingProxy::validated_get`).
async fn fill(cache: &TieredCache, key: &str, body: &'static [u8]) {
    let obj = cached(body);
    obj.mark_trusted(cache.suspect_gen());
    cache.insert(ckey(key), obj).await;
}

/// What the tiers hold for `key`, in the three states a recovery can leave it in:
/// `None` gone, `Some(false)` present but **suspect** (held, and refused by
/// `validated_get` until the LIST index proves it), `Some(true)` present and proved
/// (served straight back).
async fn held(cache: &TieredCache, key: &str) -> Option<bool> {
    let obj = cache.get(&ckey(key)).await?;
    Some(obj.trusted(cache.suspect_gen()))
}

/// One counter's current value, by its exposition name.
fn counter(metrics: &Metrics, name: &str) -> u64 {
    let text = metrics.prometheus_text();
    let prefix = format!("s3cache_{name} ");
    text.lines()
        .find_map(|line| line.strip_prefix(&prefix))
        .and_then(|value| value.parse().ok())
        .unwrap_or_else(|| panic!("{name} is not exposed:\n{text}"))
}

/// A bare write feed on `group` at an explicit epoch — the seam for two things a
/// [`WriteSync`] cannot express: a write published by a peer whose lease shell has
/// already been dropped, and a **fresh-epoch** frame, which is what a writer restart
/// looks like to a subscriber and the one deterministic way to force a real
/// [`PeerWrite::Gap`].
fn raw_feed(group: &Group, epoch: u64) -> WriteFeed<IndexEvent> {
    WriteFeed::new(
        group.clone(),
        std::num::NonZeroUsize::new(FEED_CAPACITY).expect("a non-zero ring"),
        encode_event,
    )
    .with_epoch(epoch)
}

/// A feed whose payloads this node's decoder rejects, so its writes are skipped
/// while its **head still advances** — the one shape that makes an advertised head
/// unreachable rather than merely late. [`PeerWrites`] steps its cursor past an
/// undecodable entry without emitting anything, so no [`Frontier`] watermark ever
/// covers it: a peer publishing an envelope this build cannot read.
fn undecodable_feed(group: &Group) -> WriteFeed<IndexEvent> {
    WriteFeed::new(
        group.clone(),
        std::num::NonZeroUsize::new(FEED_CAPACITY).expect("a non-zero ring"),
        |_: &IndexEvent| vec![WIRE_MAGIC, WIRE_MAGIC],
    )
}

/// The smallest event that indexes a key and invalidates a peer's copy of it.
fn put_event(key: &str) -> IndexEvent {
    IndexEvent {
        op: IndexOp::Put {
            size: Some(1),
            etag: None,
            content_type: None,
            storage_class: None,
        },
        bucket: "bkt".to_owned(),
        key: key.to_owned(),
        ts_us: to_micros(SystemTime::now()),
    }
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
    let state: Index = Arc::new(RwLock::new(HashMap::new()));
    let sync_b = Arc::new(attach(b_group, b_id, consistency));
    sync_b.start_apply(cache.local(), state.clone(), Arc::new(|| {}), metrics);
    let sync_a = attach(a_group, a_id, consistency);
    (sync_a, sync_b, state, cache)
}

/// Assert `peer` is on the far side of the exemption line — non-`Alive`, and held
/// that way for a full watch interval, so
/// [`LapseWatch::exempt`](crate::sync::recovery::LapseWatch::exempt) excludes it
/// and its later reap indicts nothing.
///
/// Every test that leans on a peer being exempt asserts this rather than assuming
/// it: the exemption is a *timing* property (the detector reached its verdict before
/// the lease expired), and a test whose timing slipped would otherwise pass for a
/// reason it never meant to check.
fn assert_exempt(group: &Group, peer: &NodeId) {
    let (status, for_how_long) = group
        .status_held_for(peer)
        .expect("the peer is still a member, tombstone and all");
    assert_ne!(
        status,
        Status::Alive,
        "the detector's verdict landed before the lease even expired"
    );
    assert!(
        for_how_long >= lapse_poll(TEST_LEASE),
        "and had been standing for a full watch interval ({for_how_long:?})"
    );
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
    assert!(parse_seeds("").is_empty());
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

/// Only the leased mode advertises [`CAP_LEASE`], because only it constructs a
/// lease set — a node advertising it without a running granter freezes every other
/// reader's confirmation until membership reaps it.
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

/// The settle window is computed per call, from the group's own config
/// and the membership it currently has to sweep — and never from a view
/// of one, which is what a node that has not yet met its peer sees.
#[tokio::test]
async fn the_settle_window_is_sized_per_call_and_floors_at_two_members() {
    let alone_net = Network::new();
    let alone_id = NodeId::new("win-alone");
    // The same timings the pair below runs, so the comparison at the end is about
    // membership size and nothing else.
    let alone_node = Node::builder(alone_id.clone(), alone_net.endpoint(alone_id))
        .config(brisk())
        .spawn();
    let alone = alone_node.join_group("s3cache");
    assert_eq!(alone.members().len(), 1, "nobody to gossip with");
    let cfg = alone.config();
    assert_eq!(
        settle_window(&alone),
        Duration::from_millis(cfg.detection_window_ms(2)),
        "a node that has not seen its peer still budgets for one"
    );
    assert!(
        settle_window(&alone) > Duration::from_millis(cfg.detection_window_ms(1)),
        "the floor is strictly longer than the group-of-one bound it replaces"
    );

    let net = Network::new();
    let (_a_id, _a_node, a_group) = spawn_node(&net, "win-a", "win-b");
    let (_b_id, _b_node, _b_group) = spawn_node(&net, "win-b", "win-a");
    let (_c_id, _c_node, _c_group) = spawn_node(&net, "win-c", "win-a");
    eventually(
        || a_group.members().len() == 3,
        "a three-member view to converge",
    )
    .await;
    assert!(
        settle_window(&a_group) > settle_window(&alone),
        "the window grows with the membership the detector has to sweep"
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
    let unprefixed = bincode::serialize(&event).expect("the event shape serializes");
    assert!(
        decode_event(&unprefixed).is_none(),
        "the decoder rejects bytes outside the current envelope"
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

    sync_a
        .publish_put("bkt", "obj", &written(42), &metrics)
        .await;
    eventually(
        || indexed_size(&state, "bkt", "obj") == Some(42),
        "put reaches the peer index",
    )
    .await;
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
        .publish_del("bkt", "obj", SystemTime::now(), &metrics)
        .await;
    eventually(
        || indexed_size(&state, "bkt", "obj").is_none(),
        "delete reaches the peer index",
    )
    .await;
}

/// The strict-LIST barrier: after a publish, `await_fresh` on the peer
/// returns true only once the event is actually applied.
#[tokio::test]
async fn await_fresh_reflects_the_publishers_head() {
    let net = Network::new();
    let (sync_a, sync_b, state, _cache) = wired_pair(&net);
    let metrics = Metrics::default();

    sync_a
        .publish_put("bkt", "fresh", &written(7), &metrics)
        .await;
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

    let receipt = sync_a
        .publish_put("bkt", "tok", &written(1), &metrics)
        .await;
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
/// and an unacked write stands this node's authoritative 404s down, because a peer
/// that did not apply our write in time is a peer whose writes we may equally be
/// missing. (In `strong` there is no cluster-wide clock to stand down: each reader's
/// own lease carries that honesty, on that reader's own clock.)
#[tokio::test]
async fn in_strong_acks_an_unacked_write_is_counted_and_stands_the_404_down() {
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

    let receipt = sync_b
        .publish_put("bkt", "unacked", &written(1), &metrics)
        .await;
    sync_b
        .ack_write(
            receipt.token,
            Duration::from_millis(200),
            "bkt",
            "unacked",
            &metrics,
        )
        .await;
    assert!(
        metrics
            .prometheus_text()
            .contains("\ns3cache_ack_timeouts 1\n"),
        "the ack timeout is counted"
    );
    assert!(
        !sync_b.may_answer_404(),
        "and an index miss is no longer an authoritative 404"
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
    let receipt = sync_a
        .publish_put("bkt", "fast", &written(9), &metrics)
        .await;
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
    let receipt = sync_a
        .publish_put("bkt", "lapsed", &written(1), &metrics)
        .await;
    sync_a
        .ack_write(
            receipt.token,
            Duration::from_millis(200),
            "bkt",
            "lapsed",
            &metrics,
        )
        .await;
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

    // And the cost is not recurring: the lapsed holder has left the wait set, so the
    // next write is back on the fast path.
    let started = Instant::now();
    let receipt = sync_a
        .publish_put("bkt", "after", &written(2), &metrics)
        .await;
    assert!(matches!(
        sync_a
            .wait_cluster_applied(receipt.token, Duration::from_secs(5))
            .await,
        WriteWait::Applied
    ));
    assert!(
        started.elapsed() < TEST_LEASE,
        "a lapsed peer is excused once, not once per write"
    );
}

/// The read-side licence, end to end: a booting reader holds none, an affirmed lease
/// is one, a gap takes it back — and only the resync that actually ran may give it
/// back. The last part is the generation guard, and it is what stops a resync that a
/// second gap superseded from re-opening a window nothing re-synchronized for.
#[tokio::test]
async fn the_readers_licence_is_its_lease_and_only_a_current_resync_restores_it() {
    let net = Network::new();
    let (_sync_a, sync_b, _state, _cache) =
        wired_pair_named(&net, ("gate-a", "gate-b"), Consistency::Strong);

    assert!(
        !sync_b.may_serve_local(),
        "a booting reader has missed every invalidation issued while it was down"
    );
    assert!(
        !sync_b.may_answer_404(),
        "and an index miss is certainly not an authoritative absence"
    );

    // The shell's reader boot guard latches after one detection window plus two
    // anti-entropy rounds, and declines the affirmation until it does — so the
    // affirmation polls rather than treating one refusal as a verdict.
    sync_b.affirm_resynced(sync_b.resync_gen()).await;
    assert!(sync_b.may_serve_local(), "an affirmed lease is the licence");
    assert!(sync_b.may_answer_404());

    sync_b.require_resync();
    assert!(
        !sync_b.may_serve_local(),
        "a gap takes it back immediately — not at the end of the remediation"
    );

    // Two gaps, one after the other: the first resync's affirmation must not speak
    // for the second's, which has not run yet.
    let superseded = sync_b.resync_gen();
    sync_b.require_resync();
    sync_b.affirm_resynced(superseded).await;
    assert!(
        !sync_b.may_serve_local(),
        "a superseded resync cannot affirm catch-up it never caught up on"
    );

    sync_b.affirm_resynced(sync_b.resync_gen()).await;
    assert!(
        sync_b.may_serve_local(),
        "and the resync that did run gives the licence back"
    );
}

/// **The lapse watcher**, and the failure it exists for: a lapse that arrives with no
/// write-feed gap behind it.
///
/// B does not gap A — it simply dies, with the feed quiet so no ring can overflow,
/// which is what a scale-in, a permanent loss and a rolling restart of a read-mostly
/// peer all look like from A. A's confirmation freezes on B's silence, its lease
/// lapses, and the lapse **latches**: the apply loop will never hear about it, so
/// without a watcher A serves every read from the origin for the rest of its life.
///
/// With one, the staged recovery runs — and what this test pins is everything it
/// does *not* do. B was already down when the lapse landed, so no live peer
/// vanished; nothing was written that A did not apply; the barrier proves the cache
/// untouched. The body survives with its proof intact and is served straight back
/// the moment the reap frees A's confirmation: no flush, no re-LIST, no distrust.
#[tokio::test]
async fn a_lapse_with_no_gap_keeps_the_cache_and_the_reader_recovers() {
    let net = Network::new();
    let (a_id, _a_node, a_group) = spawn_node(&net, "lapse-watch-a", "lapse-watch-b");
    let (b_id, _b_node, b_group, b_alive) = spawn_killable(&net, "lapse-watch-b", "lapse-watch-a");
    let metrics = Arc::new(Metrics::default());
    let (sync_a, cache, resyncs) = leased_reader(a_group, a_id, &metrics);
    let _sync_b = attach(b_group, b_id.clone(), Consistency::Strong);
    let _state = start_watching(&sync_a, &cache, &resyncs, &metrics);

    // A has to *hold* a lease before losing one can mean anything: B's grant, its own
    // warm-up window, and the boot affirmation.
    sync_a.affirm_resynced(sync_a.resync_gen()).await;
    assert!(sync_a.may_serve_local(), "A takes its serve-lease");
    // A body copy, proved current — so what the recovery leaves behind is observed
    // rather than inferred from the licence coming back.
    fill(&cache, "obj", b"body").await;
    assert_eq!(held(&cache, "obj").await, Some(true));

    b_alive.store(false, Ordering::Relaxed); // B dies. No gap, no event, no warning.

    eventually(
        || sync_a.lease_lapses() == 1,
        "A's lease to lapse on B's silence",
    )
    .await;
    assert!(
        !sync_a.may_serve_local(),
        "a lapsed reader serves nothing cached — that is the latch"
    );

    eventually(
        || counter(&metrics, "lapse_barrier_retains") == 1,
        "the staged recovery to prove the cache and keep it",
    )
    .await;
    assert_eq!(
        held(&cache, "obj").await,
        Some(true),
        "the body is still here AND still proved — served with no index lookup at all"
    );
    eventually(
        || sync_a.may_serve_local(),
        "A to affirm its way back into service once the reap frees its confirmation",
    )
    .await;

    assert_eq!(
        counter(&metrics, "lease_lapse_resyncs"),
        0,
        "the expensive arm never ran"
    );
    assert_eq!(counter(&metrics, "lapse_barrier_fallbacks"), 0);
    assert_eq!(
        counter(&metrics, "feed_gaps"),
        0,
        "and no write-feed gap was involved in any of it"
    );
    assert_eq!(
        resyncs.load(Ordering::Relaxed),
        0,
        "nothing was re-LISTed from the origin: there was nothing to re-learn"
    );
}

/// The barrier is not just proving a quiet cluster quiet: a **live writer** writes
/// straight through the recovery.
///
/// C stops granting, so A's lease lapses; B is healthy throughout and writes `k1`
/// during the lapse. The apply loop never stopped, so that invalidation lands as an
/// ordinary peer write — and the barrier is what turns the *ordering* into a
/// guarantee, because A does not serve again until B's advertised head has been
/// applied. The key B wrote is gone; the key it did not touch is still proved.
#[tokio::test]
async fn a_lapse_beside_a_live_writer_evicts_only_what_it_wrote() {
    let net = Network::new();
    let (a_id, _a_node, a_group) = spawn_node(&net, "live-a", "live-b");
    let (b_id, _b_node, b_group) = spawn_node(&net, "live-b", "live-a");
    let (c_id, _c_node, c_group, c_alive) = spawn_killable(&net, "live-c", "live-a");
    let metrics = Arc::new(Metrics::default());
    let (sync_a, cache, resyncs) = leased_reader(a_group, a_id, &metrics);
    let sync_b = attach(b_group, b_id.clone(), Consistency::Strong);
    let _sync_c = attach(c_group, c_id.clone(), Consistency::Strong);
    let state = start_watching(&sync_a, &cache, &resyncs, &metrics);

    // Both peers must be in A's granter roster *before* A affirms: one joining
    // afterwards would freeze the confirmation until its first grant arrived, which
    // is a lapse of its own and not the one under test.
    eventually(
        || sync_a.lease_holders().len() == 2,
        "A to adopt both peers' serve-leases",
    )
    .await;
    sync_a.affirm_resynced(sync_a.resync_gen()).await;
    assert!(sync_a.may_serve_local(), "A takes its serve-lease");
    assert_eq!(
        sync_a.lease_lapses(),
        0,
        "and holds it over a converged roster"
    );
    fill(&cache, "k1", b"stale").await;
    fill(&cache, "k2", b"untouched").await;

    c_alive.store(false, Ordering::Relaxed);
    eventually(
        || sync_a.lease_lapses() == 1,
        "A's lease to lapse on C's silence",
    )
    .await;

    // A lapse-era write, from the peer that never stopped being healthy.
    sync_b
        .publish_put("bkt", "k1", &written(1), &Metrics::default())
        .await;

    eventually(
        || counter(&metrics, "lapse_barrier_retains") == 1,
        "the staged recovery to barrier on the live writer's head and keep the rest",
    )
    .await;
    assert_eq!(
        indexed_size(&state, "bkt", "k1"),
        Some(1),
        "B's lapse-era write was applied, not merely waited on"
    );
    assert_eq!(
        held(&cache, "k1").await,
        None,
        "so the stale body went with it — the apply loop evicted exactly that key"
    );
    assert_eq!(
        held(&cache, "k2").await,
        Some(true),
        "and every key the writer did not touch kept its proof"
    );
    assert_eq!(counter(&metrics, "lapse_barrier_fallbacks"), 0);
    assert_eq!(counter(&metrics, "lease_lapse_resyncs"), 0);
    assert_eq!(
        resyncs.load(Ordering::Relaxed),
        0,
        "a live peer is exactly the case the cheap arm is for"
    );
}

/// **The headline shape**: a partition that heals.
///
/// A is cut off both ways — it hears nothing and is heard by nobody — so its
/// confirmation freezes and its lease lapses, while B writes `k1` on the other side.
/// The ring covers one write, so the heal produces no gap: this is a lapse and
/// nothing else, which is precisely the case that used to cost A its whole cache.
///
/// After the heal the staged recovery waits for the confirmation to move, settles,
/// finds nobody vanished, and barriers on B's advertised head. That barrier is what
/// makes the answer safe: `k1`'s stale body is gone because the write it barriered
/// on evicted it, and `k2` — which nothing wrote — is still there and still proved.
#[tokio::test]
async fn a_healed_partition_evicts_what_changed_and_keeps_what_did_not() {
    let net = Network::new();
    let (a_id, _a_node, a_group, a_plugged) = spawn_killable(&net, "part-a", "part-b");
    let (b_id, _b_node, b_group) = spawn_node(&net, "part-b", "part-a");
    let metrics = Arc::new(Metrics::default());
    let (sync_a, cache, resyncs) = leased_reader(a_group, a_id, &metrics);
    let sync_b = attach(b_group, b_id.clone(), Consistency::Strong);
    let state = start_watching(&sync_a, &cache, &resyncs, &metrics);

    sync_a.affirm_resynced(sync_a.resync_gen()).await;
    assert!(sync_a.may_serve_local(), "A takes its serve-lease");
    fill(&cache, "k1", b"stale").await;
    fill(&cache, "k2", b"untouched").await;

    a_plugged.store(false, Ordering::Relaxed); // the partition
    sync_b
        .publish_put("bkt", "k1", &written(1), &Metrics::default())
        .await;
    eventually(
        || sync_a.lease_lapses() == 1,
        "A's lease to lapse behind the partition",
    )
    .await;
    assert!(!sync_a.may_serve_local());
    a_plugged.store(true, Ordering::Relaxed); // the heal

    eventually(
        || counter(&metrics, "lapse_barrier_retains") == 1,
        "the staged recovery to complete on the far side of the heal",
    )
    .await;
    assert_eq!(
        counter(&metrics, "feed_gaps"),
        0,
        "the ring covered the partition, so this is a lapse and not a gap"
    );
    assert_eq!(
        indexed_size(&state, "bkt", "k1"),
        Some(1),
        "B's partition-era write was applied before the licence came back"
    );
    assert_eq!(held(&cache, "k1").await, None, "so its stale body is gone");
    assert_eq!(
        held(&cache, "k2").await,
        Some(true),
        "and the key the partition did not touch survived, proof and all"
    );
    assert_eq!(counter(&metrics, "lapse_barrier_fallbacks"), 0);
    assert_eq!(
        resyncs.load(Ordering::Relaxed),
        0,
        "no origin re-LIST: the barrier proved what a flush would have thrown away"
    );
}

/// A gap that lands **inside** the staged recovery supersedes it, and the two do not
/// each buy a remediation.
///
/// B's lease shell dies while its process does not, which freezes A's confirmation
/// on a granter that is neither publishing nor reapable — so A's lease lapses and
/// the recovery parks in stage 1 with nothing to wait for. Then B's feed comes back
/// at a fresh epoch, which is exactly what a writer restart looks like to a
/// subscriber: a real [`PeerWrite::Gap`]. The gap's remediation is strictly stronger
/// than anything the recovery was about to conclude, so the recovery yields on the
/// generation check rather than racing it — and is counted as neither a retain nor a
/// fallback, because it concluded nothing.
#[tokio::test]
async fn a_gap_during_the_recovery_supersedes_it_and_remediates_once() {
    let net = Network::new();
    let (a_id, _a_node, a_group) = spawn_node(&net, "super-a", "super-b");
    let (b_id, _b_node, b_group) = spawn_node(&net, "super-b", "super-a");
    let metrics = Arc::new(Metrics::default());
    let (sync_a, cache, resyncs) = leased_reader(a_group, a_id, &metrics);
    let sync_b = attach(b_group.clone(), b_id.clone(), Consistency::Strong);
    let state = start_watching(&sync_a, &cache, &resyncs, &metrics);

    sync_a.affirm_resynced(sync_a.resync_gen()).await;
    eventually(
        || sync_a.lease_holders() == [b_id.clone()],
        "A to adopt B's serve-lease",
    )
    .await;
    assert!(sync_a.may_serve_local(), "A takes its serve-lease");
    fill(&cache, "k1", b"body").await;

    // B's first feed life, so A holds a cursor an epoch bump can invalidate.
    raw_feed(&b_group, 1).publish(&put_event("seed")).await;
    eventually(
        || indexed_size(&state, "bkt", "seed") == Some(1),
        "A to apply B's first-life write",
    )
    .await;

    drop(sync_b); // B stops granting; B's process keeps gossiping.
    eventually(
        || sync_a.lease_lapses() == 1,
        "A's lease to lapse on B's frozen grants",
    )
    .await;
    // Give the watch its stage-0 look, so the gap lands *inside* the recovery rather
    // than before it — the other order is the interlock test below.
    tokio::time::sleep(lapse_poll(TEST_LEASE) * 3).await;
    let staged = sync_a.resync_gen();
    assert_eq!(
        staged, 1,
        "the recovery stood the licence down and owns one generation"
    );

    // The gap. A different key, so the body under test is left for the remediation
    // to speak about rather than evicted by the write that triggered it.
    raw_feed(&b_group, 2).publish(&put_event("restarted")).await;

    eventually(
        || counter(&metrics, "feed_gaps") == 1,
        "the apply loop to see the fresh epoch as a gap",
    )
    .await;
    eventually(
        || resyncs.load(Ordering::Relaxed) == 1,
        "the gap's remediation to run",
    )
    .await;
    // Comfortably more than one stage-1 poll: long enough for the recovery to reach
    // its next generation check and yield.
    tokio::time::sleep(AFFIRM_POLL * 8).await;

    assert_eq!(
        sync_a.resync_gen(),
        staged + 1,
        "the gap started one generation of its own, and the recovery started none"
    );
    assert_eq!(
        resyncs.load(Ordering::Relaxed),
        1,
        "one remediation for the two triggers, not two"
    );
    assert_eq!(
        held(&cache, "k1").await,
        Some(false),
        "the gap kept every body and distrusted it: present in the tier, refused until \
             the re-LISTed index proves it"
    );
    assert_eq!(
        counter(&metrics, "lapse_barrier_retains"),
        0,
        "the superseded recovery concluded nothing"
    );
    assert_eq!(
        counter(&metrics, "lapse_barrier_fallbacks"),
        0,
        "and yielded rather than falling back, so it is counted as neither"
    );
    assert_eq!(counter(&metrics, "lease_lapse_resyncs"), 0);
}

/// **The crux.** A peer that was `Alive` when the lapse landed and is *gone* by the
/// time the barrier runs took its feed frame with it — so there is no head to sample
/// and no way to tell whether it wrote. The cheap arm is refused.
///
/// B's lease shell dies while its process stays up, which freezes A's confirmation
/// on a granter A still sees as perfectly `Alive` — that is what puts B in the
/// recovery's accounting instead of its exemption list. Only then does B really die,
/// having published a write A can never receive; membership reaps it, and with it
/// every trace of that write. The recovery finds a peer it cannot account for and
/// falls back to the full remediation, which is exactly the right answer: A's copy
/// of `k1` is stale, and only the re-LISTed index can say so.
#[tokio::test]
async fn a_peer_that_vanishes_between_the_lapse_and_the_barrier_forces_the_fallback() {
    let net = Network::new();
    let (a_id, _a_node, a_group) = spawn_node(&net, "crux-a", "crux-b");
    let (b_id, _b_node, b_group, b_alive) = spawn_killable(&net, "crux-b", "crux-a");
    let metrics = Arc::new(Metrics::default());
    let (sync_a, cache, resyncs) = leased_reader(a_group.clone(), a_id, &metrics);
    let sync_b = attach(b_group.clone(), b_id.clone(), Consistency::Strong);
    let _state = start_watching(&sync_a, &cache, &resyncs, &metrics);

    sync_a.affirm_resynced(sync_a.resync_gen()).await;
    eventually(
        || sync_a.lease_holders() == [b_id.clone()],
        "A to adopt B's serve-lease",
    )
    .await;
    assert!(sync_a.may_serve_local(), "A takes its serve-lease");
    fill(&cache, "k1", b"stale").await;

    drop(sync_b); // B stops granting; B's process keeps gossiping.
    eventually(
        || sync_a.lease_lapses() == 1,
        "A's lease to lapse on B's frozen grants",
    )
    .await;
    assert_eq!(
        a_group.member_status(&b_id),
        Some(Status::Alive),
        "B is ALIVE when the lapse lands, so nothing exempts it from the accounting"
    );
    // Let the recovery take its stage-0 snapshot while B is still live.
    tokio::time::sleep(lapse_poll(TEST_LEASE) * 3).await;

    // Now B really dies — having published a write A can never receive.
    b_alive.store(false, Ordering::Relaxed);
    raw_feed(&b_group, 1).publish(&put_event("k1")).await;

    eventually(
        || counter(&metrics, "lapse_barrier_fallbacks") == 1,
        "the vanished-peer check to refuse the cheap arm",
    )
    .await;
    assert!(
        !a_group.statuses().iter().any(|(peer, _)| *peer == b_id),
        "B is gone from membership: reaped, feed frame and all"
    );
    assert_eq!(
        counter(&metrics, "lapse_barrier_retains"),
        0,
        "nothing was ever proved"
    );
    assert_eq!(
        counter(&metrics, "lease_lapse_resyncs"),
        1,
        "the fallback IS the full remediation, and is counted as one"
    );
    assert_eq!(
        resyncs.load(Ordering::Relaxed),
        1,
        "which re-LISTs the index exactly once"
    );
    assert_eq!(
        counter(&metrics, "feed_gaps"),
        0,
        "and none of it arrived as a gap"
    );
    assert_eq!(
        held(&cache, "k1").await,
        Some(false),
        "the body A could not account for is held and no longer proved — refused until \
             the re-LISTed index says otherwise"
    );
    eventually(
        || sync_a.may_serve_local(),
        "A back in service once the reap frees its confirmation",
    )
    .await;
    assert_eq!(
        held(&cache, "k1").await,
        Some(false),
        "and it stays suspect after the licence returns: the licence is not the proof"
    );
}

/// **The second hinge**, and the one a roster-wide min cannot see.
///
/// Three nodes, so A's confirmation is a min over *two* granters. C dies first, which
/// freezes its grant and leaves B — still granting for a moment longer — strictly
/// above it. Then B's lease shell dies while B's process keeps gossiping, so B is a
/// granter that is `Alive`, in the roster, and frozen. A's lease lapses on the two of
/// them.
///
/// What happens next is the test. Membership reaps C, and the **min moves** — up to
/// B's frozen grant — with nobody having re-granted anything. "The confirmation
/// advanced" is exactly the proof stage 1 used to rest on, and here it is false:
/// nothing else objects either, because C is exempt (down since before the lapse), B
/// is present, and a granter that has published no writes has no advertised head to
/// barrier on. The old stage 1 walked through all of that into a retain it had not
/// earned — while B, for all this node can tell, spent the lapse completing writes
/// against A's expired lease.
///
/// Stage 1 now reads each granter's grant on its own, so it is still waiting on B
/// when B finally dies too; B's reap takes it out of the roster and hands it to stage
/// 3, which refuses the cheap arm for a peer that was live at the lapse and gone
/// before the affirmation.
///
/// The discriminating assertion is the **negative** one in the middle: put stage 1
/// back on the min and `lapse_barrier_retains` is already 1 there.
#[tokio::test]
async fn a_granter_frozen_behind_a_reaped_min_holder_refuses_the_cheap_arm() {
    let net = Network::new();
    let (a_id, _a_node, a_group) = spawn_node(&net, "hinge-a", "hinge-b");
    let (b_id, _b_node, b_group, b_alive) = spawn_killable(&net, "hinge-b", "hinge-a");
    let (c_id, _c_node, c_group, c_alive) = spawn_killable(&net, "hinge-c", "hinge-a");
    let metrics = Arc::new(Metrics::default());
    let (sync_a, cache, resyncs) = leased_reader(a_group.clone(), a_id, &metrics);
    let sync_b = attach(b_group.clone(), b_id.clone(), Consistency::Strong);
    let _sync_c = attach(c_group, c_id.clone(), Consistency::Strong);
    let _state = start_watching(&sync_a, &cache, &resyncs, &metrics);

    // Two granters, both actually granting, before A affirms: a min over one is the
    // shape this test exists to *not* be.
    eventually(
        || sync_a.lease_granted_by(&b_id).is_some() && sync_a.lease_granted_by(&c_id).is_some(),
        "both peers to publish a grant of A's serve-lease",
    )
    .await;
    sync_a.affirm_resynced(sync_a.resync_gen()).await;
    assert!(sync_a.may_serve_local(), "A takes its serve-lease");
    fill(&cache, "k1", b"stale").await;

    // C dies: from here it is the min-holder, frozen at whatever it last adopted.
    c_alive.store(false, Ordering::Relaxed);
    eventually(
        || sync_a.lease_granted_by(&b_id) > sync_a.lease_granted_by(&c_id),
        "B's grant to outrun the dead C's — C pins the min, B sits above it",
    )
    .await;
    assert_eq!(
        sync_a.lease_lapses(),
        0,
        "B has to freeze before the lapse, so the recovery snapshots it already frozen"
    );
    drop(sync_b); // B stops granting; B's process keeps gossiping.

    eventually(
        || sync_a.lease_lapses() == 1,
        "A's lease to lapse on two frozen granters",
    )
    .await;
    // C's exemption is the precondition for the negative assertion below: an
    // unexempt C would have its own reap refuse the cheap arm, and the test would
    // prove nothing about the min.
    assert_exempt(&a_group, &c_id);
    // Past the watch's first look, so the recovery has taken its stage-0 snapshot and
    // B's last in-flight grant map has landed. Nothing moves B's grant after this.
    tokio::time::sleep(lapse_poll(TEST_LEASE) * 2).await;
    let frozen = sync_a.lease_granted_by(&b_id);
    assert!(
        frozen.is_some(),
        "B's grant is still standing in A's view — frozen, not gone"
    );

    eventually(
        || !a_group.statuses().iter().any(|(peer, _)| *peer == c_id),
        "membership to reap C — the event that moves the roster-wide min",
    )
    .await;
    // Everything a min-based stage 1 waits for has now happened. Give a recovery
    // resting on it every chance to settle, barrier on nothing, and conclude.
    tokio::time::sleep(TEST_LEASE * 3).await;
    assert_eq!(
        sync_a.lease_granted_by(&b_id),
        frozen,
        "B never re-granted: the min moved because C left, not because B spoke"
    );
    assert_eq!(
        counter(&metrics, "lapse_barrier_retains"),
        0,
        "and a min that moved for a reap is not proof that every granter re-granted"
    );
    assert_eq!(
        counter(&metrics, "lapse_barrier_fallbacks"),
        0,
        "the recovery has concluded nothing either way — it is still waiting on B"
    );
    assert_eq!(resyncs.load(Ordering::Relaxed), 0);

    // B goes too. Its grant never moved, so stage 1 is still on it, and the reap is
    // what finally answers — by taking B out of the roster and handing it to stage 3.
    b_alive.store(false, Ordering::Relaxed);
    eventually(
        || counter(&metrics, "lapse_barrier_fallbacks") == 1,
        "the frozen granter's own reap to refuse the cheap arm",
    )
    .await;
    assert_eq!(
        counter(&metrics, "lapse_barrier_retains"),
        0,
        "nothing was ever proved"
    );
    assert_eq!(
        counter(&metrics, "lease_lapse_resyncs"),
        1,
        "the fallback IS the full remediation, and is counted as one"
    );
    assert_eq!(
        resyncs.load(Ordering::Relaxed),
        1,
        "which re-LISTs the index exactly once"
    );
    assert_eq!(
        counter(&metrics, "feed_gaps"),
        0,
        "and none of it arrived as a gap"
    );
    assert_eq!(
        held(&cache, "k1").await,
        Some(false),
        "the body A could not account for is held and no longer proved"
    );
}

/// **The barrier itself**, isolated: a peer whose advertised head this node can
/// never apply is a proof that never arrives, and the recovery must fall back rather
/// than wait forever or assume.
///
/// B publishes an envelope A's decoder rejects while A is partitioned. The apply
/// loop steps its cursor past it — no event, no watermark — so B's head is
/// advertised and permanently unreachable. Nothing else in the recovery objects: the
/// confirmation moves on the heal, the settle passes, nobody vanished. Only stage 4
/// can catch this, and what it does about it is the fallback.
///
/// This is also the test that *discriminates* the barrier: with stage 4 removed
/// every other lapse test still passes (the writes had already applied by the time
/// they looked), and this one retains a cache it never proved.
#[tokio::test]
async fn a_head_this_node_can_never_apply_times_the_barrier_out_into_the_fallback() {
    let net = Network::new();
    let (a_id, _a_node, a_group, a_plugged) = spawn_killable(&net, "unreach-a", "unreach-b");
    let (b_id, _b_node, b_group) = spawn_node(&net, "unreach-b", "unreach-a");
    let metrics = Arc::new(Metrics::default());
    let (sync_a, cache, resyncs) = leased_reader(a_group, a_id, &metrics);
    let _sync_b = attach(b_group.clone(), b_id.clone(), Consistency::Strong);
    let _state = start_watching(&sync_a, &cache, &resyncs, &metrics);

    sync_a.affirm_resynced(sync_a.resync_gen()).await;
    assert!(sync_a.may_serve_local(), "A takes its serve-lease");
    fill(&cache, "k1", b"stale").await;

    a_plugged.store(false, Ordering::Relaxed); // the partition
    undecodable_feed(&b_group).publish(&put_event("k1")).await;
    eventually(
        || sync_a.lease_lapses() == 1,
        "A's lease to lapse behind the partition",
    )
    .await;
    a_plugged.store(true, Ordering::Relaxed); // the heal

    eventually(
        || counter(&metrics, "lapse_barrier_fallbacks") == 1,
        "the barrier to run out of time on a head it cannot reach",
    )
    .await;
    assert_eq!(
        counter(&metrics, "lapse_barrier_retains"),
        0,
        "an unreachable head is never a proof"
    );
    assert_eq!(counter(&metrics, "lease_lapse_resyncs"), 1);
    assert_eq!(resyncs.load(Ordering::Relaxed), 1);
    assert_eq!(
        counter(&metrics, "feed_applied"),
        0,
        "nothing was applied — that is precisely why the head stayed out of reach"
    );
    assert_eq!(
        held(&cache, "k1").await,
        Some(false),
        "so the body is held and distrusted, not served on an unproved licence"
    );
}

/// The other side of the crux: a peer that was **already** down before the lapse is
/// exempt, and its reap mid-recovery indicts nothing.
///
/// This is the same reap that forces the fallback above, arriving on a peer the
/// recovery never counted — so the barrier path proceeds and the cache survives. Its
/// precondition is asserted rather than assumed: the failure detector had reached
/// its verdict a full watch interval before the lease expired.
#[tokio::test]
async fn a_peer_down_since_before_the_lapse_is_exempt_from_the_vanished_check() {
    let net = Network::new();
    let (a_id, _a_node, a_group) = spawn_node(&net, "exempt-a", "exempt-b");
    let (b_id, _b_node, b_group, b_alive) = spawn_killable(&net, "exempt-b", "exempt-a");
    let metrics = Arc::new(Metrics::default());
    let (sync_a, cache, resyncs) = leased_reader(a_group.clone(), a_id, &metrics);
    let _sync_b = attach(b_group, b_id.clone(), Consistency::Strong);
    let _state = start_watching(&sync_a, &cache, &resyncs, &metrics);

    sync_a.affirm_resynced(sync_a.resync_gen()).await;
    assert!(sync_a.may_serve_local(), "A takes its serve-lease");
    fill(&cache, "k1", b"body").await;

    b_alive.store(false, Ordering::Relaxed);
    eventually(
        || sync_a.lease_lapses() == 1,
        "A's lease to lapse on B's silence",
    )
    .await;
    assert_exempt(&a_group, &b_id);

    eventually(
        || counter(&metrics, "lapse_barrier_retains") == 1,
        "the barrier path to proceed despite the reap",
    )
    .await;
    assert!(
        !a_group.statuses().iter().any(|(peer, _)| *peer == b_id),
        "B was reaped during the recovery — the exact event that indicts a live peer"
    );
    assert_eq!(
        counter(&metrics, "lapse_barrier_fallbacks"),
        0,
        "an exempt peer's reap indicts nothing"
    );
    assert_eq!(counter(&metrics, "lease_lapse_resyncs"), 0);
    assert_eq!(resyncs.load(Ordering::Relaxed), 0);
    assert_eq!(held(&cache, "k1").await, Some(true));
}

/// The interlock: a lapse a gap has **already** stood the lease down for buys one
/// remediation, not two.
///
/// The order under test is the racing one — the lease lapses, then a gap arrives
/// before anything has remediated the lapse — and the race is *decided* here rather
/// than sampled: the apply loop (and with it the watcher) starts after the gap's
/// stand-down, so the watcher's first look is guaranteed to be the one that must
/// yield. It yields because
/// [`ResyncGate::require_resync`](crate::sync::recovery::ResyncGate::require_resync) recorded the lapse
/// count it covers, and the distrust and origin re-LIST that follow a stand-down
/// cover every lapse observed before it. Without that watermark the watcher would
/// distrust and re-LIST a second time, on top of a resync already in flight.
///
/// The reverse order is deliberately *not* deduplicated: a gap that lands after the
/// watcher recovered is independent proof of missed writes and is entitled to its
/// own resync.
#[tokio::test]
async fn a_lapse_a_gap_already_owns_is_not_remediated_twice() {
    let net = Network::new();
    let (a_id, _a_node, a_group) = spawn_node(&net, "dedupe-a", "dedupe-b");
    let (b_id, _b_node, b_group, b_alive) = spawn_killable(&net, "dedupe-b", "dedupe-a");
    let metrics = Arc::new(Metrics::default());
    let (sync_a, cache, resyncs) = leased_reader(a_group, a_id, &metrics);
    let _sync_b = attach(b_group, b_id.clone(), Consistency::Strong);

    sync_a.affirm_resynced(sync_a.resync_gen()).await;
    assert!(sync_a.may_serve_local(), "A takes its serve-lease");
    b_alive.store(false, Ordering::Relaxed);
    eventually(
        || sync_a.lease_lapses() == 1,
        "A's lease to lapse on B's silence",
    )
    .await;

    // The gap arm's stand-down, verbatim — a gap landing on the same lapse.
    let generation = sync_a.resync_gen();
    sync_a.require_resync();
    assert_eq!(
        sync_a.resync_gen(),
        generation + 1,
        "the gap started a generation of its own"
    );

    // Now the watcher starts, with the lapse already counted and already covered.
    let _state = start_watching(&sync_a, &cache, &resyncs, &metrics);
    tokio::time::sleep(lapse_poll(TEST_LEASE) * 5).await;

    assert_eq!(
        resyncs.load(Ordering::Relaxed),
        0,
        "the watcher yielded: the gap's remediation already owns this lapse"
    );
    assert_eq!(
        sync_a.resync_gen(),
        generation + 1,
        "and it started no generation of its own to affirm against"
    );
    assert_eq!(
        counter(&metrics, "lease_lapse_resyncs"),
        0,
        "a lapse remediated by the gap is not also counted as the watcher's"
    );
    assert_eq!(
        counter(&metrics, "lapse_barrier_retains"),
        0,
        "and the watcher concluded nothing about the cache either way"
    );
    assert_eq!(counter(&metrics, "lapse_barrier_fallbacks"), 0);
}

/// The modes that hold no lease answer the licence questions the way they always
/// did, and nothing in the lease path can silently change that.
#[tokio::test]
async fn an_unleased_mode_keeps_its_own_licence_and_affirms_vacuously() {
    let net = Network::new();
    let (_sync_a, sync_b, _state, _cache) =
        wired_pair_named(&net, ("bnd-a", "bnd-b"), Consistency::Bounded);
    assert!(
        sync_b.may_serve_local(),
        "bounded serves local state always — freshness is the bound"
    );
    // No window to open, so the affirmation must return rather than poll to its
    // deadline. A budget far under AFFIRM_DEADLINE is the assertion.
    tokio::time::timeout(
        Duration::from_millis(500),
        sync_b.affirm_resynced(sync_b.resync_gen()),
    )
    .await
    .expect("an unleased affirmation is immediate");
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
