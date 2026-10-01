use std::sync::{Arc, OnceLock};
use std::time::Duration;

use groupnet::consistency::LeaseConfig;
use groupnet::core::{Config, NodeId};
use groupnet::runtime::{Group, NamedSeeds, Node, SeedEvent, SystemResolver};
use groupnet::transport::Transport;
use groupnet::transport::udp::UdpTransport;
use tracing::{info, warn};

use crate::sync::coherence::{Consistency, DEAD_TIMEOUT_FLOOR_MS, DEFAULT_LEASE_MS, WriteSync};
use crate::sync::readiness::UnmetSeeds;

/// Attempts to resolve a gossip seed's DNS name before it is reported
/// unresolvable — a `StatefulSet` peer's record can lag its own startup.
const SEED_RESOLVE_ATTEMPTS: u32 = 30;

/// The wait between those first attempts.
const SEED_RETRY: Duration = Duration::from_secs(1);

/// How often each seed is re-resolved, forever. Pod IPs churn on restarts
/// and the seed's DNS record follows; re-resolution is the recovery channel
/// that works even when gossip cannot deliver the new address (a rebooted
/// peer is deaf to us until OUR datagrams come from an address it knows).
const SEED_REFRESH: Duration = Duration::from_secs(15);

/// The seed resolution [`WriteSync::new`] runs: the system resolver, on
/// s3cache's startup window and refresh cadence.
fn seed_resolution() -> NamedSeeds {
    NamedSeeds::new(SystemResolver)
        .retry_interval(SEED_RETRY)
        .startup_attempts(SEED_RESOLVE_ATTEMPTS)
        .refresh_interval(SEED_REFRESH)
}

/// Logs what the seed resolver did and releases readiness for a seed that
/// outlived its startup window: no pod holds that name, let alone an index.
fn observe_seed(event: &SeedEvent, unmet: &OnceLock<Arc<UnmetSeeds>>) {
    match event {
        SeedEvent::Resolved {
            node,
            addr,
            previous: Some(_),
            ..
        } => info!("gossip seed `{node}` moved to {addr}; re-registering"),
        SeedEvent::Resolved { previous: None, .. } => {}
        SeedEvent::Unresolved { node, name, error } => {
            warn!("gossip seed `{node}={name}` not resolving yet ({error}); will keep trying");
            if let Some(unmet) = unmet.get() {
                unmet.give_up(node);
            }
        }
    }
}

/// Everything the gossip layer needs, independent of where it came from.
/// [`from_env`] fills it from `S3CACHE_GOSSIP_*`; anything driving two nodes in
/// one process (tests) constructs it directly, since mutating the environment
/// to configure a node is neither safe nor parallel-friendly in edition 2024.
pub struct SyncConfig {
    /// UDP address to bind the gossip transport to (`S3CACHE_GOSSIP_BIND`).
    pub bind: String,
    /// The address peers should reach this node on; the bound address when
    /// `None` (`S3CACHE_GOSSIP_ADVERTISE`).
    pub advertise: Option<String>,
    /// Statically-addressed peers as `(node id, host:port)`. Every other peer
    /// resolves itself through gossiped advertisements, so only seeds need
    /// static addressing (`S3CACHE_GOSSIP_SEEDS`).
    pub seeds: Vec<(String, String)>,
    /// This node's identity in the cluster (the pod name, in the chart).
    pub node_id: String,
    /// How much coherence the cluster pays for (`S3CACHE_CONSISTENCY`).
    pub consistency: Consistency,
    /// The coherence-lease duration `D` in milliseconds (`S3CACHE_LEASE_MS`),
    /// used only by [`Consistency::Strong`]. [`DEFAULT_LEASE_MS`] is the value
    /// the environment defaults to, and the one a caller with no opinion wants.
    pub lease_ms: u64,
}

impl WriteSync {
    /// Bind the gossip transport, join the cluster group and attach the write
    /// feed. `None` when the bind address is unusable — gossip is optional, and
    /// a node that cannot join is a strict single node, not a dead one.
    /// Start the apply loop with `start_apply` once the
    /// local cache exists (the proxy does this in `start_coherence`).
    ///
    /// # The one tuned protocol knob, and what it buys
    ///
    /// `dead_timeout_ms` is pulled down from groupnet's 10s default to the lease
    /// duration `D` (floored at `DEAD_TIMEOUT_FLOOR_MS`), uniformly in every mode so a
    /// mixed fleet has one membership timing rather than two. The tuning **is** part of
    /// the lease migration, not decoration around it:
    ///
    /// * A reader's confirmation is a min over its whole roster, and only a
    ///   **departure** removes a member from it: a reap does not, because an
    ///   asymmetric partition that outlives the reap horizon reaps a writer that is
    ///   still writing. So one `CAP_LEASE` member that stops publishing grants without
    ///   departing — crashed, hung, partitioned — keeps *every* reader serving from the
    ///   origin until it grants again (`read_absent_granter_bypasses` and a log line
    ///   name it). A planned stop departs and is dropped at once.
    /// * What the tuning still buys is membership's own timing: suspicion, the `Dead`
    ///   verdict and the reap that the write-feed apply loop, readiness and the fleet
    ///   bootstrap follow.
    ///
    /// What it costs is the other end of the same horizon: `2 × dead_timeout_ms` is also
    /// how long a returning node's entries stay recoverable by a digest, so a partition
    /// outliving ~4s lands on the write-feed **gap** path instead of reconciling —
    /// distrust every cached body, re-LIST from the origin. That is not a regression to
    /// work around; the origin is the authority this index caches, and the gap path is
    /// s3cache's standing remedy for "this node provably missed writes".
    pub async fn new(cfg: SyncConfig) -> Option<Self> {
        Self::bind(cfg, seed_resolution()).await
    }

    /// [`new`](Self::new) with seeds resolved by `resolution` (resolver and
    /// timing); the seeds themselves and the observer come from here.
    pub(crate) async fn bind(cfg: SyncConfig, resolution: NamedSeeds) -> Option<Self> {
        let me = NodeId::new(cfg.node_id.as_str());
        let transport = match UdpTransport::bind(me.clone(), cfg.bind.as_str()).await {
            Ok(transport) => transport,
            Err(error) => {
                let bind = &cfg.bind;
                warn!("gossip disabled: cannot bind `{bind}`: {error}");
                return None;
            }
        };
        let lease = lease_config(cfg.lease_ms);
        let advertise = cfg
            .advertise
            .or_else(|| transport.local_addr().ok().map(|addr| addr.to_string()));
        let seeds: Vec<NodeId> = cfg
            .seeds
            .iter()
            .filter(|(id, _)| *id != cfg.node_id) // a pod seeding itself (uniform config) is a no-op
            .map(|(id, _)| NodeId::new(id.as_str()))
            .collect();
        // Seeds resolve off the startup path (DNS for a just-starting peer may
        // lag, and a slow resolver must not delay serving), inside groupnet:
        // each joins the seed set now and reaches the transport once resolved.
        // The readiness set is filled below, before this function yields, and
        // the first `Unresolved` comes a whole startup window later.
        let unmet = Arc::new(OnceLock::new());
        let observed = Arc::clone(&unmet);
        let mut resolution = resolution.on_event(move |event| observe_seed(event, &observed));
        for (id, addr) in &cfg.seeds {
            resolution = resolution.seed(NodeId::new(id.as_str()), addr.as_str());
        }
        let mut builder = Node::builder(me.clone(), transport)
            .config(gossip_config(cfg.lease_ms))
            .named_seeds(resolution);
        if let Some(advertise) = advertise {
            builder = builder.advertise_addr(advertise);
        }
        let node = builder.spawn();
        let group = node.join_group("s3cache");
        let (bind, node_id, mode) = (&cfg.bind, &cfg.node_id, cfg.consistency.label());
        let lease_ms = cfg.lease_ms;
        info!(
            "gossip coherence bound on `{bind}` as `{node_id}` (consistency: {mode}, lease: {lease_ms}ms)"
        );
        let sync = WriteSync::attach(group, me, cfg.consistency, lease, Some(Box::new(node)));
        let _ = unmet.set(sync.expect_seeds(seeds));
        Some(sync)
    }

    /// Join the cluster group as `node_id` over an already-built groupnet
    /// `transport`, seeded with `seeds`, with exactly the membership tuning and
    /// coherence lease [`WriteSync::new`] runs. `new` is this over UDP plus seed
    /// address resolution; any other transport (the in-memory one, for a whole
    /// fleet in one process) resolves its own peers.
    ///
    /// # Panics
    /// Outside a Tokio runtime: the node spawns its tasks here.
    pub fn over_transport<T: Transport>(
        transport: T,
        node_id: &str,
        seeds: &[&str],
        consistency: Consistency,
        lease_ms: u64,
    ) -> Self {
        let me = NodeId::new(node_id);
        let mut builder = Node::builder(me.clone(), transport).config(gossip_config(lease_ms));
        for seed in seeds.iter().filter(|seed| **seed != node_id) {
            builder = builder.seed(NodeId::new(*seed));
        }
        let node = builder.spawn();
        let group = node.join_group("s3cache");
        let sync = WriteSync::attach(
            group,
            me,
            consistency,
            lease_config(lease_ms),
            Some(Box::new(node)),
        );
        sync.expect_seeds(
            seeds
                .iter()
                .filter(|seed| **seed != node_id)
                .map(|seed| NodeId::new(*seed)),
        );
        sync
    }

    /// The gossip group this node's feed, leases and membership ride on, for
    /// a read of its roster.
    #[must_use]
    pub fn group(&self) -> &Group {
        &self.group
    }
}

/// The one tuned membership knob (see [`WriteSync::new`]).
fn gossip_config(lease_ms: u64) -> Config {
    Config {
        dead_timeout_ms: lease_ms.max(DEAD_TIMEOUT_FLOOR_MS),
        ..Config::default()
    }
}

fn lease_config(lease_ms: u64) -> LeaseConfig {
    let lease = LeaseConfig::for_duration(Duration::from_millis(lease_ms));
    debug_assert!(
        lease.validate().is_ok(),
        "S3CACHE_LEASE_MS outside the lease tier's envelope: {:?}",
        lease.validate()
    );
    lease
}

/// Build the gossip node and write feed from `S3CACHE_GOSSIP_*`, or `None`
/// when `S3CACHE_GOSSIP_BIND` is unset (single-node: the sole writer is
/// already strict). A thin read of the environment over [`WriteSync::new`].
pub async fn from_env(node_name: &str) -> Option<WriteSync> {
    let cfg = SyncConfig {
        bind: env_var("S3CACHE_GOSSIP_BIND")?,
        advertise: env_var("S3CACHE_GOSSIP_ADVERTISE"),
        seeds: parse_seeds(&env_var("S3CACHE_GOSSIP_SEEDS").unwrap_or_default()),
        node_id: node_name.to_owned(),
        consistency: Consistency::parse(&env_var("S3CACHE_CONSISTENCY").unwrap_or_default()),
        lease_ms: parse_lease_ms(env_var("S3CACHE_LEASE_MS").as_deref()),
    };
    WriteSync::new(cfg).await
}

/// Read the `S3CACHE_LEASE_MS` spelling: the coherence-lease duration `D`, in
/// milliseconds. Anything unusable — unset, not a number, or zero, which is the
/// engine's "never expires" and so precisely the stale claim the tier exists to
/// prevent — falls back to [`DEFAULT_LEASE_MS`], loudly when it was set at all.
pub(super) fn parse_lease_ms(raw: Option<&str>) -> u64 {
    let Some(raw) = raw else {
        return DEFAULT_LEASE_MS;
    };
    match raw.trim().parse::<u64>() {
        Ok(ms) if ms > 0 => ms,
        _ => {
            warn!("unusable S3CACHE_LEASE_MS `{raw}`; using {DEFAULT_LEASE_MS}ms");
            DEFAULT_LEASE_MS
        }
    }
}

/// An environment variable, treating "set but empty" as unset — a Helm value
/// that renders to `""` (an unset optional knob) must read as absent.
fn env_var(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|value| !value.is_empty())
}

/// Comma-separated `id=host:port` seeds; malformed entries are dropped loudly
/// rather than taking gossip down.
pub(super) fn parse_seeds(raw: &str) -> Vec<(String, String)> {
    raw.split(',')
        .map(str::trim)
        .filter(|seed| !seed.is_empty())
        .filter_map(|seed| {
            let Some((id, addr)) = seed.split_once('=') else {
                warn!("ignoring malformed gossip seed `{seed}` (want id=host:port)");
                return None;
            };
            Some((id.to_owned(), addr.to_owned()))
        })
        .collect()
}
