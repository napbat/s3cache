use std::time::Duration;

use groupnet::consistency::LeaseConfig;
use groupnet::core::{Config, NodeId};
use groupnet::runtime::Node;
use groupnet::transport::udp::UdpTransport;
use tracing::{info, warn};

use crate::sync::coherence::{Consistency, DEAD_TIMEOUT_FLOOR_MS, DEFAULT_LEASE_MS, WriteSync};

/// Attempts (one per second) to resolve a gossip seed's DNS name within one
/// refresh cycle — a `StatefulSet` peer's record can lag its own startup.
const SEED_RESOLVE_ATTEMPTS: u32 = 30;

/// How often each seed is re-resolved, forever. Pod IPs churn on restarts
/// and the seed's DNS record follows; re-resolution is the recovery channel
/// that works even when gossip cannot deliver the new address (a rebooted
/// peer is deaf to us until OUR datagrams come from an address it knows).
const SEED_REFRESH: Duration = Duration::from_secs(15);

/// Resolves `host:port` (a DNS name or a literal address) to a socket
/// address, retrying briefly; `None` when it never resolves.
async fn resolve_seed(addr: &str) -> Option<std::net::SocketAddr> {
    for attempt in 0..SEED_RESOLVE_ATTEMPTS {
        if attempt > 0 {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        match tokio::net::lookup_host(addr).await {
            Ok(mut addrs) => {
                if let Some(sock) = addrs.next() {
                    return Some(sock);
                }
            }
            Err(error) => tracing::debug!("resolving gossip seed `{addr}`: {error}"),
        }
    }
    None
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
    /// * A reader's confirmation is a min over its whole roster, and only a **reap**
    ///   removes a member from it. So one `CAP_LEASE` member that stops publishing
    ///   grants — crashed, hung, partitioned — freezes *every* reader's confirmation
    ///   cluster-wide. Each reader's window closes within one `D` of the freeze and
    ///   cannot reopen until membership reaps the silent member, at the reap horizon:
    ///   `2 × dead_timeout_ms` past the `Dead` verdict, itself up to
    ///   `detection_window_ms` past the silence.
    /// * Untuned that is `0.9 + 20 − 2` ≈ **19s of cluster-wide origin-serving** —
    ///   correct reads throughout, none of them cached. At `dead_timeout_ms = D = 2s` it
    ///   is ≈ **3s**.
    ///
    /// What it costs is the other end of the same horizon: `2 × dead_timeout_ms` is also
    /// how long a returning node's entries stay recoverable by a digest, so a partition
    /// outliving ~4s lands on the write-feed **gap** path instead of reconciling —
    /// distrust every cached body, re-LIST from the origin. That is not a regression to
    /// work around; the origin is the authority this index caches, and the gap path is
    /// s3cache's standing remedy for "this node provably missed writes". Trading a rare,
    /// loud, correct
    /// resync for 16s off every unreaped-granter freeze is the right side of that deal
    /// for a cache.
    pub async fn new(cfg: SyncConfig) -> Option<Self> {
        let me = NodeId::new(cfg.node_id.as_str());
        let transport = match UdpTransport::bind(me.clone(), cfg.bind.as_str()).await {
            Ok(transport) => transport,
            Err(error) => {
                let bind = &cfg.bind;
                warn!("gossip disabled: cannot bind `{bind}`: {error}");
                return None;
            }
        };
        let lease = LeaseConfig::for_duration(Duration::from_millis(cfg.lease_ms));
        debug_assert!(
            lease.validate().is_ok(),
            "S3CACHE_LEASE_MS outside the lease tier's envelope: {:?}",
            lease.validate()
        );
        let advertise = cfg
            .advertise
            .or_else(|| transport.local_addr().ok().map(|addr| addr.to_string()));
        let mut builder = Node::builder(me.clone(), transport.clone()).config(Config {
            dead_timeout_ms: cfg.lease_ms.max(DEAD_TIMEOUT_FLOOR_MS),
            ..Config::default()
        });
        if let Some(advertise) = advertise {
            builder = builder.advertise_addr(advertise);
        }
        let node = builder.spawn();
        let group = node.join_group("s3cache");
        // Seeds resolve off the startup path (DNS for a just-starting peer may
        // lag, and a slow resolver must not delay serving): each one registers
        // with the transport and joins via `add_peer` once its address is known.
        for (id, addr) in cfg.seeds {
            if id == cfg.node_id {
                continue; // a pod seeding itself (uniform config) is a no-op
            }
            let (transport, group) = (transport.clone(), group.clone());
            tokio::spawn(async move {
                let mut registered: Option<std::net::SocketAddr> = None;
                loop {
                    match resolve_seed(&addr).await {
                        Some(sock) if registered != Some(sock) => {
                            if registered.is_some() {
                                info!("gossip seed `{id}` moved to {sock}; re-registering");
                            }
                            transport.register_peer(NodeId::new(id.as_str()), sock);
                            group.add_peer(NodeId::new(id.as_str()));
                            registered = Some(sock);
                        }
                        None if registered.is_none() => {
                            warn!("gossip seed `{id}={addr}` not resolving yet; will keep trying");
                        }
                        Some(_) | None => {}
                    }
                    tokio::time::sleep(SEED_REFRESH).await;
                }
            });
        }
        let (bind, node_id, mode) = (&cfg.bind, &cfg.node_id, cfg.consistency.label());
        let lease_ms = cfg.lease_ms;
        info!(
            "gossip coherence bound on `{bind}` as `{node_id}` (consistency: {mode}, lease: {lease_ms}ms)"
        );
        Some(WriteSync::attach(
            group,
            me,
            cfg.consistency,
            lease,
            Some(node),
        ))
    }
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
