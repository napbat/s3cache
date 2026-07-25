//! Fast cross-node body-cache coherence over groupnet's consistency layer.
//!
//! Every durable write is published into a per-node [`WriteFeed`]; each peer's
//! apply loop turns [`PeerWrite::Wrote`] into a local invalidation and a
//! [`PeerWrite::Gap`] (writes provably missed — ring overflow or a peer
//! restart) into a full local flush, since the stale subset is unknowable.
//! Invalidation reaches live peers at network latency (the engine pushes
//! deltas eagerly), well ahead of the Valkey commit-log tail.
//!
//! Division of labour with [`coherence`](crate::coherence): the commit log
//! owns the LIST index and its read barrier (a shared, globally-ordered
//! stream); this feed owns *body-cache* invalidation. Both invalidate
//! idempotently, so running both is redundancy, not conflict — and a
//! deployment without gossip (`S3CACHE_GOSSIP_BIND` unset) keeps exactly the
//! coherence it has today.
//!
//! Not provided here (yet): read-your-writes tokens in API responses. The
//! feed already returns them; surfacing them needs a header design.

use std::num::NonZeroUsize;

use groupnet::consistency::{PeerWrite, PeerWrites, WriteFeed};
use groupnet::core::NodeId;
use groupnet::runtime::{Group, Node};
use groupnet::transport::udp::UdpTransport;
use tracing::{info, warn};

use crate::tier::{CacheKey, LocalCache};

/// Ring capacity: a peer that falls further behind than this many writes
/// gets a gap (full local flush) instead of per-key invalidations.
const FEED_CAPACITY: usize = 4096;

/// The publishing half of the write feed, plus what the apply loop needs.
pub(crate) struct WriteSync {
    feed: WriteFeed<CacheKey>,
    group: Group,
    me: NodeId,
    /// Keeps the gossip node (receive loop, group actors) alive for the
    /// process lifetime. `None` when a test drives a raw group directly.
    _node: Option<Node<UdpTransport>>,
}

/// `bucket` length-prefixed, then the key — unambiguous for any bucket/key.
fn encode_key(key: &CacheKey) -> Vec<u8> {
    let bucket = key.0.as_bytes();
    let mut out = Vec::with_capacity(4 + bucket.len() + key.1.len());
    out.extend_from_slice(
        &u32::try_from(bucket.len())
            .unwrap_or(u32::MAX)
            .to_le_bytes(),
    );
    out.extend_from_slice(bucket);
    out.extend_from_slice(key.1.as_bytes());
    out
}

fn decode_key(bytes: &[u8]) -> Option<CacheKey> {
    let len = usize::try_from(u32::from_le_bytes(bytes.get(0..4)?.try_into().ok()?)).ok()?;
    let bucket = std::str::from_utf8(bytes.get(4..4 + len)?).ok()?;
    let key = std::str::from_utf8(bytes.get(4 + len..)?).ok()?;
    Some((bucket.to_owned(), key.to_owned()))
}

impl WriteSync {
    /// Attach a feed to `group` as `me`. Start the apply loop separately with
    /// [`start_apply`](Self::start_apply) once the local cache exists.
    pub(crate) fn attach(group: Group, me: NodeId, node: Option<Node<UdpTransport>>) -> Self {
        let capacity = NonZeroUsize::new(FEED_CAPACITY).unwrap_or(NonZeroUsize::MIN);
        let feed = WriteFeed::new(group.clone(), capacity, encode_key);
        Self {
            feed,
            group,
            me,
            _node: node,
        }
    }

    /// Advertise a durable write to peers. Fire-and-forget: the returned
    /// read-your-writes token is unused until the API surfaces one.
    pub(crate) async fn publish(&self, bucket: &str, key: &str) {
        let _token = self
            .feed
            .publish(&(bucket.to_owned(), key.to_owned()))
            .await;
    }

    /// Spawn the apply loop: peers' writes drop the local copies; a gap
    /// flushes every local tier.
    pub(crate) fn start_apply(&self, local: LocalCache) {
        let mut peers = PeerWrites::new(self.group.clone(), self.me.clone(), decode_key);
        tokio::spawn(async move {
            while let Some(event) = peers.next().await {
                match event {
                    PeerWrite::Wrote { key, .. } => local.invalidate(&key).await,
                    PeerWrite::Gap { peer, .. } => {
                        warn!("write-feed gap from `{peer}`: flushing local tiers");
                        local.flush().await;
                    }
                }
            }
        });
    }
}

/// Build the gossip node and write feed from `S3CACHE_GOSSIP_*`, or `None`
/// when `S3CACHE_GOSSIP_BIND` is unset (single-node, or Valkey-only
/// coherence). Seeds are comma-separated `id=host:port` pairs; every other
/// peer resolves itself through gossiped advertisements, so only seeds need
/// static addressing.
pub(crate) async fn from_env(node_name: &str) -> Option<WriteSync> {
    let bind = std::env::var("S3CACHE_GOSSIP_BIND").ok()?;
    let me = NodeId::new(node_name);
    let transport = match UdpTransport::bind(me.clone(), bind.as_str()).await {
        Ok(transport) => transport,
        Err(error) => {
            warn!("gossip disabled: cannot bind `{bind}`: {error}");
            return None;
        }
    };
    let advertise = std::env::var("S3CACHE_GOSSIP_ADVERTISE")
        .ok()
        .or_else(|| transport.local_addr().ok().map(|addr| addr.to_string()));
    let mut builder = Node::builder(me.clone(), transport.clone());
    if let Some(advertise) = advertise {
        builder = builder.advertise_addr(advertise);
    }
    let seeds = std::env::var("S3CACHE_GOSSIP_SEEDS").unwrap_or_default();
    for seed in seeds.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        let Some((id, addr)) = seed.split_once('=') else {
            warn!("ignoring malformed gossip seed `{seed}` (want id=host:port)");
            continue;
        };
        match addr.parse() {
            Ok(sock) => {
                transport.register_peer(NodeId::new(id), sock);
                builder = builder.seed(NodeId::new(id));
            }
            Err(error) => warn!("ignoring gossip seed `{seed}`: {error}"),
        }
    }
    let node = builder.spawn();
    let group = node.join_group("s3cache");
    info!("gossip coherence bound on `{bind}` as `{node_name}`");
    Some(WriteSync::attach(group, me, Some(node)))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use groupnet::core::NodeId;
    use groupnet::runtime::{Group, Node};
    use groupnet::transport::mem::{MemTransport, Network};
    use s3s::dto::GetObjectOutput;

    use super::{WriteSync, decode_key, encode_key};
    use crate::metrics::Metrics;
    use crate::tier::{CachedObject, TieredCache};

    fn spawn_node(net: &Network, id: &str, peer: &str) -> (NodeId, Node<MemTransport>, Group) {
        let me = NodeId::new(id);
        let node = Node::builder(me.clone(), net.endpoint(me.clone()))
            .seed(NodeId::new(peer))
            .gossip_interval_ms(10)
            .anti_entropy_interval_ms(25)
            .spawn();
        let group = node.join_group("s3cache");
        (me, node, group)
    }

    fn cached(body: &'static [u8]) -> Arc<CachedObject> {
        Arc::new(CachedObject::from_get(
            &GetObjectOutput::default(),
            bytes::Bytes::from_static(body),
        ))
    }

    #[test]
    fn key_codec_round_trips_and_rejects_garbage() {
        let key = ("bucket-1".to_owned(), "a/b\0weird key".to_owned());
        assert_eq!(decode_key(&encode_key(&key)), Some(key));
        assert_eq!(decode_key(b"xx"), None);
        assert_eq!(decode_key(&u32::MAX.to_le_bytes()), None);
    }

    #[tokio::test]
    async fn peer_write_invalidates_the_local_tiers() {
        let net = Network::new();
        let (a_id, _a_node, a_group) = spawn_node(&net, "sync-a", "sync-b");
        let (b_id, _b_node, b_group) = spawn_node(&net, "sync-b", "sync-a");

        // Node B holds a soon-stale copy and runs the apply loop.
        let cache = TieredCache::new(1024 * 1024, None, Arc::new(Metrics::default()));
        let key = ("bkt".to_owned(), "obj".to_owned());
        cache.insert(key.clone(), cached(b"stale")).await;
        assert!(cache.get(&key).await.is_some());
        WriteSync::attach(b_group, b_id, None).start_apply(cache.local());

        // Node A publishes the write; B's copy must go.
        let sync_a = WriteSync::attach(a_group, a_id, None);
        sync_a.publish("bkt", "obj").await;
        for _ in 0..300 {
            if cache.get(&key).await.is_none() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("peer write did not invalidate the local copy");
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
}
