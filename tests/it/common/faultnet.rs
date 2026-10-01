//! An in-process gossip fabric a test can break: the in-memory transport's
//! routing, plus per-direction cuts, delays and loss. Groupnet's transport
//! contract is best-effort datagrams (dropped, reordered or duplicated), so every
//! fault here is one the protocol has to survive; a link that recovers is a
//! partition that heals.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use groupnet::core::NodeId;
use groupnet::transport::{Inbound, Transport};
use tokio::sync::{Mutex as AsyncMutex, mpsc};

/// The faults on one direction of one link.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Link {
    /// Every datagram is dropped.
    pub cut: bool,
    /// Every delivered datagram is held this long first.
    pub delay: Duration,
    /// Each datagram is dropped with this probability, in thousandths.
    pub loss_per_mille: u16,
}

#[derive(Default)]
struct Fabric {
    inboxes: HashMap<NodeId, mpsc::UnboundedSender<Inbound>>,
    links: HashMap<(NodeId, NodeId), Link>,
    rng: u64,
}

impl Fabric {
    /// A splitmix64 step: deterministic per seed, which is all a loss model needs.
    fn next(&mut self) -> u64 {
        self.rng = self.rng.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.rng;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}

/// The shared fabric. Clone it freely; every clone routes through one table.
#[derive(Clone)]
pub struct FaultNet(Arc<Mutex<Fabric>>);

impl FaultNet {
    /// A healthy fabric whose loss decisions follow `seed`.
    #[must_use]
    pub fn new(seed: u64) -> Self {
        Self(Arc::new(Mutex::new(Fabric {
            rng: seed,
            ..Fabric::default()
        })))
    }

    /// Register `id`'s endpoint, replacing a previous life's: a restarted pod
    /// keeps its name, and datagrams for the old one go nowhere.
    #[must_use]
    pub fn endpoint(&self, id: &str) -> FaultTransport {
        let (tx, rx) = mpsc::unbounded_channel();
        let id = NodeId::new(id);
        self.0.lock().unwrap().inboxes.insert(id.clone(), tx);
        FaultTransport {
            id,
            net: self.clone(),
            inbox: AsyncMutex::new(rx),
        }
    }

    /// Set the faults on datagrams from `from` to `to`.
    pub fn set(&self, from: &str, to: &str, link: Link) {
        self.0
            .lock()
            .unwrap()
            .links
            .insert((NodeId::new(from), NodeId::new(to)), link);
    }

    /// Set the same faults in both directions between `a` and `b`.
    pub fn both(&self, a: &str, b: &str, link: Link) {
        self.set(a, b, link);
        self.set(b, a, link);
    }

    /// Every link healthy again.
    pub fn heal(&self) {
        self.0.lock().unwrap().links.clear();
    }

    /// Where a datagram goes, if anywhere, and how late.
    fn route(
        &self,
        from: &NodeId,
        to: &NodeId,
    ) -> Option<(mpsc::UnboundedSender<Inbound>, Duration)> {
        let mut fabric = self.0.lock().unwrap();
        let link = fabric
            .links
            .get(&(from.clone(), to.clone()))
            .copied()
            .unwrap_or_default();
        if link.cut {
            return None;
        }
        if link.loss_per_mille > 0 && fabric.next() % 1000 < u64::from(link.loss_per_mille) {
            return None;
        }
        let tx = fabric.inboxes.get(to)?.clone();
        Some((tx, link.delay))
    }
}

/// One node's endpoint on a [`FaultNet`].
pub struct FaultTransport {
    id: NodeId,
    net: FaultNet,
    inbox: AsyncMutex<mpsc::UnboundedReceiver<Inbound>>,
}

/// The endpoint's receiver was closed: its pod is gone.
#[derive(Debug)]
pub struct Closed;

impl std::fmt::Display for Closed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("fault transport endpoint closed")
    }
}

impl std::error::Error for Closed {}

impl Transport for FaultTransport {
    type Error = Closed;

    fn send(&self, to: &NodeId, msg: &[u8]) -> impl Future<Output = Result<(), Closed>> + Send {
        if let Some((tx, delay)) = self.net.route(&self.id, to) {
            let inbound = Inbound {
                from: self.id.clone(),
                msg: msg.to_vec(),
            };
            if delay.is_zero() {
                let _ = tx.send(inbound);
            } else {
                // Held on the sender's runtime, as a slow wire would hold it: a pod
                // that stops takes its undelivered datagrams with it.
                tokio::spawn(async move {
                    tokio::time::sleep(delay).await;
                    let _ = tx.send(inbound);
                });
            }
        }
        std::future::ready(Ok(()))
    }

    async fn recv(&self) -> Result<Inbound, Closed> {
        let mut inbox = self.inbox.lock().await;
        inbox.recv().await.ok_or(Closed)
    }
}
