//! Peer writes announced before their origin call and not settled yet.
//!
//! A `strong` writer publishes [`IndexOp::Pending`](crate::sync::wire::IndexOp) and
//! waits for its peers before it calls the origin. A reader holding one answers the
//! key from the origin until the writer settles it: a writer that crashes after the
//! origin applied its write never says what it did. A settling event names its pending
//! write, and every event carries the writer's oldest one still open, so a settle lost
//! to a gap is covered by the next event. What a writer's ended life left open is
//! fenced as uncertain instead (see [`fence_uncertain_key`](super::fence_uncertain_key)):
//! only the origin can say what it did.

use std::collections::{BTreeMap, HashMap};
use std::ops::Bound;

use groupnet::core::volatile_bootstrap::journal::{DeltaIdentity, NativeCut};

use super::{KeyIndex, KeyIndexReadGuard, KeyIndexState};

/// A key fenced as uncertain, with the token its origin reconciliation must present.
pub(crate) type UncertainKey = (String, String, u64);

/// One writer life's open pending writes, by feed sequence.
struct Life {
    epoch: u64,
    open: BTreeMap<u64, (String, String)>,
}

#[derive(Default)]
pub(super) struct PendingWrites {
    writers: HashMap<Vec<u8>, Life>,
    /// How many open pending writes fence each key, per bucket.
    keys: HashMap<String, BTreeMap<String, u32>>,
}

impl PendingWrites {
    #[cfg(test)]
    fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    fn fenced(&self, bucket: &str, key: &str) -> bool {
        self.keys
            .get(bucket)
            .is_some_and(|keys| keys.contains_key(key))
    }

    pub(super) fn fenced_under(&self, bucket: &str, prefix: &str) -> bool {
        self.keys
            .get(bucket)
            .is_some_and(|keys| first_under(keys, prefix))
    }

    fn hold(&mut self, writer: &[u8], epoch: u64, seq: u64, bucket: &str, key: &str) {
        let life = self.writers.entry(writer.to_vec()).or_insert_with(|| Life {
            epoch,
            open: BTreeMap::new(),
        });
        if life.epoch != epoch {
            return;
        }
        let entry = (bucket.to_owned(), key.to_owned());
        if life.open.insert(seq, entry).is_none() {
            *self
                .keys
                .entry(bucket.to_owned())
                .or_default()
                .entry(key.to_owned())
                .or_default() += 1;
        }
    }

    /// Settle `settles` and everything below `floor` (below `seq`, the settling
    /// event's own position, when the writer had nothing open).
    fn settle(
        &mut self,
        writer: &[u8],
        epoch: u64,
        seq: u64,
        settles: Option<u64>,
        floor: Option<u64>,
    ) {
        let Some(life) = self.writers.get_mut(writer) else {
            return;
        };
        if life.epoch != epoch {
            return;
        }
        let mut done: Vec<(String, String)> = settles
            .and_then(|settled| life.open.remove(&settled))
            .into_iter()
            .collect();
        let still_open = life.open.split_off(&floor.unwrap_or(seq));
        done.extend(std::mem::replace(&mut life.open, still_open).into_values());
        if life.open.is_empty() {
            self.writers.remove(writer);
        }
        for (bucket, key) in &done {
            self.release(bucket, key);
        }
    }

    /// End `writer`'s life if it is older than `epoch`, returning what it left open.
    fn end_life_before(&mut self, writer: &[u8], epoch: u64) -> Vec<(String, String)> {
        if self
            .writers
            .get(writer)
            .is_none_or(|life| life.epoch >= epoch)
        {
            return Vec::new();
        }
        let Some(life) = self.writers.remove(writer) else {
            return Vec::new();
        };
        let ended: Vec<(String, String)> = life.open.into_values().collect();
        for (bucket, key) in &ended {
            self.release(bucket, key);
        }
        ended
    }

    fn release(&mut self, bucket: &str, key: &str) {
        let Some(keys) = self.keys.get_mut(bucket) else {
            return;
        };
        if let Some(count) = keys.get_mut(key) {
            *count -= 1;
            if *count == 0 {
                keys.remove(key);
            }
        }
        if keys.is_empty() {
            self.keys.remove(bucket);
        }
    }
}

/// Whether any key of a sorted set starts with `prefix`.
fn first_under<V>(keys: &BTreeMap<String, V>, prefix: &str) -> bool {
    keys.range::<str, _>((Bound::Included(prefix), Bound::Unbounded))
        .next()
        .is_some_and(|(key, _)| key.starts_with(prefix))
}

impl KeyIndexState {
    /// Fence what an ended writer life left pending as uncertain.
    fn end_pending_life(&mut self, writer: &[u8], epoch: u64) -> Vec<UncertainKey> {
        self.pending
            .end_life_before(writer, epoch)
            .into_iter()
            .map(|(bucket, key)| {
                let token = self.fence_key(&bucket, &key);
                (bucket, key, token)
            })
            .collect()
    }
}

impl KeyIndex {
    /// Hold a peer's pending write under the lock that notes its feed position. The
    /// event has no index effect, so the donor journal records a no-op for it.
    /// Returns the keys an ended life of that writer left open, now uncertain.
    pub(crate) fn hold_pending_native(
        &self,
        bucket: &str,
        key: &str,
        cut: &NativeCut,
        floor: Option<u64>,
    ) -> Vec<UncertainKey> {
        let mut index = self.inner.write().unwrap();
        if !index.note_native_cut(cut) {
            return Vec::new();
        }
        if let Some(capture) = index.capture.as_mut() {
            capture.record_native_noop(DeltaIdentity::Native(cut.clone()));
        }
        let ended = index.end_pending_life(&cut.writer, cut.epoch);
        index
            .pending
            .settle(&cut.writer, cut.epoch, cut.sequence, None, floor);
        index
            .pending
            .hold(&cut.writer, cut.epoch, cut.sequence, bucket, key);
        ended
    }

    /// Settle a peer's pending writes as a later event of its feed says, after that
    /// event's own index effect. An [`IndexOp::Abandoned`](crate::sync::wire::IndexOp)
    /// has none, so `noop` notes its position and journals it here. Returns the keys
    /// an ended life of that writer left open, now uncertain.
    pub(crate) fn settle_pending_native(
        &self,
        cut: &NativeCut,
        settles: Option<u64>,
        floor: Option<u64>,
        noop: bool,
    ) -> Vec<UncertainKey> {
        let mut index = self.inner.write().unwrap();
        if noop {
            if !index.note_native_cut(cut) {
                return Vec::new();
            }
            if let Some(capture) = index.capture.as_mut() {
                capture.record_native_noop(DeltaIdentity::Native(cut.clone()));
            }
        }
        let ended = index.end_pending_life(&cut.writer, cut.epoch);
        index
            .pending
            .settle(&cut.writer, cut.epoch, cut.sequence, settles, floor);
        ended
    }

    /// End every pending write of `writer`'s lives before `epoch`: its feed moved
    /// past them (a new life, or a seal). Returns those keys, now uncertain.
    pub(crate) fn end_pending_life(&self, writer: &[u8], epoch: u64) -> Vec<UncertainKey> {
        self.inner
            .write()
            .unwrap()
            .end_pending_life(writer, epoch)
    }

    /// Note this node's own feed event that changes no index row — its pending and
    /// abandoned writes — under the lock that assigns its position, journaled as a
    /// no-op so the donor journal stays contiguous.
    pub(crate) fn note_own_native<R>(&self, assign: impl FnOnce() -> (R, NativeCut)) -> R {
        let mut index = self.inner.write().unwrap();
        let (value, position) = assign();
        if index.note_native_cut(&position)
            && let Some(capture) = index.capture.as_mut()
        {
            capture.record_native_noop(DeltaIdentity::Native(position));
        }
        value
    }
}

impl KeyIndexReadGuard<'_> {
    /// Whether `key` may only be answered from the origin: its last mutation is
    /// unreconciled, or a peer's write of it is pending.
    pub(crate) fn key_fenced(&self, bucket: &str, key: &str) -> bool {
        self.0.pending.fenced(bucket, key)
            || self
                .0
                .buckets
                .get(bucket)
                .is_some_and(|b| b.uncertain_keys.contains_key(key))
    }

    /// Whether any key under `prefix` is fenced (see [`key_fenced`](Self::key_fenced)).
    pub(crate) fn fenced_under(&self, bucket: &str, prefix: &str) -> bool {
        self.0.pending.fenced_under(bucket, prefix)
            || self
                .0
                .buckets
                .get(bucket)
                .is_some_and(|b| first_under(&b.uncertain_keys, prefix))
    }
}

#[cfg(test)]
mod tests {
    use groupnet::core::volatile_bootstrap::journal::NativeCut;

    use crate::index::{KeyIndex, uncertain_key_is_current};

    fn cut(epoch: u64, sequence: u64) -> NativeCut {
        NativeCut {
            writer: b"writer".to_vec(),
            epoch,
            sequence,
        }
    }

    fn fenced(state: &KeyIndex, key: &str) -> bool {
        state.read().unwrap().key_fenced("bkt", key)
    }

    #[test]
    fn a_settle_releases_only_the_pending_write_it_names() {
        let state = KeyIndex::default();
        state.hold_pending_native("bkt", "a", &cut(1, 1), None);
        state.hold_pending_native("bkt", "b", &cut(1, 2), Some(1));
        state.settle_pending_native(&cut(1, 3), Some(2), Some(1), false);
        assert!(fenced(&state, "a"), "a's write is still open");
        assert!(!fenced(&state, "b"));
        state.settle_pending_native(&cut(1, 4), Some(1), None, false);
        assert!(!fenced(&state, "a"));
        assert!(state.read().unwrap().0.pending.is_empty());
    }

    /// Two writes of one key by one writer fence it until both settle.
    #[test]
    fn a_key_stays_fenced_while_any_write_of_it_is_open() {
        let state = KeyIndex::default();
        state.hold_pending_native("bkt", "k", &cut(1, 1), None);
        state.hold_pending_native("bkt", "k", &cut(1, 2), Some(1));
        state.settle_pending_native(&cut(1, 3), Some(1), Some(2), false);
        assert!(fenced(&state, "k"));
        state.settle_pending_native(&cut(1, 4), Some(2), None, false);
        assert!(!fenced(&state, "k"));
    }

    /// The floor settles a write whose settling event a gap swallowed.
    #[test]
    fn the_floor_settles_a_write_whose_settle_was_lost() {
        let state = KeyIndex::default();
        state.hold_pending_native("bkt", "lost", &cut(1, 1), None);
        state.hold_pending_native("bkt", "open", &cut(1, 2), Some(1));
        // Sequence 3 settled `lost` and never arrived; 4 says 2 is the oldest open.
        state.settle_pending_native(&cut(1, 4), None, Some(2), true);
        assert!(!fenced(&state, "lost"));
        assert!(fenced(&state, "open"));
    }

    /// A duplicate delivery changes nothing.
    #[test]
    fn a_duplicate_pending_event_is_held_once() {
        let state = KeyIndex::default();
        state.hold_pending_native("bkt", "k", &cut(1, 1), None);
        state.hold_pending_native("bkt", "k", &cut(1, 1), None);
        state.settle_pending_native(&cut(1, 2), Some(1), None, true);
        assert!(!fenced(&state, "k"));
    }

    /// A writer's next life fences what its last one left open as uncertain: the
    /// crashed life cannot say whether the origin applied it.
    #[test]
    fn a_new_life_turns_open_writes_into_uncertain_keys() {
        let state = KeyIndex::default();
        state.hold_pending_native("bkt", "k", &cut(1, 1), None);
        assert!(state.end_pending_life(b"writer", 1).is_empty(), "same life");
        let ended = state.end_pending_life(b"writer", 2);
        assert_eq!(ended.len(), 1);
        let (bucket, key, token) = &ended[0];
        assert_eq!((bucket.as_str(), key.as_str()), ("bkt", "k"));
        assert!(uncertain_key_is_current(&state, "bkt", "k", *token));
        assert!(state.read().unwrap().0.pending.is_empty());
        assert!(fenced(&state, "k"), "uncertain until the origin says");
    }

    #[test]
    fn a_list_is_fenced_only_under_a_prefix_holding_a_fenced_key() {
        let state = KeyIndex::default();
        state.hold_pending_native("bkt", "log/0002", &cut(1, 1), None);
        let guard = state.read().unwrap();
        assert!(guard.fenced_under("bkt", ""));
        assert!(guard.fenced_under("bkt", "log/"));
        assert!(!guard.fenced_under("bkt", "ptr/"));
        assert!(!guard.fenced_under("bkt", "log/0003"));
        assert!(!guard.fenced_under("other", ""));
    }
}
