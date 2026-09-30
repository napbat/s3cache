//! One bucket's rows, live and deleted, each in a persistent ordered map
//! (`imbl`'s B+tree). A clone shares every node with its source, so the
//! donor's snapshot at C costs O(1) per bucket however many rows the bucket
//! holds. A later write copies only the nodes on its own path, and only while
//! such a snapshot is still alive; with none alive, every node is uniquely
//! owned and a write mutates it in place. Each map also keeps the image tally
//! of its rows, so C sizes the image it reserves for without walking a row.

use std::ops::{Bound, Index};
use std::time::SystemTime;

use imbl::OrdMap;
use imbl::ordmap::{Iter, RangedIter};
use imbl::shared_ptr::DefaultSharedPtr;

use super::ObjEntry;
use super::fleet::ImageTally;

/// A bucket's live rows in key order.
#[derive(Clone, Default)]
pub(crate) struct KeyRows {
    map: OrdMap<String, ObjEntry>,
    image: ImageTally,
}

impl KeyRows {
    pub(crate) fn len(&self) -> usize {
        self.map.len()
    }

    pub(crate) fn get(&self, key: &str) -> Option<&ObjEntry> {
        self.map.get(key)
    }

    pub(crate) fn contains_key(&self, key: &str) -> bool {
        self.map.contains_key(key)
    }

    pub(crate) fn iter(&self) -> Iter<'_, String, ObjEntry, DefaultSharedPtr> {
        self.map.iter()
    }

    /// The rows from `lower` on, in key order.
    pub(crate) fn range_from<'a>(
        &'a self,
        lower: Bound<&'a str>,
    ) -> RangedIter<'a, String, ObjEntry, DefaultSharedPtr> {
        self.map.range::<_, str>((lower, Bound::Unbounded))
    }

    /// Index `entry` under `key`, returning the row it replaced.
    pub(crate) fn insert(&mut self, key: String, entry: ObjEntry) -> Option<ObjEntry> {
        let len = key.len();
        self.image.add(ImageTally::entry(len, &entry));
        let previous = self.map.insert(key, entry);
        if let Some(previous) = &previous {
            self.image.sub(ImageTally::entry(len, previous));
        }
        previous
    }

    pub(crate) fn remove(&mut self, key: &str) -> Option<ObjEntry> {
        let previous = self.map.remove(key)?;
        self.image.sub(ImageTally::entry(key.len(), &previous));
        Some(previous)
    }

    /// Change the row under `key` in place, if there is one.
    pub(crate) fn update<R>(
        &mut self,
        key: &str,
        change: impl FnOnce(&mut ObjEntry) -> R,
    ) -> Option<R> {
        let entry = self.map.get_mut(key)?;
        let before = ImageTally::entry(key.len(), entry);
        let result = change(entry);
        self.image.sub(before);
        self.image.add(ImageTally::entry(key.len(), entry));
        Some(result)
    }

    /// The image tally of every row.
    pub(crate) fn image(&self) -> ImageTally {
        self.image
    }
}

impl Index<&str> for KeyRows {
    type Output = ObjEntry;

    fn index(&self, key: &str) -> &ObjEntry {
        &self.map[key]
    }
}

impl<'a> IntoIterator for &'a KeyRows {
    type Item = (&'a String, &'a ObjEntry);
    type IntoIter = Iter<'a, String, ObjEntry, DefaultSharedPtr>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// Tombstones one sweep step visits: expiry work per delete stays bounded
/// however many tombstones the bucket holds.
pub(crate) const SWEEP_STEP: usize = 16;

/// A bucket's delete tombstones in key order, each with its deletion time.
///
/// Expired tombstones are forgotten by an incremental sweep: a cursor walks the
/// key order [`SWEEP_STEP`] tombstones at a time and wraps at the end, so every
/// tombstone is revisited once per lap. `floor` is a lower bound on every
/// deletion time held, exact as of the last completed lap and lowered by each
/// later insert; while it has not expired, a sweep visits nothing.
#[derive(Clone, Default)]
pub(crate) struct GoneRows {
    map: OrdMap<String, SystemTime>,
    image: ImageTally,
    /// The last key the current lap visited; `None` starts a lap.
    cursor: Option<String>,
    /// Lower bound on every deletion time held; `None` when none is.
    floor: Option<SystemTime>,
    /// Lowest deletion time the current lap kept, or saw inserted behind its
    /// cursor where the lap will not visit it: the next `floor` once the lap
    /// wraps.
    lap_floor: Option<SystemTime>,
}

impl GoneRows {
    pub(crate) fn len(&self) -> usize {
        self.map.len()
    }

    pub(crate) fn get(&self, key: &str) -> Option<&SystemTime> {
        self.map.get(key)
    }

    pub(crate) fn iter(&self) -> Iter<'_, String, SystemTime, DefaultSharedPtr> {
        self.map.iter()
    }

    /// Record `key` as deleted at `at`, returning the tombstone it replaced.
    pub(crate) fn insert(&mut self, key: String, at: SystemTime) -> Option<SystemTime> {
        let len = key.len();
        let behind = self
            .cursor
            .as_deref()
            .is_some_and(|cursor| key.as_str() <= cursor);
        let previous = self.map.insert(key, at);
        if previous.is_none() {
            self.image.add(ImageTally::tombstone(len));
        }
        self.floor = Some(self.floor.map_or(at, |floor| floor.min(at)));
        if behind {
            self.lap_floor = Some(self.lap_floor.map_or(at, |floor| floor.min(at)));
        }
        previous
    }

    /// Record `key` as deleted at `at` unless it already carries a tombstone
    /// at least as late.
    pub(crate) fn raise(&mut self, key: &str, at: SystemTime) {
        if self.map.get(key).is_none_or(|dead| *dead < at) {
            self.insert(key.to_owned(), at);
        }
    }

    pub(crate) fn remove(&mut self, key: &str) -> Option<SystemTime> {
        let previous = self.map.remove(key)?;
        self.image.sub(ImageTally::tombstone(key.len()));
        Some(previous)
    }

    /// Forget tombstones deleted before `cutoff`, visiting at most
    /// [`SWEEP_STEP`] of them from where the last step stopped, and none when
    /// no held tombstone can be that old. Returns how many were forgotten.
    pub(crate) fn sweep_expired(&mut self, cutoff: SystemTime) -> usize {
        if self.floor.is_none_or(|floor| floor >= cutoff) {
            return 0;
        }
        let mut cursor = self.cursor.take();
        let mut expired = Vec::new();
        let mut visited = 0;
        let mut last = None;
        let lower = cursor.as_deref().map_or(Bound::Unbounded, Bound::Excluded);
        for (key, at) in self
            .map
            .range::<_, str>((lower, Bound::Unbounded))
            .take(SWEEP_STEP)
        {
            visited += 1;
            if *at < cutoff {
                expired.push(key.clone());
            } else {
                self.lap_floor = Some(self.lap_floor.map_or(*at, |floor| floor.min(*at)));
            }
            last = Some(key);
        }
        if visited < SWEEP_STEP {
            // The lap reached the last key: every tombstone still held was
            // either kept by this lap or inserted during it.
            self.floor = self.lap_floor.take();
            cursor = None;
        } else if let Some(last) = last {
            match &mut cursor {
                Some(cursor) => {
                    cursor.clear();
                    cursor.push_str(last);
                }
                None => cursor = Some(last.clone()),
            }
        }
        self.cursor = cursor;
        for key in &expired {
            self.remove(key);
        }
        expired.len()
    }

    /// The image tally of every tombstone.
    pub(crate) fn image(&self) -> ImageTally {
        self.image
    }
}

impl Index<&str> for GoneRows {
    type Output = SystemTime;

    fn index(&self, key: &str) -> &SystemTime {
        &self.map[key]
    }
}

impl<'a> IntoIterator for &'a GoneRows {
    type Item = (&'a String, &'a SystemTime);
    type IntoIter = Iter<'a, String, SystemTime, DefaultSharedPtr>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, UNIX_EPOCH};

    use super::*;

    fn at(secs: u64) -> SystemTime {
        UNIX_EPOCH + Duration::from_secs(secs)
    }

    #[test]
    fn sweep_forgets_only_expired_tombstones_a_bounded_step_at_a_time() {
        let mut gone = GoneRows::default();
        let mut kept = GoneRows::default();
        for n in 0..40 {
            let key = format!("k{n:02}");
            let dead = if n % 2 == 0 { at(1) } else { at(100) };
            gone.insert(key.clone(), dead);
            if n % 2 == 1 {
                kept.insert(key, dead);
            }
        }
        // Two full steps, then a short one that reaches the end of the lap.
        assert_eq!(gone.sweep_expired(at(50)), SWEEP_STEP / 2);
        assert_eq!(gone.sweep_expired(at(50)), SWEEP_STEP / 2);
        // A tombstone inserted behind the cursor still bounds the next floor.
        gone.insert("a".to_owned(), at(60));
        kept.insert("a".to_owned(), at(60));
        assert_eq!(gone.sweep_expired(at(50)), 4);
        assert_eq!(gone.len(), 21);
        assert!(gone.iter().all(|(_, dead)| *dead >= at(50)));
        assert_eq!(gone.image(), kept.image());
        // The lap proved nothing held expires before 60: no step visits a row.
        assert_eq!(gone.sweep_expired(at(60)), 0);
        assert!(gone.cursor.is_none());
        // Past the floor the next lap forgets exactly what has now expired.
        assert_eq!(gone.sweep_expired(at(61)), 1);
        assert_eq!(gone.get("a"), None);
        // An older insert lowers the floor at once.
        gone.insert("z".to_owned(), at(2));
        let mut forgotten = 0;
        for _ in 0..=gone.len() / SWEEP_STEP + 1 {
            forgotten += gone.sweep_expired(at(61));
        }
        assert_eq!(forgotten, 1);
        assert_eq!(gone.len(), 20);
    }
}
