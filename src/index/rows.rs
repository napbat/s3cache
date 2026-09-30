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

/// A bucket's delete tombstones in key order, each with its deletion time.
#[derive(Clone, Default)]
pub(crate) struct GoneRows {
    map: OrdMap<String, SystemTime>,
    image: ImageTally,
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
        let previous = self.map.insert(key, at);
        if previous.is_none() {
            self.image.add(ImageTally::tombstone(len));
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

    /// Keep only the tombstones `keep` accepts.
    pub(crate) fn retain(&mut self, mut keep: impl FnMut(&str, &SystemTime) -> bool) {
        let dropped: Vec<String> = self
            .map
            .iter()
            .filter(|(key, at)| !keep(key, at))
            .map(|(key, _)| key.clone())
            .collect();
        for key in dropped {
            self.remove(&key);
        }
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
