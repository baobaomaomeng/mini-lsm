use std::ops::Bound;
use std::path::Path;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use anyhow::{Ok, Result};
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use ouroboros::self_referencing;

use crate::iterators::StorageIterator;
use crate::key::KeySlice;
use crate::table::SsTableBuilder;
use crate::wal::Wal;

/// A basic mem-table based on crossbeam-skiplist.
///
/// An initial implementation of memtable is part of week 1, day 1. It will be incrementally implemented in other
/// chapters of week 1 and week 2.
pub struct MemTable {
    map: Arc<SkipMap<Bytes, Bytes>>,
    wal: Option<Wal>,
    id: usize,
    approximate_size: Arc<AtomicUsize>,
}

/// Create a bound of `Bytes` from a bound of `&[u8]`.
pub(crate) fn map_bound(bound: Bound<&[u8]>) -> Bound<Bytes> {
    match bound {
        Bound::Included(x) => Bound::Included(Bytes::copy_from_slice(x)),
        Bound::Excluded(x) => Bound::Excluded(Bytes::copy_from_slice(x)),
        Bound::Unbounded => Bound::Unbounded,
    }
}

impl MemTable {
    pub fn create(id: usize) -> Self {
        Self {
            map: Arc::new(SkipMap::new()),
            wal: None,
            id: id,
            approximate_size: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Create a new mem-table with WAL
    pub fn create_with_wal(id: usize, path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            map: Arc::new(SkipMap::new()),
            wal: Some(Wal::create(path)?),
            id: id,
            approximate_size: Arc::new(AtomicUsize::new(0)),
        })
    }

    /// Create a memtable from WAL
    pub fn recover_from_wal(id: usize, path: impl AsRef<Path>) -> Result<Self> {
        let map = SkipMap::new();
        let wal = Wal::recover(path, &map)?;

        Ok(Self {
            map: Arc::new(map),
            wal: Some(wal),
            id: id,
            approximate_size: Arc::new(AtomicUsize::new(0)),
        })
    }

    pub fn for_testing_put_slice(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.put(key, value)
    }

    pub fn for_testing_get_slice(&self, key: &[u8]) -> Option<Bytes> {
        self.get(key)
    }

    pub fn for_testing_scan_slice(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> MemTableIterator {
        self.scan(lower, upper)
    }

    pub fn get(&self, _key: &[u8]) -> Option<Bytes> {
        if let Some(value) = self.map.get(_key) {
            return Some(value.value().clone());
        }
        None
    }

    /// Put a key-value pair into the mem-table.
    ///
    /// In week 1, day 1, simply put the key-value pair into the skipmap.
    /// In week 2, day 6, also flush the data to WAL.
    /// In week 3, day 5, modify the function to use the batch API.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        if let Some(wal) = &self.wal {
            let _ = wal.put(key, value);
        }
        let key_size = key.len();
        let value_size = value.len();
        self.approximate_size
            .fetch_add(key_size + value_size, std::sync::atomic::Ordering::Relaxed);
        self.map
            .insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value));
        Ok(())
    }

    /// Implement this in week 3, day 5.
    pub fn put_batch(&self, _data: &[(KeySlice, &[u8])]) -> Result<()> {
        unimplemented!()
    }

    ///刷新自身
    pub fn sync_wal(&self) -> Result<()> {
        if let Some(ref wal) = self.wal {
            wal.sync()?;
        }
        Ok(())
    }

    /// Get an iterator over a range of keys.
    pub fn scan(&self, _lower: Bound<&[u8]>, _upper: Bound<&[u8]>) -> MemTableIterator {
        let lower_bound = map_bound(_lower);
        let upper_bound = map_bound(_upper);
        let bounds = (lower_bound, upper_bound);
        let mut iterator = MemTableIteratorBuilder {
            map: self.map.clone(),
            iter_builder: |map| map.range(bounds),
            item: (Bytes::new(), Bytes::new()),
        }
        .build();
        iterator.next().unwrap();
        iterator
    }

    /// Flush the mem-table to SSTable. Implement in week 1 day 6.
    pub fn flush(&self, builder: &mut SsTableBuilder) -> Result<()> {
        for entry in self.map.iter() {
            builder.add(
                KeySlice::from_slice(entry.key().as_ref()),
                entry.value().as_ref(),
            );
        }
        Ok(())
    }

    pub fn id(&self) -> usize {
        self.id
    }

    pub fn approximate_size(&self) -> usize {
        self.approximate_size
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Only use this function when closing the database
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

type SkipMapRangeIter<'a> =
    crossbeam_skiplist::map::Range<'a, Bytes, (Bound<Bytes>, Bound<Bytes>), Bytes, Bytes>;

/// An iterator over a range of `SkipMap`. This is a self-referential structure and please refer to week 1, day 2
/// chapter for more information.
///
/// This is part of week 1, day 2.
#[self_referencing]
pub struct MemTableIterator {
    /// Stores a reference to the skipmap.
    map: Arc<SkipMap<Bytes, Bytes>>,
    /// Stores a skipmap iterator that refers to the lifetime of `MemTableIterator` itself.
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    /// Stores the current key-value pair.
    item: (Bytes, Bytes),
}

impl StorageIterator for MemTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn value(&self) -> &[u8] {
        self.borrow_item().1.as_ref()
    }

    fn key(&self) -> KeySlice {
        KeySlice::from_slice(self.borrow_item().0.as_ref())
    }

    fn is_valid(&self) -> bool {
        !self.borrow_item().0.is_empty()
    }

    fn next(&mut self) -> Result<()> {
        let entry = self.with_iter_mut(|iter| match iter.next() {
            Some(entry) => (entry.key().clone(), entry.value().clone()),
            None => (Bytes::new(), Bytes::new()),
        });
        self.with_item_mut(|item| {
            *item = entry;
        });
        Ok(())
    }
}
