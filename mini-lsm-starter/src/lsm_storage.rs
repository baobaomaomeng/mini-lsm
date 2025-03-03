// Copyright (c) 2022-2025 Alex Chi Z
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::collections::{BTreeSet, HashMap};
use std::fs::File;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use anyhow::{Context, Ok, Result};
use bytes::Bytes;
use parking_lot::{Mutex, MutexGuard, RwLock};

use crate::block::Block;
use crate::compact::{
    CompactionController, CompactionOptions, LeveledCompactionController, LeveledCompactionOptions,
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, TieredCompactionController,
};
use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::key::{self, KeySlice};
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::manifest::{Manifest, ManifestRecord};
use crate::mem_table::{map_bound, map_key_bound_plus_ts, MemTable};
use crate::mvcc::txn::{Transaction, TxnIterator};
use crate::mvcc::LsmMvccInner;
use crate::table::{FileObject, SsTable, SsTableIterator};

pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

/// Represents the state of the storage engine.
#[derive(Clone)]
pub struct LsmStorageState {
    /// The current memtable.
    pub memtable: Arc<MemTable>,
    /// Immutable memtables, from latest to earliest.
    pub imm_memtables: Vec<Arc<MemTable>>,
    /// L0 SSTs, from latest to earliest.
    pub l0_sstables: Vec<usize>,
    /// SsTables sorted by key range; L1 - L_max for leveled compaction, or tiers for tiered
    /// compaction.
    pub levels: Vec<(usize, Vec<usize>)>,
    /// SST objects.
    pub sstables: HashMap<usize, Arc<SsTable>>,
}

pub enum WriteBatchRecord<T: AsRef<[u8]>> {
    Put(T, T),
    Del(T),
}

impl LsmStorageState {
    fn create(options: &LsmStorageOptions) -> Self {
        let levels = match &options.compaction_options {
            CompactionOptions::Leveled(LeveledCompactionOptions { max_levels, .. })
            | CompactionOptions::Simple(SimpleLeveledCompactionOptions { max_levels, .. }) => (1
                ..=*max_levels)
                .map(|level| (level, Vec::new()))
                .collect::<Vec<_>>(),
            CompactionOptions::Tiered(_) => Vec::new(),
            CompactionOptions::NoCompaction => vec![(1, Vec::new())],
        };
        Self {
            memtable: Arc::new(MemTable::create(0)),
            imm_memtables: Vec::new(),
            l0_sstables: Vec::new(),
            levels,
            sstables: Default::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LsmStorageOptions {
    // Block size in bytes
    pub block_size: usize,
    // SST size in bytes, also the approximate memtable capacity limit
    pub target_sst_size: usize,
    // Maximum number of memtables in memory, flush to L0 when exceeding this limit
    pub num_memtable_limit: usize,
    pub compaction_options: CompactionOptions,
    pub enable_wal: bool,
    pub serializable: bool,
}

impl LsmStorageOptions {
    pub fn default_for_week1_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 50,
            serializable: false,
        }
    }

    pub fn default_for_week1_day6_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }

    pub fn default_for_week2_test(compaction_options: CompactionOptions) -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 1 << 20, // 1MB
            compaction_options,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }
}

#[derive(Clone, Debug)]
pub enum CompactionFilter {
    Prefix(Bytes),
}

/// The storage interface of the LSM tree.
pub(crate) struct LsmStorageInner {
    pub(crate) state: Arc<RwLock<Arc<LsmStorageState>>>,
    pub(crate) state_lock: Mutex<()>,
    path: PathBuf,
    pub(crate) block_cache: Arc<BlockCache>,
    next_sst_id: AtomicUsize,
    pub(crate) options: Arc<LsmStorageOptions>,
    pub(crate) compaction_controller: CompactionController,
    pub(crate) manifest: Manifest,
    pub(crate) mvcc: LsmMvccInner,
    pub(crate) compaction_filters: Arc<Mutex<Vec<CompactionFilter>>>,
}

/// A thin wrapper for `LsmStorageInner` and the user interface for MiniLSM.
pub struct MiniLsm {
    pub(crate) inner: Arc<LsmStorageInner>,
    /// Notifies the L0 flush thread to stop working. (In week 1 day 6)
    flush_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the flush thread. (In week 1 day 6)
    flush_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Notifies the compaction thread to stop working. (In week 2)
    compaction_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the compaction thread. (In week 2)
    compaction_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Drop for MiniLsm {
    fn drop(&mut self) {
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();
    }
}

impl MiniLsm {
    pub fn close(&self) -> Result<()> {
        self.inner.sync_dir()?;
        // 向合并线程发送停止信号
        self.compaction_notifier.send(()).ok();
        // 向转储线程发送停止信号
        self.flush_notifier.send(()).ok();

        let mut compaction_thread = self.compaction_thread.lock();
        if let Some(compaction_thread) = compaction_thread.take() {
            compaction_thread
                .join()
                .map_err(|e| anyhow::anyhow!("{:?}", e))?;
        }
        let mut flush_thread = self.flush_thread.lock();
        if let Some(flush_thread) = flush_thread.take() {
            flush_thread
                .join()
                .map_err(|e| anyhow::anyhow!("{:?}", e))?;
        }

        if self.inner.options.enable_wal {
            self.inner.sync()?;
            self.inner.sync_dir()?;
            return Ok(());
        }

        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .force_freeze_memtable(&self.inner.state_lock.lock())?;
        }

        while {
            let snapshot = self.inner.state.read();
            !snapshot.imm_memtables.is_empty()
        } {
            self.inner.force_flush_next_imm_memtable()?;
        }
        self.inner.sync_dir()?;
        Ok(())
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Arc<Self>> {
        let inner = Arc::new(LsmStorageInner::open(path, options)?);
        let (tx1, rx) = crossbeam_channel::unbounded();
        let compaction_thread = inner.spawn_compaction_thread(rx)?;
        let (tx2, rx) = crossbeam_channel::unbounded();
        let flush_thread = inner.spawn_flush_thread(rx)?;
        Ok(Arc::new(Self {
            inner,
            flush_notifier: tx2,
            flush_thread: Mutex::new(flush_thread),
            compaction_notifier: tx1,
            compaction_thread: Mutex::new(compaction_thread),
        }))
    }

    pub fn new_txn(&self) -> Result<Arc<Transaction>> {
        self.inner.new_txn()
    }

    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        self.inner.write_batch(batch)
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        self.inner.add_compaction_filter(compaction_filter)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put(key, value)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.delete(key)
    }

    pub fn sync(&self) -> Result<()> {
        self.inner.sync()
    }

    pub fn scan(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<TxnIterator> {
        self.inner.scan(lower, upper)
    }

    /// Only call this in test cases due to race conditions
    pub fn force_flush(&self) -> Result<()> {
        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .force_freeze_memtable(&self.inner.state_lock.lock())?;
        }
        if !self.inner.state.read().imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        self.inner.force_full_compaction()
    }
}

impl LsmStorageInner {
    pub(crate) fn next_sst_id(&self) -> usize {
        self.next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub(crate) fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self> {
        let mut last_commit_ts = 0;
        let path = path.as_ref();
        if !path.exists() {
            std::fs::create_dir_all(path)?;
        }
        let mut state = LsmStorageState::create(&options);

        let compaction_controller = match &options.compaction_options {
            CompactionOptions::Leveled(options) => {
                CompactionController::Leveled(LeveledCompactionController::new(options.clone()))
            }
            CompactionOptions::Tiered(options) => {
                CompactionController::Tiered(TieredCompactionController::new(options.clone()))
            }
            CompactionOptions::Simple(options) => CompactionController::Simple(
                SimpleLeveledCompactionController::new(options.clone()),
            ),
            CompactionOptions::NoCompaction => CompactionController::NoCompaction,
        };

        let manifest_path = path.join("MANIFEST");
        let manifest;
        let block_cache = Arc::new(BlockCache::new(1 << 20));
        let mut next_sst_id = 1;
        if !manifest_path.exists() {
            if options.enable_wal {
                state.memtable = Arc::new(MemTable::create_with_wal(
                    state.memtable.id(),
                    Self::path_of_wal_static(path, state.memtable.id()),
                )?);
            }
            manifest = Manifest::create(manifest_path)?;
            manifest.add_record_when_init(ManifestRecord::NewMemtable(state.memtable.id()))?;
        } else {
            let (old_manifest, records) = Manifest::recover(&manifest_path)?;
            let mut memtables = BTreeSet::new();
            for record in records {
                let record_sst_next;
                match record {
                    ManifestRecord::Flush(sst_id) => {
                        let res = memtables.remove(&sst_id);
                        assert!(res, "memtable not exist?");
                        if compaction_controller.flush_to_l0() {
                            state.l0_sstables.insert(0, sst_id);
                        } else {
                            state.levels.insert(0, (sst_id, vec![sst_id]));
                        }
                        record_sst_next = sst_id + 1;
                    }
                    ManifestRecord::NewMemtable(x) => {
                        record_sst_next = x + 1;
                        memtables.insert(x);
                    }
                    ManifestRecord::Compaction(task, output) => {
                        let (new_state, _) = compaction_controller
                            .apply_compaction_result(&state, &task, &output, true);
                        state = new_state;
                        record_sst_next = output.iter().max().copied().unwrap_or_default() + 1;
                    }
                }
                next_sst_id = next_sst_id.max(record_sst_next);
            }

            let block_cache = Arc::new(BlockCache::new(1 << 20));

            //恢复已持久化的sstables
            for sst_id in state
                .l0_sstables
                .iter()
                .chain(state.levels.iter().map(|(_, ssts)| ssts).flatten())
            {
                let sst = SsTable::open(
                    *sst_id,
                    Some(block_cache.clone()),
                    FileObject::open(&Self::path_of_sst_static(path, *sst_id))
                        .context("failed to open SST")?,
                )?;
                last_commit_ts = last_commit_ts.max(sst.max_ts());
                state.sstables.insert(*sst_id, Arc::new(sst));
            }

            //回复未被持久化的memtable
            if options.enable_wal {
                let mut wal_cnt = 0;
                for id in memtables.iter() {
                    let (memtable, max_ts) =
                        MemTable::recover_from_wal(*id, Self::path_of_wal_static(path, *id))?;
                    if !memtable.is_empty() {
                        state.imm_memtables.insert(0, Arc::new(memtable));
                        wal_cnt += 1;
                    }
                    last_commit_ts = last_commit_ts.max(max_ts);
                }
                println!("{} WALs recovered", wal_cnt);
                state.memtable = Arc::new(MemTable::create_with_wal(
                    next_sst_id,
                    Self::path_of_wal_static(path, next_sst_id),
                )?);
            } else {
                state.memtable = Arc::new(MemTable::create(next_sst_id));
            }
            old_manifest.add_record_when_init(ManifestRecord::NewMemtable(state.memtable.id()))?;
            manifest = old_manifest;
            next_sst_id += 1;
        }

        // Sort SSTs on each level (only for leveled compaction)
        if let CompactionController::Leveled(_) = &compaction_controller {
            for (_id, ssts) in &mut state.levels {
                ssts.sort_by(|x, y| {
                    state
                        .sstables
                        .get(x)
                        .unwrap()
                        .first_key()
                        .cmp(state.sstables.get(y).unwrap().first_key())
                })
            }
        }

        let storage = Self {
            state: Arc::new(RwLock::new(Arc::new(state))),
            state_lock: Mutex::new(()),
            path: path.to_path_buf(),
            block_cache,
            next_sst_id: AtomicUsize::new(next_sst_id),
            compaction_controller,
            manifest,
            options: options.into(),
            mvcc: LsmMvccInner::new(last_commit_ts),
            compaction_filters: Arc::new(Mutex::new(Vec::new())),
        };
        storage.sync_dir()?;

        Ok(storage)
    }

    ///刷新memtable
    pub fn sync(&self) -> Result<()> {
        self.state.read().memtable.sync_wal()
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        let mut compaction_filters = self.compaction_filters.lock();
        compaction_filters.push(compaction_filter);
    }

    pub fn get<'a>(self: &'a Arc<Self>, key: &[u8]) -> Result<Option<Bytes>> {
        let txn = self.new_txn()?;
        txn.get(key)
    }

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    pub fn get_with_ts(&self, key: &[u8], ts: u64) -> Result<Option<Bytes>> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };

        let memtable_iter = snapshot.memtable.scan(
            Bound::Included(KeySlice::from_slice(key, ts)),
            Bound::Included(KeySlice::from_slice(key, key::TS_RANGE_END)),
        );
        if memtable_iter.is_valid() && memtable_iter.key().key_ref() == key {
            if memtable_iter.value().is_empty() {
                return Ok(None);
            }
            return Ok(Some(Bytes::copy_from_slice(memtable_iter.value())));
        }

        for memtable in &snapshot.imm_memtables {
            let memtable_iter = memtable.scan(
                Bound::Included(KeySlice::from_slice(key, ts)),
                Bound::Included(KeySlice::from_slice(key, key::TS_RANGE_END)),
            );
            if memtable_iter.is_valid() && memtable_iter.key().key_ref() == key {
                if memtable_iter.value().is_empty() {
                    return Ok(None);
                }

                return Ok(Some(Bytes::copy_from_slice(memtable_iter.value())));
            }
        }

        for table in snapshot.l0_sstables.iter() {
            let table = snapshot.sstables[table].clone();
            if table.bloom.is_some()
                && !table
                    .bloom
                    .as_ref()
                    .unwrap()
                    .may_contain(farmhash::fingerprint32(key))
            {
                continue;
            }

            let iter =
                SsTableIterator::create_and_seek_to_key(table, KeySlice::from_slice(key, ts))?;
            if iter.is_valid() && iter.key().key_ref() == key {
                if iter.value().is_empty() {
                    return Ok(None);
                }
                return Ok(Some(Bytes::copy_from_slice(iter.value())));
            }
        }

        for (_, v) in snapshot.levels.iter() {
            let mut sst = vec![];
            for table in v.iter() {
                let table = snapshot.sstables[table].clone();
                if table.bloom.is_some()
                    && !table
                        .bloom
                        .as_ref()
                        .unwrap()
                        .may_contain(farmhash::fingerprint32(key))
                {
                    continue;
                }
                sst.push(table.clone());
            }
            let iter =
                SstConcatIterator::create_and_seek_to_key(sst, KeySlice::from_slice(key, ts))?;
            if iter.is_valid() && iter.key().key_ref() == key {
                if iter.value().is_empty() {
                    return Ok(None);
                }
                return Ok(Some(Bytes::copy_from_slice(iter.value())));
            }
        }
        Ok(None)
    }

    fn write_batch_inner(&self, key: KeySlice, value: &[u8]) -> Result<()> {
        let _ = self.state.read().memtable.put(key, value);
        let approximate_size = self.state.read().memtable.approximate_size();
        if approximate_size >= self.options.target_sst_size {
            let state_lock = self.state_lock.lock();
            let guard = self.state.read();
            if guard.memtable.approximate_size() >= self.options.target_sst_size {
                drop(guard);
                self.force_freeze_memtable(&state_lock)?;
            }
        }
        Ok(())
    }

    pub fn no_serializable_write_batch<T: AsRef<[u8]>>(
        &self,
        batch: &[WriteBatchRecord<T>],
    ) -> Result<u64> {
        let _lock = self.mvcc.write_lock.lock();
        let ts = self.mvcc.latest_commit_ts() + 1;
        for record in batch {
            match record {
                WriteBatchRecord::Put(key, value) => {
                    self.write_batch_inner(KeySlice::from_slice(key.as_ref(), ts), value.as_ref())?;
                }
                WriteBatchRecord::Del(key) => {
                    self.write_batch_inner(KeySlice::from_slice(key.as_ref(), ts), b"")?;
                }
            }
        }
        self.mvcc.update_commit_ts(ts);
        Ok(ts)
    }

    pub fn write_batch<T: AsRef<[u8]>>(
        self: &Arc<Self>,
        batch: &[WriteBatchRecord<T>],
    ) -> Result<()> {
        if !self.options.serializable {
            self.no_serializable_write_batch(batch)?;
        } else {
            let txn = self.mvcc.new_txn(self.clone(), self.options.serializable);
            for record in batch {
                match record {
                    WriteBatchRecord::Del(key) => {
                        txn.delete(key.as_ref());
                    }
                    WriteBatchRecord::Put(key, value) => {
                        txn.put(key.as_ref(), value.as_ref());
                    }
                }
            }
            txn.commit()?;
        }
        Ok(())
    }

    pub fn put(self: &Arc<Self>, key: &[u8], value: &[u8]) -> Result<()> {
        if !self.options.serializable {
            self.no_serializable_write_batch(&[WriteBatchRecord::Put(key, value)])?;
        } else {
            let txn = self.mvcc.new_txn(self.clone(), self.options.serializable);
            txn.put(key, value);
            txn.commit()?;
        }
        Ok(())
    }

    pub fn delete(self: &Arc<Self>, key: &[u8]) -> Result<()> {
        if !self.options.serializable {
            self.no_serializable_write_batch(&[WriteBatchRecord::Del(key)])?;
        } else {
            let txn = self.mvcc.new_txn(self.clone(), self.options.serializable);
            txn.delete(key);
            txn.commit()?;
        }
        Ok(())
    }

    pub(crate) fn path_of_sst_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.sst", id))
    }

    pub(crate) fn path_of_sst(&self, id: usize) -> PathBuf {
        Self::path_of_sst_static(&self.path, id)
    }

    pub(crate) fn path_of_wal_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.wal", id))
    }

    pub(crate) fn path_of_wal(&self, id: usize) -> PathBuf {
        Self::path_of_wal_static(&self.path, id)
    }

    pub(super) fn sync_dir(&self) -> Result<()> {
        File::open(&self.path)?.sync_all()?;
        Ok(())
    }

    /// Force freeze the current memtable to an immutable memtable
    pub fn force_freeze_memtable(&self, _state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {
        let new_memtable_id = self.next_sst_id();
        let new_men_table: Arc<MemTable> = if self.options.enable_wal {
            Arc::new(MemTable::create_with_wal(
                new_memtable_id,
                self.path_of_wal(new_memtable_id),
            )?)
        } else {
            Arc::new(MemTable::create(new_memtable_id))
        };
        {
            let mut guard = self.state.write();
            let mut snapshot = guard.as_ref().clone();
            let old_men_table = std::mem::replace(&mut snapshot.memtable, new_men_table.clone());
            snapshot.imm_memtables.insert(0, old_men_table);
            snapshot.memtable = new_men_table;
            *guard = Arc::new(snapshot)
        }

        let _ = self.manifest.add_record(
            _state_lock_observer,
            ManifestRecord::NewMemtable(new_memtable_id),
        );

        Ok(())
    }

    pub fn force_flush_next_imm_memtable(&self) -> Result<()> {
        let _guard = self.state_lock.lock();
        let imm_memtable = {
            let guard = self.state.read();
            let memtable_to_flush = guard
                .imm_memtables
                .last()
                .expect("no imm memtables!")
                .clone();
            memtable_to_flush
        };

        let sst_id = imm_memtable.id();
        let sst_path = self.path_of_sst(sst_id);
        let mut builder = crate::table::SsTableBuilder::new(self.options.block_size);
        imm_memtable.flush(&mut builder)?;
        let sst = builder.build(sst_id, Some(self.block_cache.clone()), sst_path)?;

        {
            let mut guard = self.state.write();
            let mut snapshot = guard.as_ref().clone();
            snapshot.imm_memtables.pop();
            if self.compaction_controller.flush_to_l0() {
                // In leveled compaction or no compaction, simply flush to L0
                snapshot.l0_sstables.insert(0, sst.sst_id());
            } else {
                // In tiered compaction, create a new tier
                snapshot
                    .levels
                    .insert(0, (sst.sst_id(), vec![sst.sst_id()]));
            }
            snapshot.sstables.insert(sst_id, Arc::new(sst));
            *guard = Arc::new(snapshot);
        }

        self.manifest
            .add_record(&_guard, ManifestRecord::Flush(sst_id))?;

        if self.options.enable_wal {
            std::fs::remove_file(self.path_of_wal(sst_id))?;
        }
        Ok(())
    }

    pub fn new_txn(self: &Arc<Self>) -> Result<Arc<Transaction>> {
        Ok(self.mvcc.new_txn(self.clone(), self.options.serializable))
    }

    pub fn scan<'a>(
        self: &'a Arc<Self>,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<TxnIterator> {
        let txn = self.new_txn()?;
        txn.scan(lower, upper)
    }

    /// Create an iterator over a range of keys.
    pub fn scan_with_ts(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
        ts: u64,
    ) -> Result<FusedIterator<LsmIterator>> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };
        let mut memtable_iters = Vec::with_capacity(snapshot.imm_memtables.len() + 1);
        memtable_iters.push(Box::new(snapshot.memtable.scan(
            map_key_bound_plus_ts(lower, ts),
            map_key_bound_plus_ts(upper, key::TS_RANGE_END),
        )));
        for memtable in snapshot.imm_memtables.iter() {
            memtable_iters.push(Box::new(memtable.scan(
                map_key_bound_plus_ts(lower, ts),
                map_key_bound_plus_ts(upper, key::TS_RANGE_END),
            )));
        }
        let merge_memtable_iter = MergeIterator::create(memtable_iters);

        let mut l0_iters = Vec::new();
        for sstable_id in snapshot.l0_sstables.iter() {
            let table = snapshot.sstables[sstable_id].clone();
            if range_overlap(
                lower,
                upper,
                table.first_key().as_key_slice(),
                table.last_key().as_key_slice(),
            ) {
                let iter = match lower {
                    Bound::Included(key) => SsTableIterator::create_and_seek_to_key(
                        table,
                        key::Key::from_slice(key, key::TS_RANGE_BEGIN),
                    )?,
                    Bound::Excluded(key) => {
                        let mut iter = SsTableIterator::create_and_seek_to_key(
                            table,
                            key::Key::from_slice(key, key::TS_RANGE_BEGIN),
                        )?;
                        if iter.is_valid() && iter.key().key_ref() == key {
                            iter.next()?;
                        }
                        iter
                    }
                    Bound::Unbounded => SsTableIterator::create_and_seek_to_first(table)?,
                };
                l0_iters.push(Box::new(iter));
            }
        }
        let merge_l0_iter = MergeIterator::create(l0_iters);

        let mut level_iters = Vec::new();
        for i in 0..snapshot.levels.len() {
            let mut ssts = Vec::with_capacity(snapshot.levels[i].1.len());
            for id in snapshot.levels[i].1.iter() {
                let table = snapshot.sstables[id].clone();
                if range_overlap(
                    lower,
                    upper,
                    table.first_key().as_key_slice(),
                    table.last_key().as_key_slice(),
                ) {
                    ssts.push(table);
                }
            }

            let iter = match lower {
                Bound::Included(key) => SstConcatIterator::create_and_seek_to_key(
                    ssts,
                    key::Key::from_slice(key, key::TS_RANGE_BEGIN),
                )?,
                Bound::Excluded(key) => {
                    let mut iter = SstConcatIterator::create_and_seek_to_key(
                        ssts,
                        key::Key::from_slice(key, key::TS_RANGE_BEGIN),
                    )?;
                    if iter.is_valid() && iter.key().key_ref() == key {
                        iter.next()?;
                    }
                    iter
                }
                Bound::Unbounded => SstConcatIterator::create_and_seek_to_first(ssts)?,
            };
            level_iters.push(Box::new(iter));
        }
        let merge_level_iter = MergeIterator::create(level_iters);
        let iter = TwoMergeIterator::create(merge_memtable_iter, merge_l0_iter)?;
        let iter = TwoMergeIterator::create(iter, merge_level_iter)?;
        Ok(FusedIterator::new(LsmIterator::new(
            iter,
            map_bound(upper),
            ts,
        )?))
    }
}
fn range_overlap(
    user_begin: Bound<&[u8]>,
    user_end: Bound<&[u8]>,
    table_begin: KeySlice,
    table_end: KeySlice,
) -> bool {
    match user_end {
        Bound::Excluded(key) if key <= table_begin.key_ref() => {
            return false;
        }
        Bound::Included(key) if key < table_begin.key_ref() => {
            return false;
        }
        _ => {}
    }
    match user_begin {
        Bound::Excluded(key) if key >= table_end.key_ref() => {
            return false;
        }
        Bound::Included(key) if key > table_end.key_ref() => {
            return false;
        }
        _ => {}
    }
    true
}
