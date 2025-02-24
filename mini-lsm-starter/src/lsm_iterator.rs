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

use std::ops::Bound;

use anyhow::{bail, Ok, Result};
use bytes::Bytes;

use crate::{
    iterators::{
        concat_iterator::SstConcatIterator, merge_iterator::MergeIterator,
        two_merge_iterator::TwoMergeIterator, StorageIterator,
    },
    mem_table::MemTableIterator,
    table::SsTableIterator,
};

/// Represents the internal type for an LSM iterator. This type will be changed across the tutorial for multiple times.
type LsmIteratorInner = TwoMergeIterator<
    TwoMergeIterator<MergeIterator<MemTableIterator>, MergeIterator<SsTableIterator>>,
    MergeIterator<SstConcatIterator>,
>;

pub struct LsmIterator {
    inner: LsmIteratorInner,
    upper: Bound<Bytes>,
    prev_key: Vec<u8>,
    read_ts: u64,
}

impl LsmIterator {
    pub(crate) fn new(iter: LsmIteratorInner, upper: Bound<Bytes>, read_ts: u64) -> Result<Self> {
        let mut lsm = Self {
            inner: iter,
            upper,
            prev_key: Vec::new(),
            read_ts,
        };
        if lsm.is_valid() {
            lsm.prev_key = lsm.key().to_vec();
            if lsm.value().is_empty() || lsm.inner.key().ts() > read_ts {
                let _ = lsm.next();
            }
        }
        Ok(lsm)
    }
}

impl StorageIterator for LsmIterator {
    type KeyType<'a> = &'a [u8];

    fn is_valid(&self) -> bool {
        if !self.inner.is_valid() {
            return false;
        }
        match &self.upper {
            Bound::Included(key) => self.inner.key().key_ref() <= key,
            Bound::Excluded(key) => self.inner.key().key_ref() < key,
            Bound::Unbounded => true,
        }
    }

    fn key(&self) -> &[u8] {
        self.inner.key().key_ref()
    }

    fn value(&self) -> &[u8] {
        self.inner.value()
    }

    fn num_active_iterators(&self) -> usize {
        self.inner.num_active_iterators()
    }

    fn next(&mut self) -> Result<()> {
        while self.inner.next().is_ok() {
            if !self.inner.is_valid() {
                break;
            }
            if self.inner.value().is_empty() || self.inner.key().ts() > self.read_ts {
                continue;
            }
            if self.prev_key != self.key().to_vec() {
                self.prev_key = self.key().to_vec();
                break;
            }
        }
        Ok(())
    }
}

/// A wrapper around existing iterator, will prevent users from calling `next` when the iterator is
/// invalid. If an iterator is already invalid, `next` does not do anything. If `next` returns an error,
/// `is_valid` should return false, and `next` should always return an error.
pub struct FusedIterator<I: StorageIterator> {
    iter: I,
    has_errored: bool,
}

impl<I: StorageIterator> FusedIterator<I> {
    pub fn new(iter: I) -> Self {
        Self {
            iter,
            has_errored: false,
        }
    }
}

impl<I: StorageIterator> StorageIterator for FusedIterator<I> {
    type KeyType<'a>
        = I::KeyType<'a>
    where
        Self: 'a;

    fn is_valid(&self) -> bool {
        //注意需要把is_valid放在前面，因为has_errored为true时，is_valid会触发panic
        !self.has_errored && self.iter.is_valid()
    }

    fn key(&self) -> Self::KeyType<'_> {
        if !self.is_valid() {
            panic!("invalid access to the underlying iterator");
        }
        self.iter.key()
    }

    fn value(&self) -> &[u8] {
        if !self.is_valid() {
            panic!("invalid access to the underlying iterator");
        }
        self.iter.value()
    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }

    fn next(&mut self) -> Result<()> {
        if self.has_errored {
            bail!("the iterator is tainted");
        }
        if self.iter.is_valid() {
            if let Err(e) = self.iter.next() {
                println!("return error");
                self.has_errored = true;
                return Err(e);
            }
        }
        Ok(())
    }
}
