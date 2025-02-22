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

use anyhow::Result;

use super::StorageIterator;

/// Merges two iterators of different types into one. If the two iterators have the same key, only
/// produce the key once and prefer the entry from A.
pub struct TwoMergeIterator<A: StorageIterator, B: StorageIterator> {
    a: A,
    b: B,
    // true if mem_table, false if sstable
    mem_table_or_sstable: bool,
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > TwoMergeIterator<A, B>
{
    pub fn choose(&mut self) {
        if !self.a.is_valid() {
            self.mem_table_or_sstable = false;
            return;
        } else if !self.b.is_valid() {
            self.mem_table_or_sstable = true;
            return;
        }
        self.mem_table_or_sstable = self.a.key() <= self.b.key();
    }

    pub fn create(a: A, b: B) -> Result<Self> {
        let mut result = Self {
            a,
            b,
            mem_table_or_sstable: true,
        };
        result.choose();
        Ok(result)
    }
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > StorageIterator for TwoMergeIterator<A, B>
{
    type KeyType<'a> = A::KeyType<'a>;

    fn key(&self) -> Self::KeyType<'_> {
        if self.mem_table_or_sstable {
            self.a.key()
        } else {
            self.b.key()
        }
    }

    fn value(&self) -> &[u8] {
        if self.mem_table_or_sstable {
            self.a.value()
        } else {
            self.b.value()
        }
    }

    fn is_valid(&self) -> bool {
        if self.mem_table_or_sstable {
            self.a.is_valid()
        } else {
            self.b.is_valid()
        }
    }

    fn num_active_iterators(&self) -> usize {
        self.a.num_active_iterators() + self.b.num_active_iterators()
    }

    fn next(&mut self) -> Result<()> {
        if self.a.is_valid() && self.b.is_valid() && self.a.key() == self.b.key() {
            self.b.next()?;
        }
        if self.mem_table_or_sstable {
            self.a.next()?;
        } else {
            self.b.next()?;
        }
        self.choose();
        Ok(())
    }
}
impl<A: StorageIterator, B: StorageIterator> TwoMergeIterator<A, B> {
    fn get_two_key(&self) -> (A::KeyType<'_>, B::KeyType<'_>) {
        (self.a.key(), self.b.key())
    }
}
