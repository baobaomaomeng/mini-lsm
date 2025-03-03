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

use std::sync::Arc;

use anyhow::Result;

use super::SsTable;
use crate::{
    block::BlockIterator,
    iterators::StorageIterator,
    key::{self, KeySlice},
};

/// An iterator over the contents of an SSTable.
pub struct SsTableIterator {
    table: Arc<SsTable>,
    blk_iter: BlockIterator,
    blk_idx: usize,
}

impl SsTableIterator {
    /// Create a new iterator and seek to the first key-value pair in the first data block.
    pub fn create_and_seek_to_first(table: Arc<SsTable>) -> Result<Self> {
        let iter = Self {
            table: table.clone(),
            blk_iter: BlockIterator::create_and_seek_to_first(table.read_block_cached(0)?),
            blk_idx: 0,
        };
        Ok(iter)
    }

    /// Seek to the first key-value pair in the first data block.
    pub fn seek_to_first(&mut self) -> Result<()> {
        self.blk_idx = 0;
        self.blk_iter = BlockIterator::create_and_seek_to_first(self.table.read_block_cached(0)?);
        Ok(())
    }

    /// Create a new iterator and seek to the first key-value pair which >= `key`.
    pub fn create_and_seek_to_key(table: Arc<SsTable>, key: KeySlice) -> Result<Self> {
        let mut idx = table.find_block_idx(key);
        let mut block_iter =
            BlockIterator::create_and_seek_to_key(table.read_block_cached(idx)?, key);
        if !block_iter.is_valid() && idx != table.num_of_blocks() - 1 {
            idx += 1;
            block_iter = BlockIterator::create_and_seek_to_first(table.read_block_cached(idx)?);
        }
        let iter = Self {
            table: table.clone(),
            blk_iter: block_iter,
            blk_idx: idx,
        };
        Ok(iter)
    }

    /// Seek to the first key-value pair which >= `key`.
    /// Note: You probably want to review the handout for detailed explanation when implementing
    /// this function.
    pub fn seek_to_key(&mut self, key: KeySlice) -> Result<()> {
        let mut index = self.table.find_block_idx(key);
        println!("seekindex: {}", index);
        self.blk_iter = BlockIterator::create_and_seek_to_key(self.table.read_block(index)?, key);
        if !self.blk_iter.is_valid() && index != self.table.num_of_blocks() - 1 {
            index += 1;
            println!("index add: {}", index);
            self.blk_iter = BlockIterator::create_and_seek_to_first(self.table.read_block(index)?);
        }
        self.blk_idx = index;
        Ok(())
    }
}

impl StorageIterator for SsTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    /// Return the `key` that's held by the underlying block iterator.
    fn key(&self) -> KeySlice {
        self.blk_iter.key()
    }

    /// Return the `value` that's held by the underlying block iterator.
    fn value(&self) -> &[u8] {
        self.blk_iter.value()
    }

    /// Return whether the current block iterator is valid or not.
    fn is_valid(&self) -> bool {
        self.blk_iter.is_valid()
    }

    /// Move to the next `key` in the block.
    /// Note: You may want to check if the current block iterator is valid after the move.
    fn next(&mut self) -> Result<()> {
        self.blk_iter.next();
        if !self.is_valid() && self.blk_idx != self.table.num_of_blocks() - 1 {
            self.blk_idx += 1;
            self.blk_iter =
                BlockIterator::create_and_seek_to_first(self.table.read_block(self.blk_idx)?);
        }
        Ok(())
    }
}
