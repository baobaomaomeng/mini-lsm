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

use bytes::Buf;

use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the current value range in the block.data, corresponds to the current key
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
            first_key: KeyVec::new(),
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut iter = BlockIterator::new(block);
        iter.seek_to_first();
        iter
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut iter = BlockIterator::new(block);
        iter.seek_to_key(key);
        iter
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice {
        self.key.as_key_slice()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        &self.block.data[self.value_range.0..self.value_range.1]
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        !self.key().is_empty()
    }

    //得到对应idx的key，返回key值结尾的位置方便得到value
    fn get_key(&mut self, star: usize) -> usize {
        let mut entry = &self.block.data[star..];
        // 解码key的长度，get_u16会自动向后移动2个字节
        let key_len = entry.get_u16() as usize;
        let key = &entry[..key_len];
        self.key.clear();
        self.key.append(key);

        star + 2 + key_len
    }

    fn get_value(&mut self, star: usize) {
        let value_len = (&self.block.data[star..star + 2]).get_u16() as usize;
        self.value_range = (star + 2, star + 2 + value_len);
    }

    fn get_idx_kv(&mut self, index: usize) {
        assert!(index < self.block.offsets.len());
        self.idx = index;
        let key_star = self.block.offsets[index] as usize;
        let value_star = self.get_key(key_star);
        self.get_value(value_star);
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.get_idx_kv(0);
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        self.idx += 1;
        if self.idx >= self.block.offsets.len() {
            self.key = KeyVec::new();
            return;
        }
        self.get_idx_kv(self.idx);
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        self.seek_to_first();
        let mut result = self.key().cmp(&key);
        while result == std::cmp::Ordering::Less {
            self.next();
            if !self.is_valid() {
                break;
            };
            result = self.key().cmp(&key);
        }
    }
}
