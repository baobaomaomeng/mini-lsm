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

use bytes::BufMut;

use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        return Self {
            offsets: vec![],
            data: vec![],
            block_size,
            first_key: KeyVec::new(),
        };
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        // 计算新条目的总大小
        let entry_size = 4 + key.len() + value.len(); // key_len + key + value_len + value
        let data_len = self.data.len();
        let mut add_entry = || {
            self.offsets.push(self.data.len() as u16);
            self.data.put_u16(key.len() as u16);
            self.data.extend_from_slice(key.raw_ref());
            self.data.put_u16(value.len() as u16);
            self.data.extend_from_slice(value);
        };

        if data_len == 0 {
            // 第一个条目，不在乎大小是否超出
            self.first_key.set_from_slice(key);
            add_entry();
            return true;
        }

        // 检查是否超出块大小限制
        if data_len + entry_size + 2 > self.block_size {
            return false;
        }
        add_entry();
        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
