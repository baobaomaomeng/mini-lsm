use bytes::BufMut;

use crate::key::{KeyBytes, KeySlice, KeyVec};

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
        Self {
            offsets: vec![],
            data: vec![],
            block_size,
            first_key: KeyVec::new(),
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        // 计算新条目的总大小
        let entry_size = 4 + key.len() as u64 + value.len() as u64; // key_len + key + value_len + value
        let data_len = self.data.len() as u64;
        let offsets_len = self.offsets.len() as u64 * 2;
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
        if data_len + offsets_len + 8 + entry_size > self.block_size as u64 {
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

    pub fn first_key(&self) -> KeyBytes {
        self.first_key.clone().into_key_bytes()
    }
}
