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
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::BufMut;

use super::{bloom::Bloom, BlockMeta, SsTable};
use crate::{
    block::BlockBuilder,
    key::{KeySlice, KeyVec},
    lsm_storage::BlockCache,
    table::FileObject,
};

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: KeyVec,
    last_key: KeyVec,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    pub(crate) key_hash: Vec<u32>,
    block_size: usize,
    max_ts: u64,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            builder: BlockBuilder::new(block_size),
            first_key: KeyVec::new(),
            last_key: KeyVec::new(),
            data: Vec::new(),
            meta: Vec::new(),
            key_hash: Vec::new(),
            block_size,
            max_ts: 0,
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)

    fn clear_keys(&mut self) {
        self.first_key.clear();
        self.last_key.clear();
    }

    fn finish_block(&mut self) {
        let old_builder = std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));
        let encoded_block = old_builder.build().encode();
        // 记录旧 block 在 self.data 中的起始偏移
        let offset = self.data.len();
        let checksum = crc32fast::hash(&encoded_block);
        self.data.append(&mut encoded_block.to_vec());
        self.data.put_u32(checksum);
        // 将旧 block 的元信息加入到 meta
        self.meta.push(BlockMeta {
            offset: offset as usize,
            first_key: self.first_key.clone().into_key_bytes(),
            last_key: self.last_key.clone().into_key_bytes(),
        });
        self.clear_keys();
    }

    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        if self.first_key.is_empty() {
            // 如果这是整个 Block 的第一条记录，保存下来
            self.first_key = key.to_key_vec();
        }
        self.max_ts = self.max_ts.max(key.ts());

        // 先尝试往当前 block 中插入
        // 如果返回 false，表示加入失败，需要对当前 block 进行封装并创建新的 block
        if !self.builder.add(key, value) {
            self.finish_block();
            self.first_key = key.to_key_vec();
            self.last_key = key.to_key_vec();
            // 因为刚才 add 失败，需要在新的 builder 中再次插入这条 key-value
            let _ = self.builder.add(key, value);
        }

        // 最后更新一下 last_key
        self.last_key = key.to_key_vec();
        self.key_hash.push(farmhash::fingerprint32(key.key_ref()));
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }

    pub fn have_data_to_build(&self) -> bool {
        !self.builder.is_empty()
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        &mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        if !self.builder.is_empty() {
            self.finish_block();
        }
        let mut buf: Vec<u8> = vec![];
        buf.extend_from_slice(self.data.as_slice());
        BlockMeta::encode_block_meta(self.meta.as_slice(), self.max_ts, &mut buf);
        buf.put_u32(self.data.len() as u32);
        //添加bloom
        let bloom = Bloom::build_from_key_hashes(
            &self.key_hash,
            Bloom::bloom_bits_per_key(self.key_hash.len(), 0.01),
        );
        let bloom_offset = buf.len();
        bloom.encode(&mut buf);
        buf.put_u32(bloom_offset as u32);
        let file = FileObject::create(path.as_ref(), buf)?;
        Ok(SsTable {
            id,
            block_cache,
            file,
            block_meta: self.meta.clone(),
            block_meta_offset: self.data.len(),
            first_key: self.meta[0].clone().first_key,
            last_key: self.meta[self.meta.len() - 1].clone().last_key,
            bloom: Some(bloom),
            max_ts: 0,
        })
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(&mut self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
