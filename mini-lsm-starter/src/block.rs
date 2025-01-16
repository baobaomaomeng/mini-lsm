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

mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::{Buf, BufMut, Bytes};
pub use iterator::BlockIterator;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the course
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let mut buf = Vec::new();
        buf.extend_from_slice(&self.data);
        // 将每个offset写入buf
        for offset in &self.offsets {
            buf.put_u16(*offset);
        }
        // 写入offset数组的长度
        buf.put_u16(self.offsets.len() as u16);
        buf.into()
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        // 从末尾读取offset数组的长度
        let num_offsets = (&data[data.len() - 2..]).get_u16() as usize;
        println!("{} || {}", num_offsets, data.len());
        // 计算offset数组的起始位置
        let offsets_start = data.len() - 2 - num_offsets * 2;
        println!("{}", offsets_start);
        // 提取数据部分
        let ans_data = data[..offsets_start].to_vec();
        // 提取并解析offset数组
        let mut offsets = Vec::with_capacity(num_offsets);
        for i in 0..num_offsets {
            let mut offset_bytes = &data[(offsets_start + i * 2)..(offsets_start + (i + 1) * 2)];
            offsets.push(offset_bytes.get_u16());
        }
        Self {
            data: ans_data,
            offsets,
        }
    }
}
