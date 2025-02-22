#![allow(dead_code)]
// REMOVE THIS LINE after fully implementing this functionality
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
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use bytes::{Buf, BufMut, Bytes};
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;

use crate::key::{KeyBytes, KeySlice};

pub struct Wal {
    file: Arc<Mutex<BufWriter<File>>>,
}

impl Wal {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(
                OpenOptions::new()
                    .read(true)
                    .create_new(true)
                    .write(true)
                    .open(path)
                    .context("failed to create wal")?,
            ))),
        })
    }

    pub fn recover(path: impl AsRef<Path>, skiplist: &SkipMap<KeyBytes, Bytes>) -> Result<Self> {
        let mut buf = Vec::new();
        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(path)
            .context("failed to recover from WAL")?;

        file.read_to_end(&mut buf)?;
        let mut buf = buf.as_slice();

        while buf.has_remaining() {
            let start = buf;
            let key_len = buf.get_u16() as usize;
            let key = KeyBytes::from_bytes_with_ts(buf.copy_to_bytes(key_len), buf.get_u64());
            let value_len = buf.get_u16() as usize;
            let value = Bytes::copy_from_slice(&buf[..value_len]);
            buf.advance(value_len);
            let checksum = buf.get_u32();
            skiplist.insert(key, value);

            if checksum != crc32fast::hash(&start[..2 + key_len + 2 + value_len]) {
                bail!("checksum mismatched for wal key-value");
            }
        }

        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }
    pub fn put(&self, key: KeySlice, value: &[u8]) -> Result<()> {
        let len = std::mem::size_of::<u16>()
            + key.key_len()
            + std::mem::size_of::<u16>()
            + value.len()
            + std::mem::size_of::<u32>()
            + std::mem::size_of::<u64>();
        let mut buf = Vec::with_capacity(len);

        let key_len = key.key_len();
        buf.put_u16(key_len as u16);
        buf.put_slice(key.key_ref());
        buf.put_u64(key.ts());
        let value_len = value.len().try_into().unwrap();
        buf.put_u16(value_len);
        buf.put_slice(value);

        let checksum = crc32fast::hash(&buf);
        buf.put_u32(checksum);

        debug_assert_eq!(buf.len(), len);

        let mut file = self.file.lock();
        file.write_all(&buf)?;

        Ok(())
    }

    /// Implement this in week 3, day 5.
    pub fn put_batch(&self, _data: &[(&[u8], &[u8])]) -> Result<()> {
        unimplemented!()
    }

    pub fn sync(&self) -> Result<()> {
        let mut file = self.file.lock();
        file.flush()?;
        file.get_mut().sync_all()?;
        Ok(())
    }
}
