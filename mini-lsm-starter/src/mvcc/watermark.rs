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

use std::collections::{HashMap, VecDeque};

pub struct Watermark {
    dequeue: VecDeque<u64>,
    nums:HashMap<u64, usize>
}

impl Watermark {
    pub fn new() -> Self {
        Self {
            dequeue: VecDeque::new(),
            nums: HashMap::new(),
        }
    }

    pub fn add_reader(&mut self, ts: u64) {
        if let Some(cnt) = self.nums.get_mut(&ts) {
            *cnt += 1;
        } else {
            self.nums.insert(ts, 1);
        }
        self.dequeue.push_back(ts);
    }

    pub fn remove_reader(&mut self, ts: u64) {
        if let Some(cnt) = self.nums.get_mut(&ts) {
            *cnt -= 1;
            if *cnt == 0 {
                self.nums.remove(&ts);
                self.dequeue.pop_front();
            }
        }
    }

    pub fn watermark(&self) -> Option<u64> {
        if self.dequeue.is_empty() {
            None
        } else {
            Some(*self.dequeue.front().unwrap())
        }
    }
    
    pub fn num_retained_snapshots(&self) -> usize {
        self.nums.len()
    }
}
