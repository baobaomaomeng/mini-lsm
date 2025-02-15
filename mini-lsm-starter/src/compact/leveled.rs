use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Serialize, Deserialize)]
pub struct LeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

#[derive(Debug, Clone)]
pub struct LeveledCompactionOptions {
    pub level_size_multiplier: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
    pub base_level_size_mb: usize,
}

pub struct LeveledCompactionController {
    options: LeveledCompactionOptions,
}

impl LeveledCompactionController {
    pub fn new(options: LeveledCompactionOptions) -> Self {
        Self { options }
    }

    fn find_overlapping_ssts(
        &self,
        snapshot: &LsmStorageState,
        sst_ids: &[usize],
        in_level: usize,
    ) -> Vec<usize> {
        // sst_ids中最小的key
        // 之所以不选第一个和最后一个sst是因为sst_ids可能是l0，未经排序
        let begin_key = sst_ids
            .iter()
            .map(|id| snapshot.sstables[id].first_key())
            .min()
            .cloned()
            .unwrap();
        // sst_ids中最大的key
        let end_key = sst_ids
            .iter()
            .map(|id| snapshot.sstables[id].last_key())
            .max()
            .cloned()
            .unwrap();

        let mut overlap_ssts = Vec::new();
        for sst_id in &snapshot.levels[in_level - 1].1 {
            let sst = &snapshot.sstables[sst_id];
            let first_key = sst.first_key();
            let last_key = sst.last_key();
            // 若in_level中的SST在范围中，则选取出来用于合并
            if !(last_key < &begin_key || first_key > &end_key) {
                overlap_ssts.push(*sst_id);
            }
        }
        overlap_ssts
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<LeveledCompactionTask> {
        let mut last_level_size: u64 = snapshot.levels[self.options.max_levels - 1]
            .1
            .iter()
            .map(|x| snapshot.sstables.get(x).unwrap().table_size() as u64)
            .sum();
        let mut target = (0..self.options.max_levels).map(|_| 0).collect::<Vec<_>>();
        let mut idx = self.options.max_levels;

        target[idx - 1] = last_level_size;
        while last_level_size > self.options.base_level_size_mb as u64 * 1024 * 1024 && idx > 1 {
            last_level_size /= self.options.level_size_multiplier as u64;
            idx -= 1;
            target[idx - 1] = last_level_size;
        }

        if snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
            println!("flush L0 SST to base level {}", idx);
            return Some(LeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: snapshot.l0_sstables.clone(),
                lower_level: idx,
                lower_level_sst_ids: self.find_overlapping_ssts(
                    snapshot,
                    &snapshot.l0_sstables,
                    idx,
                ),
                is_lower_level_bottom_level: idx == self.options.max_levels,
            });
        }

        let mut priority = 0.0f64;
        let mut select_level = 0;
        for num in 0..(self.options.max_levels - 1) {
            if target[num] == 0 {
                continue;
            }
            let current_size: u64 = snapshot.levels[num]
                .1
                .iter()
                .map(|x| snapshot.sstables.get(x).unwrap().table_size())
                .sum();
            let current_priority = current_size as f64 / target[num] as f64;
            if current_priority > priority {
                priority = current_priority;
                select_level = num + 1;
            }
        }

        if select_level > 0 {
            let oldest_sst = snapshot.levels[select_level - 1]
                .1
                .iter()
                .min()
                .copied()
                .unwrap();
            return Some(LeveledCompactionTask {
                upper_level: Some(select_level),
                upper_level_sst_ids: vec![oldest_sst],
                lower_level: select_level + 1,
                lower_level_sst_ids: self.find_overlapping_ssts(
                    snapshot,
                    &[oldest_sst],
                    select_level + 1,
                ),
                is_lower_level_bottom_level: (select_level + 1) == self.options.max_levels,
            });
        }

        None
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &LeveledCompactionTask,
        output: &[usize],
        _in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        let mut snapshot = snapshot.clone();
        let mut files_to_remove: Vec<usize> = Vec::new();

        let filter_ssts = |ssts: &Vec<usize>, ssts_set: &mut HashSet<usize>| {
            ssts.iter()
                .filter_map(|x| {
                    if ssts_set.remove(x) {
                        None
                    } else {
                        Some(*x)
                    }
                })
                .collect::<Vec<_>>()
        };

        let mut upper_level_ssts_set = task
            .upper_level_sst_ids
            .iter()
            .copied()
            .collect::<HashSet<_>>();
        let mut lower_level_ssts_set = task
            .lower_level_sst_ids
            .iter()
            .copied()
            .collect::<HashSet<_>>();
        if let Some(upper_level) = task.upper_level {
            snapshot.levels[upper_level - 1].1 = filter_ssts(
                &snapshot.levels[upper_level - 1].1,
                &mut upper_level_ssts_set,
            );
        } else {
            snapshot.l0_sstables = filter_ssts(&snapshot.l0_sstables, &mut upper_level_ssts_set);
        }
        assert!(upper_level_ssts_set.is_empty());

        files_to_remove.extend(&task.upper_level_sst_ids);
        files_to_remove.extend(&task.lower_level_sst_ids);

        let mut new_lower_level_ssts = filter_ssts(
            &snapshot.levels[task.lower_level - 1].1,
            &mut lower_level_ssts_set,
        );
        assert!(lower_level_ssts_set.is_empty());
        new_lower_level_ssts.extend(output);
        //因为是部分合并的原因，所以需要重新排序
        new_lower_level_ssts.sort_by(|x, y| {
            snapshot
                .sstables
                .get(x)
                .unwrap()
                .first_key()
                .cmp(snapshot.sstables.get(y).unwrap().first_key())
        });

        snapshot.levels[task.lower_level - 1].1 = new_lower_level_ssts;
        (snapshot, files_to_remove)
    }
}
