use std::sync::Arc;

use crate::sstable::reader::SSTableReaderManager;
use crate::sstable::SSTableData;
use crate::sstable::SSTableWriter;
use crate::SharedSSTableReader;

use super::Compaction;

#[derive(Debug, Clone)]
pub struct SizeTieredCompaction {
    index_interval: usize,
    min_threshold: f64,
    max_threshold: f64,
    bucket_threshold: usize,
}

impl SizeTieredCompaction {
    pub fn new(index_interval: usize, min_threshold: Option<f64>, max_threshold: Option<f64>, bucket_threshold: Option<usize>) -> SizeTieredCompaction {
        SizeTieredCompaction {
            index_interval,
            min_threshold: min_threshold.unwrap_or(0.5),
            max_threshold: max_threshold.unwrap_or(1.5),
            bucket_threshold: bucket_threshold.unwrap_or(4),
        }
    }

    fn merge(&self, sstables: Vec<SSTableData>) -> SSTableData {
        let mut target = sstables;
        while target.len() != 1 {
            target = target.chunks(2).map(|pair| {
                if pair.len() == 1 {
                    return pair[0].clone();
                }
                let ret = self.merge_impl(&pair[0], &pair[1]);
                ret
            }).collect::<Vec<SSTableData>>();
        }
        target.pop().unwrap()
    }

    // TODO: エラー処理
    fn merge_impl(&self, left: &SSTableData, right: &SSTableData) -> SSTableData {
        let mut merged = SSTableData::new();
        let mut left_iter = left.iter();
        let mut right_iter = right.iter();

        let mut left = left_iter.next();
        let mut right = right_iter.next();
        while left.is_some() && right.is_some() {
            let left_v = left.unwrap();
            let right_v = right.unwrap();
            if left_v.key() < right_v.key() {
                let _ = merged.push(left_v.clone());
                left = left_iter.next();
            } else if left_v.key() > right_v.key() {
                let _ = merged.push(right_v.clone());
                right = right_iter.next();
            } else {
                // keyが重複している場合、timestampが大きい方を選ぶ
                if left_v.timestamp() > right_v.timestamp() {
                    let _ = merged.push(left_v.clone());
                } else {
                    let _ = merged.push(right_v.clone());
                }
                left = left_iter.next();
                right = right_iter.next();
            }
        }

        while left.is_some() {
            let left_v = left.unwrap();
            let _ = merged.push(left_v.clone());
            left = left_iter.next();
        }

        while right.is_some() {
            let right_v = right.unwrap();
            let _ = merged.push(right_v.clone());
            right = right_iter.next();
        }
        merged
    }

    fn get_interesting_bucket(&self, sstables: &Vec<Arc<SSTableReaderManager>>) -> Vec<Arc<SSTableReaderManager>> {
        let mut buckets = Vec::new();

        for sstable in sstables.iter() {
            let metadata = sstable.metadata().unwrap();
            let len = metadata.len() as f64;
            
            fn bucket_median_size(sstables: &Vec<Arc<SSTableReaderManager>>) -> f64 {
                let sum = sstables.iter().map(|sstable| {
                    sstable.metadata().unwrap().len() as f64
                }).sum::<f64>();
                let len = sstables.len() as f64;
                if len == 0.0 {
                    return 0.0;
                }
                sum / len
            }

            let bucket = buckets.iter_mut().find(|bucket| {
                let bucket_median = bucket_median_size(&bucket);
                bucket_median * self.min_threshold < len && len < bucket_median * self.max_threshold
            });
            match bucket {
                Some(bucket) => {
                    bucket.push(sstable.clone());
                }
                None => {
                    buckets.push(vec![sstable.clone()]);
                }
                
            }
        }

        buckets.into_iter().max_by(|a, b| {
            a.len().cmp(&b.len())
        }).unwrap_or_else(Vec::new)
    }

}

impl Compaction for SizeTieredCompaction {
    fn compact(
        &self, 
        shared: Arc<SharedSSTableReader>, 
        writer: SSTableWriter
    ) -> Result<(), String> {
        // let mut sstables = sstables;
        // sstables.sort_by(|a, b| {
        //     a.metadata().unwrap().len().cmp(&b.metadata().unwrap().len())
        // });

        let vec = shared.to_vec();
        let interestings = self.get_interesting_bucket(&vec);
        let interestings_data = interestings
            .iter()
            .map(|sstable| sstable.data().unwrap())
            .collect::<Vec<SSTableData>>();

        if interestings.len() < self.bucket_threshold {
            return Ok(());
        }

        let compacted = self.merge(interestings_data);

        writer.write_with_index(&compacted, self.index_interval)?;
        interestings.iter().for_each(|sstable| {
            sstable.delete();
        });
        Ok(())
    }
}

#[cfg(test)]
mod tests;