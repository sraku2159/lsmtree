use std::fs;

use crate::sstable::SSTableData;
use crate::sstable::SSTableWriter;

use super::SSTableReader;
use super::Compaction;

/*
memo

Compactionは別プロセスで行われる予定

readする際に使用するイテレータはCompactionの実装によって異なる
ここで懸念点が一つある
nextで次のReaderを返すが、実際に読み込む前にコンパクションによってファイルが削除される可能性がある
これをどう解決するか

1. Reader作成時にファイルをオープンしてしまう
カーネルは参照カウントが0になるまでファイルを削除しないため、解決は可能。
ただ、失敗がないように実装するには、シグナルハンドラを使ってコンパクション処理に割り込みを入れる必要がある

2. Read開始時にファイルが存在するか確認する。ファイルが存在しない場合、イテレータを進める
これは、対象のファイルが削除されたということは、そのファイルがコンパクションの対象であるということなので、
次に読み込む対象がコンパクションされたファイルであれば問題ない
(しかも、結構鮮やかでは？)


しかし、いずれの戦略を取るにしても、イテレーションがこちらに依存するのはあまり鮮やかではない


コンパクション
* 時系列順でコンパクションを行わなければ、整合性が取れない
* stagedディレクトリに存在するファイルが最も小さいファイルが格納されるバケットに移動されるまで、コンパクションは行わなければならない
* stagedはメッセージキューの役割を果たす

拡張性を考えると、マルチスレッドorマルチプロセスで常にこの処理側が/stagedを監視する方が良い
*/

#[derive(Debug)]
pub struct SizeTieredCompaction {
    index_interval: usize,
    min_threshold: f64,
    max_threshold: f64,
}

impl SizeTieredCompaction {
    pub fn new(index_interval: usize, min_threshold: Option<f64>, max_threshold: Option<f64>) -> SizeTieredCompaction {
        SizeTieredCompaction {
            index_interval,
            min_threshold: min_threshold.unwrap_or(0.5),
            max_threshold: max_threshold.unwrap_or(1.5),
        }
    }

    fn merge(&self, sstables: Vec<SSTableData>) -> SSTableData {
        let mut target = sstables;
        let mut merged = Vec::new();
        while merged.len() != 1 {
            target.windows(2).for_each(|pair| {
                let left = pair[0].clone();
                let right = pair[1].clone();
                let merged_data = self.merge_impl(&left, &right);
                merged.push(merged_data);
            });
            target = merged;
            merged = Vec::new();
        }
        merged.pop().unwrap()
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

    fn get_interesting_bucket(&self, sstables: &Vec<SSTableReader>) -> Vec<SSTableReader> {
        let mut buckets = Vec::new();

        for sstable in sstables.iter() {
            let metadata = sstable.metadata().unwrap();
            let len = metadata.len() as f64;
            
            fn bucket_median_size(sstables: &Vec<SSTableReader>) -> f64 {
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
    fn compact(&self, sstables: Vec<SSTableReader>, writer: SSTableWriter) -> Result<(), String> {
        let mut sstables = sstables;
        sstables.sort_by(|a, b| {
            a.metadata().unwrap().len().cmp(&b.metadata().unwrap().len())
        });
        
        let interestings = self.get_interesting_bucket(&sstables)
            .iter()
            .map(|sstable| sstable.data().unwrap())
            .collect::<Vec<SSTableData>>();

        let compacted = self.merge(interestings);
        writer.write_with_index(&compacted, self.index_interval)?;
        sstables.iter().for_each(|sstable| {
            fs::remove_file(&sstable.file).map_err(|e| e.to_string()).unwrap();
            fs::remove_file(&sstable.index_file).map_err(|e| e.to_string()).unwrap();
        });
        Ok(())
    }
}

#[cfg(test)]
mod tests;