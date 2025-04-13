use crate::sstable::SSTableData;

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
    // dir: String,
}

impl SizeTieredCompaction {
    pub fn new() -> SizeTieredCompaction {
        SizeTieredCompaction {
        }
    }

    fn merge(&self, left: &SSTableData, right: &SSTableData) -> SSTableData {
        self.merge_impl(left, right)
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

}

impl Compaction for SizeTieredCompaction {
    fn compact(&self, sstable: Vec<SSTableReader>) {
        unimplemented!();
    }

    fn get_target_dir(&self) -> String {
        unimplemented!();
        // self.dir + "/staged"
    }
}

#[cfg(test)]
mod tests;