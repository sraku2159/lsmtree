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

}

impl Compaction for SizeTieredCompaction {
    fn compact(&self, sstable: Vec<SSTableReader>) {
        unimplemented!();
    }

    fn get_target_dir(&self) -> String {
        unimplemented!();
        // self.dir + "/staged"
    }

    fn get_sstables(&self, dir: &String) -> Vec<SSTableReader> {
        unimplemented!();
    }
}