pub mod memtable;
pub mod commitlog;
pub mod sstable;
pub mod utils;

use std::thread::spawn;

use memtable::MemTable;
use commitlog::CommitLog;
use sstable::{SSTable, compaction::Compaction};

use utils::get_page_size;

// 今後、もしmemtableやsstableで異なるデータ型を使用する場合、この型をアダプターとして差分を吸収する
pub type Key = String;
pub type Value = String;

pub struct LSMTreeConf<T>
where
    T: Compaction + Send,
{
    compaction: T,
    sst_dir: String,
    commitlog_dir: String,
    memtable_threshold: usize,
}

impl<T: Compaction + Send> LSMTreeConf<T> {
    pub fn new(
        compaction: T,
        sst_dir: Option<String>,
        commitlog_dir: Option<String>,
        memtable_threshold: Option<usize>, 
        ) -> LSMTreeConf<T>
    {
        let sst_dir = sst_dir.unwrap_or("./.sst".to_string());
        let commitlog_dir = commitlog_dir.unwrap_or("./.commitlog".to_string());
        let memtable_threshold = memtable_threshold.unwrap_or(get_page_size());
        LSMTreeConf {
            compaction,
            sst_dir,
            commitlog_dir,
            memtable_threshold,
        }
    }
}

pub struct LSMTree<T>
where
    T: Compaction + Send,
{
    memtable: MemTable,
    memtable_threshold: usize,
    commitlog: CommitLog,
    sst_dir: String,
    compaction: T,
}

impl<T: Compaction + Send> LSMTree<T> {
    pub fn new(conf: LSMTreeConf<T>) -> Result<LSMTree<T>, String> {
        Ok(LSMTree {
            memtable: MemTable::new(),
            memtable_threshold: conf.memtable_threshold,
            commitlog: CommitLog::new(&conf.commitlog_dir)?,
            sst_dir: conf.sst_dir,
            compaction: conf.compaction,
        })
    }
}

impl<T: Compaction + Send> LSMTree<T> {
    pub fn put(&mut self, key: &str, value: &str) -> Option<Value> {
        let ret = self.memtable.put(key, value);
        self.commitlog.write_put(key, value);
        if self.memtable.len() >= self.memtable_threshold {
            // 大きいデータ構造なのでディープコピーの場合、パフォーマンスが悪い
            // ただ、現在の実装だと借用状態なので、ムーブができない
            let memtable = self.memtable.clone();
            let dir = self.sst_dir.clone();
            let commitlog = self.commitlog.clone();
            spawn( move || {
                let ret = Self::flush_memtable(&dir, memtable);
                match ret {
                    Ok(_) => {
                        println!("Flushed memtable");
                        match commitlog.delete_log().unwrap() {
                            Err(e) => eprintln!("{}", e),
                            _ => println!("Deleted commit log"),
                        }
                    },
                    Err(e) => eprintln!("{}", e),
                }
            });
        }
        self.memtable = MemTable::new();
        self.commitlog 
        ret
    }

    fn flush_memtable(dir: &str, memtable: MemTable) -> Result<(), String> {
        let sstable = SSTable::new(dir)?;
        sstable.write(&memtable)?;
        Ok(())
    }

    pub fn get(&self, key: &str) -> Option<Value> {
        self.memtable.get(key)
    }
}