pub mod memtable;
pub mod commitlog;
pub mod sstable;
pub mod utils;

use std::thread::spawn;

use memtable::MemTable;
use commitlog::CommitLog;
use sstable::{SSTable, Compaction};

use sstable::{LevelCompaction, SizeTieredCompaction};
use utils::get_page_size;

pub struct LSMTreeConf<T>
where
    T: Compaction + Send,
{
    compaction: T,
    sst_dir: Option<String>,
    commitlog: Option<String>,
    memtable_threshold: Option<usize>,
}

impl<T: Compaction + Send> LSMTreeConf<T> {
    pub fn new(
        compaction: T,
        sst_dir: Option<String>,
        commitlog: Option<String>,
        memtable_threshold: Option<usize>, 
        ) -> LSMTreeConf<T>
    {
        LSMTreeConf {
            compaction,
            sst_dir,
            commitlog,
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
        let sst_dir = conf.sst_dir.unwrap_or("./sst".to_string());
        let memtable_threshold = conf.memtable_threshold.unwrap_or(get_page_size());
        let compaction = conf.compaction;
        Ok(LSMTree {
            memtable: MemTable::new(),
            memtable_threshold,
            commitlog: CommitLog::new(conf.commitlog)?,
            sst_dir,
            compaction,
        })
    }
}

impl<T: Compaction + Send> LSMTree<T> {
    pub fn put(&mut self, key: &str, value: &str) -> Option<String> {
        let ret = self.memtable.put(key, value);
        self.commitlog.write_put(key, value);
        // 大きいデータ構造なので、シャローコピーでない場合、パフォーマンスが悪い
        let memtable = self.memtable.clone();
        let dir = self.sst_dir.clone();
        if self.memtable.len() >= self.memtable_threshold {
            spawn( move || {
                let ret = Self::flush_memtable(&dir, memtable);
                match ret {
                    Ok(_) => println!("Flushed memtable"),
                    Err(e) => eprintln!("{}", e),
                }
            });
        }
        self.memtable = MemTable::new();
        ret
    }

    fn flush_memtable(dir: &str, memtable: MemTable) -> Result<(), String> {
        let sstable = SSTable::new(dir)?;
        sstable.write(&memtable)?;
        Ok(())
    }

    pub fn get(&self, key: &str) -> Option<String> {
        self.memtable.get(key)
    }
}