pub mod memtable;
pub mod commitlog;
pub mod sstable;
pub mod utils;

use memtable::MemTable;
use commitlog::CommitLog;
use sstable::{SSTable, Compaction};

use sstable::{LevelCompaction, SizeTieredCompaction};
use utils::get_page_size;

pub struct LSMTreeConf<T>
where
    T: Compaction,
{
    compaction: T,
    memtable_threshold: Option<usize>,
}

impl<T: Compaction> LSMTreeConf<T> {
    pub fn new(
        compaction: T,
        memtable_threshold: Option<usize>, 
        ) -> LSMTreeConf<T>
    {
        LSMTreeConf {
            compaction,
            memtable_threshold,
        }
    }
}

pub struct LSMTree<T>
where
    T: Compaction,
{
    memtable: MemTable,
    memtable_threshold: usize,
    commitlog: CommitLog,
    sstable: SSTable,
    compaction: T,
}

impl<T: Compaction> LSMTree<T> {
    pub fn new(conf: LSMTreeConf<T>) -> LSMTree<T> {
        let memtable_threshold = conf.memtable_threshold.unwrap_or(get_page_size());
        let compaction = conf.compaction;
        LSMTree {
            memtable: MemTable::new(),
            memtable_threshold,
            commitlog: CommitLog::new(),
            sstable: SSTable::new(),
            compaction,
        }
    }
}

impl<T: Compaction> LSMTree<T> {
    pub fn put(&mut self, key: &str, value: &str) -> Option<String> {
        self.memtable.put(key, value)
    }

    pub fn get(&self, key: &str) -> Option<String> {
        self.memtable.get(key)
    }
}

#[cfg(test)]
mod tests;