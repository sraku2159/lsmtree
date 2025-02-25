pub mod memtable;
pub mod commitlog;
pub mod sstable;

use memtable::MemTable;
use commitlog::CommitLog;
use sstable::{SSTable, Compaction};

use sstable::{LevelCompaction, SizeTieredCompaction};

pub struct LSMTree<T>
where
    T: Compaction,
{
    memtable: MemTable,
    commitlog: CommitLog,
    sstable: SSTable,
    compaction: T,
}

impl<T: Compaction> LSMTree<T> {
    pub fn new(compaction: T) -> LSMTree<T> {
        LSMTree {
            memtable: MemTable::new(),
            commitlog: CommitLog::new(),
            sstable: SSTable::new(),
            compaction,
        }
    }
}

#[cfg(test)]
mod tests;