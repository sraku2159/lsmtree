use std::time::SystemTime;

use crate::memtable::MemTable;

pub struct SSTable {
    pub file: String,
}

impl SSTable {
    pub fn new(dir: &str) -> Result<SSTable, String> {
        match SystemTime::now().elapsed() {
            Ok(n) => Ok(SSTable {
                file: format!("{}/{}.sst", dir, n.as_millis()),
            }),
            Err(_) => Err("SystemTime before UNIX EPOCH!".to_string()),
        }
    }

    pub fn write(&self, memtable: &MemTable) -> Result<(), String> {
        let data = memtable.encode();
        std::fs::write(&self.file, data).map_err(|e| e.to_string())
    }
}

pub trait Compaction {
    fn compact(sstable: &SSTable);
}

pub struct LevelCompaction {
}

impl LevelCompaction {
    pub fn new() -> LevelCompaction {
        LevelCompaction {
        }
    }
}

impl Compaction for LevelCompaction {
    fn compact(sstabel: &SSTable) {
        unimplemented!();
    }
}

pub struct SizeTieredCompaction {
}

impl SizeTieredCompaction {
    pub fn new() -> SizeTieredCompaction {
        SizeTieredCompaction {
        }
    }
}

impl Compaction for SizeTieredCompaction {
    fn compact(sstable: &SSTable) {
        unimplemented!();
    }
}