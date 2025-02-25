pub struct SSTable {
}

impl SSTable {
    pub fn new() -> SSTable {
        SSTable {
        }
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