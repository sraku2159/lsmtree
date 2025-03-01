use super::SSTable;
use super::Compaction;

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