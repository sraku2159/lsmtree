use super::SSTableReader;
use super::Compaction;

#[derive(Debug)]
pub struct SizeTieredCompaction {
}

impl SizeTieredCompaction {
    pub fn new() -> SizeTieredCompaction {
        SizeTieredCompaction {
        }
    }
}

impl Compaction for SizeTieredCompaction {
    fn compact(sstable: &SSTableReader) {
        unimplemented!();
    }
}