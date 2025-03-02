use super::SSTableReader;
use super::Compaction;

#[derive(Debug)]
pub struct LeveledCompaction {
}

impl LeveledCompaction {
    pub fn new() -> LeveledCompaction {
        LeveledCompaction {
        }
    }
}

impl Compaction for LeveledCompaction {
    fn compact(sstabel: &SSTableReader) {
        unimplemented!();
    }
}
