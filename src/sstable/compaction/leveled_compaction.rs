use super::SSTable;
use super::Compaction;

pub struct LeveledCompaction {
}

impl LeveledCompaction {
    pub fn new() -> LeveledCompaction {
        LeveledCompaction {
        }
    }
}

impl Compaction for LeveledCompaction {
    fn compact(sstabel: &SSTable) {
        unimplemented!();
    }
}
