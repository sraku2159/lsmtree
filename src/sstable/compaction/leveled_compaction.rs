use crate::sstable::SSTableWriter;

use super::SSTableReader;
use super::Compaction;

#[derive(Debug, Clone)]
pub struct LeveledCompaction {
}

impl LeveledCompaction {
    pub fn new() -> LeveledCompaction {
        LeveledCompaction {
        }
    }
}

impl Compaction for LeveledCompaction {
    fn compact(
        &self, 
        sstables: Vec<SSTableReader>, 
        writer: SSTableWriter) -> Result<(), String> {
        let _ = writer;
        let _ = sstables;
        unimplemented!();
    }
}
