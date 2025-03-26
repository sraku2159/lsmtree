use super::SSTableReader;
use super::Compaction;

#[derive(Debug)]
pub struct SizeTieredCompaction {
    // dir: String,
}

impl SizeTieredCompaction {
    pub fn new() -> SizeTieredCompaction {
        SizeTieredCompaction {
        }
    }

}

impl Compaction for SizeTieredCompaction {
    fn compact(&self, sstable: Vec<SSTableReader>) {
        unimplemented!();
    }

    fn get_target_dir(&self) -> String {
        unimplemented!();
        // self.dir + "/staged"
    }

    fn get_sstables(&self, dir: &String) -> Vec<SSTableReader> {
        unimplemented!();
    }
}