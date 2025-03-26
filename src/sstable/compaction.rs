pub mod leveled_compaction;
pub mod size_tiered_compaction;

use super::SSTableReader;

pub trait Compaction {
    fn compact(&self, sstables: Vec<SSTableReader>);
    fn get_target_dir(&self) -> String;
    fn get_sstables(&self, dir: &String) -> Vec<SSTableReader>;
}
