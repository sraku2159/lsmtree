pub mod leveled_compaction;
pub mod size_tiered_compaction;

use super::SSTableReader;

pub trait Compaction {
    fn compact(sstable: &SSTableReader);
    fn get_sstables(&self, dir: &String) -> Vec<SSTableReader>;
}
