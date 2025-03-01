pub mod leveled_compaction;
pub mod size_tiered_compaction;

use super::SSTable;

pub trait Compaction {
    fn compact(sstable: &SSTable);
}
