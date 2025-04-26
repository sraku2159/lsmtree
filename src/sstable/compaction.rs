pub mod leveled_compaction;
pub mod size_tiered_compaction;

use super::{SSTableReader, SSTableWriter};

pub trait Compaction {
    fn compact(
        &self, 
        sstables: Vec<SSTableReader>, 
        rwlock_for_sstables: &std::sync::RwLock<()>,
        writer: SSTableWriter) -> Result<(), String>;
}
