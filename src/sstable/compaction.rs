pub mod leveled_compaction;
pub mod size_tiered_compaction;

use super::{SSTableReader, SSTableWriter};

pub trait Compaction {
    fn compact(&self, sstables: Vec<SSTableReader>, writer: SSTableWriter) -> Result<(), String>;
}
