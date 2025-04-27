// pub mod leveled_compaction;
pub mod size_tiered_compaction;

use std::sync::Arc;

use crate::SharedSSTableReader;

use super::SSTableWriter;

pub trait Compaction {
    fn compact(
        &self, 
        sstables: Arc<SharedSSTableReader>, 
        writer: SSTableWriter) -> Result<(), String>;
}
