pub mod compaction;

use std::time::SystemTime;

use crate::{memtable::MemTable, utils};

#[derive(Debug)]
pub struct SSTable {
    pub file: String,
}

impl SSTable {
    pub fn new(dir: &str) -> Result<SSTable, String> {
        Ok(SSTable {
            file: format!("{}/{}.sst", dir, utils::get_timestamp()),
        })
    }

    pub fn write(&self, memtable: &MemTable) -> Result<(), String> {
        Self::write_impl(memtable, &self.file)
    }

    fn write_impl(memtable: &MemTable, file: &str) -> Result<(), String> {
        let data = memtable.encode();
        std::fs::write(file, data).map_err(|e| e.to_string())
    }
}

#[cfg(test)]
mod tests;