pub mod compaction;

use std::time::SystemTime;

use crate::memtable::MemTable;

pub struct SSTable {
    pub file: String,
}

impl SSTable {
    pub fn new(dir: &str) -> Result<SSTable, String> {
        if std::fs::metadata(dir).map(|m| m.is_dir()).unwrap_or(false) {
            Self::create_dir(dir)?;
        }
        match SystemTime::now().elapsed() {
            Ok(n) => Ok(SSTable {
                file: format!("{}/{}.sst", dir, n.as_millis()),
            }),
            Err(_) => Err("SystemTime before UNIX EPOCH!".to_string()),
        }
    }

    fn create_dir(dir: &str) -> Result<(), String> {
        std::fs::create_dir_all(dir).map_err(|e| e.to_string())
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