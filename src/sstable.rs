pub mod compaction;
pub mod reader;
pub mod writer;

type Key = String;
type Value = String;
type Offset = u64;

use std::collections::HashMap;
pub use reader::SSTableReader;
pub use writer::SSTableWriter;

use crate::{memtable::MemTable, utils::get_page_size};

#[derive(Debug)]
struct SSTableHeader {
    pub file_size: u64,
    pub header_size: u64,
    pub index_size: u64,
}

impl SSTableHeader {
    pub fn new(file_size: u64, header_size: u64, index_size: u64) -> SSTableHeader {
        SSTableHeader {
            file_size,
            header_size,
            index_size,
        }
    }
}

impl SSTableHeader {
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&self.file_size.to_be_bytes());
        buf.extend_from_slice(&self.header_size.to_be_bytes());
        buf.extend_from_slice(&self.index_size.to_be_bytes());
        buf
    }

    pub fn decode(data: &[u8]) -> Result<SSTableHeader, String> {
        let file_size = u64::from_be_bytes(data[0..8]
                .try_into()
                .map_err(|e: std::array::TryFromSliceError| e.to_string())?);
        let header_size = u64::from_be_bytes(data[8..16]
                .try_into()
                .map_err(|e: std::array::TryFromSliceError| e.to_string())?);
        let index_size = u64::from_be_bytes(data[16..24]
                .try_into()
                .map_err(|e: std::array::TryFromSliceError| e.to_string())?);
        Ok(SSTableHeader {
            file_size,
            header_size,
            index_size,
        })
    }
}

#[derive(Debug)]
struct SSTableIndex(HashMap<Key, Offset>);

impl SSTableIndex {
    fn new() -> SSTableIndex {
        SSTableIndex(HashMap::new())
    }

    fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        for (key, offset) in self.0.iter() {
            buf.extend_from_slice(key.as_bytes());
            buf.extend_from_slice(&offset.to_be_bytes());
        }
        buf
    }

    fn size(&self) -> u64 {
        self.0.iter().fold(0, |acc, (key, _)| acc + key.len() as u64 + 8)
    }

    fn get(&self, key: &Key) -> Option<&Offset> {
        self.0.get(key)
    }

    fn insert(&mut self, key: Key, offset: Offset) {
        self.0.insert(key, offset);
    }
}

// ページサイズごとにインデックスを作成する
// もし、Valueでページサイズを超えた場合、超えたValueのKeyをインデックスに追加する
impl From<&MemTable> for SSTableIndex {
    fn from(memtable: &MemTable) -> SSTableIndex {
        if memtable.is_empty() {
            return SSTableIndex::new();
        }
        let mut index = SSTableIndex::new();
        let mut offset: u64 = 0;
        let mut page_cnt = 0;
        let page_size = get_page_size() as u64;

        let (k, _) = memtable.iter().next().unwrap();
        index.0.insert(k.clone(), 0);
        for (k, v) in memtable.iter() {
            if offset >= page_size {
                page_cnt += offset / page_size;
                offset %= page_size;
                index.0.insert(k.clone(), page_size * page_cnt + offset);
            }
            offset += MemTable::encode_key_value(&k, &v).len() as u64;
        }
        index
    }
}

type SSTableData = Vec<u8>;

#[cfg(test)]
mod tests;