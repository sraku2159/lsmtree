pub mod compaction;
pub mod reader;
pub mod writer;

type Key = String;
type Value = String;
type Offset = u64;

use std::{collections::{BTreeMap, HashMap}, vec};
pub use reader::SSTableReader;
pub use writer::SSTableWriter;

use crate::{memtable::MemTable, utils::get_page_size};

#[derive(Debug)]
struct SSTableHeader {
    pub header_size: u64,
    pub index_size: u64,
}

impl SSTableHeader {
    pub fn new(header_size: u64, index_size: u64) -> SSTableHeader {
        SSTableHeader {
            header_size,
            index_size,
        }
    }
}

impl SSTableHeader {
    pub fn encode(&self) -> Vec<u8> {
        vec![
            self.header_size.to_ne_bytes().to_vec(),
            self.index_size.to_ne_bytes().to_vec(),
        ].concat()
    }

    pub fn decode(data: &[u8]) -> Result<SSTableHeader, String> {
        let header_size = u64::from_ne_bytes(data[0..8]
                .try_into()
                .map_err(|e: std::array::TryFromSliceError| e.to_string())?);
        let index_size = u64::from_ne_bytes(data[8..16]
                .try_into()
                .map_err(|e: std::array::TryFromSliceError| e.to_string())?);
        Ok(SSTableHeader {
            header_size,
            index_size,
        })
    }
}

#[derive(Debug)]
struct SSTableIndex(BTreeMap<Key, Offset>);

impl SSTableIndex {
    fn new() -> SSTableIndex {
        SSTableIndex(BTreeMap::new())
    }

    fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        for (key, offset) in self.0.iter() {
            buf.extend_from_slice(&key.len().to_ne_bytes());
            buf.extend_from_slice(key.as_bytes());
            buf.extend_from_slice(&offset.to_ne_bytes());
        }
        buf
    }

    fn size(&self) -> u64 {
        self.0.iter().fold(0, |acc, (key, _)| {
            acc 
            + std::mem::size_of::<u64>() as u64
            + key.len() as u64 
            + std::mem::size_of::<Offset>() as u64
    })}

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
            offset += MemTable::encode_key_value(&k, &v.to_string()).len() as u64;
        }
        index
    }
}

type SSTableData = Vec<u8>;

#[cfg(test)]
mod tests;