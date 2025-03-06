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
    pub const SIZE: u64 = 16;

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

    pub fn size(&self) -> u64 {
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

    fn iter(&self) -> SSTableIndexIterator {
        SSTableIndexIterator {
            iter: self.0.iter(),
        }
    }
}

pub struct SSTableIndexIterator<'a> {
    iter: std::collections::btree_map::Iter<'a, Key, Offset>,
}

impl<'a> Iterator for SSTableIndexIterator<'a> {
    type Item = (&'a Key, &'a Offset);

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

impl<'a> Iterator for &'a SSTableIndex {
    type Item = (&'a Key, &'a Offset);

    fn next(&mut self) -> Option<Self::Item> {
        self.0.iter().next()
    }
}

impl IntoIterator for SSTableIndex {
    type Item = (Key, Offset);
    type IntoIter = std::collections::btree_map::IntoIter<Key, Offset>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl FromIterator<(Key, Offset)> for SSTableIndex {
    fn from_iter<T: IntoIterator<Item = (Key, Offset)>>(iter: T) -> Self {
        let mut index = SSTableIndex::new();
        for (k, v) in iter {
            index.insert(k, v);
        }
        index
    }
}

// ページサイズごとにインデックスを作成する
// もし、Valueでページサイズを超えた場合、超えたValueのKeyをインデックスに追加する
// 修正必要
// オフセットはファイルの先頭からのオフセット
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
        index.iter().map(|(k, v)| {
            (k.clone(), v + index.size() + SSTableHeader::SIZE)
        }).collect()
    }
}

impl TryFrom<&SSTableData> for SSTableIndex {
    type Error = String;

    fn try_from(data: &SSTableData) -> Result<Self, Self::Error> {
        
        fn get_key_len(data: &[u8], offset: usize) -> u64 {
            u64::from_ne_bytes(data[offset..(offset + 8)]
                .try_into()
                .map_err(|e: std::array::TryFromSliceError| e.to_string())
                .unwrap())
        }

        fn get_key(data: &[u8], offset: usize, key_len: u64) -> String {
            String::from_utf8(data[offset..(offset + key_len as usize)].to_vec())
                .map_err(|e| e.to_string())
                .unwrap()
        }

        fn get_value_len(data: &[u8], offset: usize) -> u64 {
            u64::from_ne_bytes(data[offset..(offset + 8)]
                .try_into()
                .map_err(|e: std::array::TryFromSliceError| e.to_string())
                .unwrap())
        }

        fn get_value(data: &[u8], offset: usize, value_len: u64) -> String {
            String::from_utf8(data[offset..(offset + value_len as usize)].to_vec())
                .map_err(|e| e.to_string())
                .unwrap()
        }

        unimplemented!();
        // ここから

        let mut index = SSTableIndex::new();

        let mut offset = 0;
        let mut page_cnt = 0;
        let page_size = get_page_size();
        while offset < data.len() {
            let key_len = get_key_len(data, offset) as usize;
            let data_len = get_value_len(data, offset + key_len + 8) as usize;
            // if offset  {}
            offset += 8;
            let key = get_key(data, offset, key_len as u64);
            offset += key_len;
            let value = u64::from_ne_bytes(data[offset..(offset + 8)]
                .try_into()
                .map_err(|e: std::array::TryFromSliceError| e.to_string())
                .unwrap());
            offset += 8;
            index.insert(key, value);
        }
        Ok(index)
    }
}

type SSTableData = Vec<u8>;

#[cfg(test)]
mod tests;
