pub mod compaction;
pub mod reader;
pub mod writer;

type Key = String;
type Value = String;
type Offset = u64;

use std::{collections::{BTreeMap, HashMap}, vec};
use chrono::offset;
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
        unimplemented!()
    }
}

// type SSTableData = Vec<u8>;

#[derive(Debug)]
struct SSTableData{
    data: Vec<u8>,
}

impl SSTableData {
    fn new() -> SSTableData {
        SSTableData {
            data: Vec::new(),
        }
    }

    fn extend_from_slice(&mut self, data: &[u8]) {
        self.data.extend_from_slice(data);
    }

    fn size(&self) -> u64 {
        self.iter().fold(0, |acc, (k, v)| {
            acc + k.len() as u64 + v.len() as u64
        })
    }

    // raw data length
    fn len(&self) -> usize {
        self.data.len()
    }

    fn raw_data(&self) -> &Vec<u8> {
        &self.data
    }

    fn iter(&self) -> SSTableDataIterator {
        SSTableDataIterator {
            data: &self.data,
            offset: 0,
        }
    }

    fn get_key_len(&self, offset: usize) -> Result<u64, String> {
        let data = &self.data[offset..(offset + 8)]
            .try_into()
            .map_err(|e: std::array::TryFromSliceError| e.to_string())?;
        Ok(u64::from_ne_bytes(*data))
    }
}

impl From<&[u8]> for SSTableData {
    fn from(data: &[u8]) -> SSTableData {
        SSTableData {
            data: data.to_vec(),
        }
    }
}

impl From<Vec<u8>> for SSTableData {
    fn from(data: Vec<u8>) -> SSTableData {
        SSTableData {
            data,
        }
    }
}

impl From<&Vec<u8>> for SSTableData {
    fn from(data: &Vec<u8>) -> SSTableData {
        SSTableData {
            data: data.clone(),
        }
    }
}

pub struct SSTableDataIterator<'a> {
    data: &'a Vec<u8>,
    offset: usize,
}

impl<'a> Iterator for SSTableDataIterator<'a> {
    type Item = (&'a [u8], &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        if self.offset >= self.data.len() {
            return None;
        }
        let key_len = u64::from_ne_bytes(self.data[self.offset..(self.offset + 8)]
            .try_into()
            .unwrap());
        self.offset += 8;
        let key = &self.data[self.offset..(self.offset + key_len as usize)];
        self.offset += key_len as usize;
        let value_len = u64::from_ne_bytes(self.data[self.offset..(self.offset + 8)]
            .try_into()
            .unwrap());
        self.offset += 8;
        let value = &self.data[self.offset..(self.offset + value_len as usize)];
        self.offset += value_len as usize;
        Some((key, value))
    }
}

#[cfg(test)]
mod tests;
