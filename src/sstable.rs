pub mod compaction;
pub mod reader;
pub mod writer;

type Key = String;
type Value = Option<String>;
type Offset = u64;

use std::{collections::BTreeMap, fmt, ops::Index, vec};
pub use reader::SSTableReader;
pub use writer::SSTableWriter;

use crate::{memtable::MemTable, utils::get_page_size};

#[derive(Debug, Clone)]
pub struct SSTableHeader {
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

#[derive(Debug, Clone)]
pub struct SSTableIndex(BTreeMap<Key, Offset>);

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

    pub fn decode(data: &[u8]) -> Result<SSTableIndex, String> {
        let mut i = 0;
        let mut index = SSTableIndex::new();
        while i < data.len() {
            let key_len = u64::from_ne_bytes(data[i..(i + 8)]
                .try_into()
                .map_err(|e: std::array::TryFromSliceError| e.to_string())?);
            let key = String::from_utf8(data[(i + 8)..(i + 8 + key_len as usize)].to_vec())
                .map_err(|e| e.to_string())?;
            let offset = u64::from_ne_bytes(data[(i + 8 + key_len as usize)..(i + 16 + key_len as usize)]
                .try_into()
                .map_err(|e: std::array::TryFromSliceError| e.to_string())?);
            index.insert(key, offset);
            i += 16 + key_len as usize;
        }
        Ok(index)
    }

    pub fn find_key_range(&self, key: &Key) -> Option<(u64, Option<u64>)> {
        for i in 0..self.0.len() {
            let (k, offset) = self.0.iter().nth(i).unwrap();
            let next = self.0.iter().nth(i + 1);
            if key < k {
                break;
            }
            if k <= key && next.map_or(true, |(k, _)| key < k) {
                return Some((*offset, next.map(|(_, v)| *v)));
            }
        }
        None
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
        let keys = data.get_first_keys();
        let index_size = keys.iter().fold(0, |acc, k| {
            acc + k.len() as u64 + 2 * std::mem::size_of::<Offset>() as u64
        });
        let mut index = SSTableIndex::new();
        let mut accm = index_size + SSTableHeader::SIZE;
        keys.iter().enumerate().for_each(|(i, k)| {
            index.insert(k.clone(), accm);
            accm += data.chunk_len(i) as u64;
        });

        Ok(index)
    }
}

// 一つのSSTableにおけるデータを表す
// データの重複はありえない
/// memtableから作成される (memtable内でデータの重複は起こり得ない)
// ページサイズでデータを分割するが、原則ページサイズを超えないようにしたい
/// 例外として、Valueが大きい場合はページサイズを超えることもある
/// ページサイズ程度のデータを格納することを目指す
/// 
/// N.B.
//// 同じファイル内にインデックスやヘッダーが存在する場合、それらのサイズを考慮しないと正しいチャンクにならないが、今回は考慮しない
/// 
// SSTableDataはSSTableのデータのキャッシュ的な立ち位置として振る舞う
/// SSTable自体はイミュータブルなので、これに対する更新は起きない
/// TODO: キャッシュとしての振る舞いを実装する
#[derive(Clone)]
pub struct SSTableData{
    chunks: Vec<SSTableRecords>,
}

impl SSTableData {
    fn new() -> SSTableData {
        SSTableData {
            chunks: vec![],
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        self.chunks.iter().fold(vec![], |acc, chunk| {
            chunk.iter().fold(acc, |mut acc, record| {
                acc.extend_from_slice(&record.encode());
                acc
            })
        })
    }

    pub fn decode(data: &[u8]) -> Result<SSTableData, String> {
        let mut offset = 0;
        let mut chunks = vec![];
        let threadhold = get_page_size();
        while offset < data.len() {
            let (records, size) = SSTableRecords::decode(
                    &data[offset..], 
                    threadhold)
                .map_err(|e| e.to_string())?;
            chunks.push(records);
            offset += size;
        }
        Ok(SSTableData {
            chunks,
        })
    }

    fn get_first_keys(&self) -> Vec<Key> {
        self.chunks.iter().map(|chunk| {
            chunk.0[0].0.clone()
        }).collect()
    }

    // raw data length
    fn len(&self) -> usize {
        self.chunks.iter().fold(0, |acc, chunk| {
            acc + chunk.size()
        })
    }

    fn chunk_len(&self, index: usize) -> usize {
        self.chunks[index].size()
    }

    pub fn get(&self, key: &Key, hint: Option<Offset>) -> Option<&Value> {
        if let Some(hint) = hint {
            if let Some(value) = self.chunks[hint as usize].get(key) {
                return Some(value);
            }
        }
        self.binary_search_get(key)
    }

    fn brute_force_get(&self, key: &Key) -> Option<&Value> {
        for chunk in &self.chunks {
            for record in &chunk.0 {
                if record.0 == *key {
                    return Some(record.value());
                }
            }
        }
        None
    }

    // [left, right)
    // mid <= key < mid + 1 → chunk.get(mid)
    fn binary_search_get(&self, key: &Key) -> Option<&Value> {
        let mut left = 0;
        let mut right = self.chunks.len();
        while left < right {
            let mid = (left + right) / 2;
            let mid_letf_chunk_first_key = self.chunks[mid][0usize].key();
            let mid_right_chunk_first_key = if mid + 1 < self.chunks.len() {
                Some(self.chunks[mid + 1][0usize].key())
            } else {
                None
            };
            if mid_letf_chunk_first_key <= key && mid_right_chunk_first_key.map_or(true, |k| key < k) {
                return self.chunks[mid].get(key);
            }
            if mid_letf_chunk_first_key > key {
                right = mid;
            } else {
                left = mid + 1;
            }
        }
        None
    }

    pub fn iter(&self) -> SSTableDataIterator {
        SSTableDataIterator {
            chunks: &self.chunks,
            index: (0, 0),
        }
    }
}

impl fmt::Debug for SSTableData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for i in 0..self.chunks.len() {
            writeln!(f, "chunk{}: chunk_len={}, chunk_size={}", i, self.chunk_len(i), self.chunks[i].size())?;
            writeln!(f, "{:?}", self.chunks[i])?;
        }
        Ok(())
    }
}

impl TryFrom<&[u8]> for SSTableData {
    type Error = String;

    fn try_from(data: &[u8]) -> Result<Self, Self::Error> {
        Self::decode(data)
    }
}

impl TryFrom<Vec<u8>> for SSTableData {
    type Error = String;

    fn try_from(data: Vec<u8>) -> Result<Self, Self::Error> {
        Self::decode(&data)
    }
}

impl TryFrom<&Vec<u8>> for SSTableData {
    type Error = String;

    fn try_from(data: &Vec<u8>) -> Result<Self, Self::Error> {
        Self::decode(data)
    }
}

pub struct SSTableDataIterator<'a> {
    chunks: &'a Vec<SSTableRecords>,
    index: (usize, usize),
}

impl<'a> Iterator for SSTableDataIterator<'a> {
    type Item = &'a SSTableRecord;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index.0 >= self.chunks.len() {
            return None;
        }
        let record = &self.chunks[self.index.0][self.index.1];
        self.index.1 += 1;
        if self.index.1 >= self.chunks[self.index.0].len() {
            self.index.0 += 1;
            self.index.1 = 0;
        }
        Some(record)
    }
}

#[derive(Clone)]
pub struct SSTableRecords(Vec<SSTableRecord>);

impl SSTableRecords {
    fn new() -> SSTableRecords {
        SSTableRecords(vec![])
    }

    fn encode(&self) -> Vec<u8> {
        self.0.iter().fold(vec![], |mut acc, record| {
            acc.extend_from_slice(&record.encode());
            acc
        })
    }

    fn decode(data: &[u8], threadhold: usize) -> Result<(Self, usize), String> {
        let mut offset = 0;
        let mut records = Self::new();
        // TODO: ファイルフォーマットの見直し
        while offset < data.len() {
            let (record, record_size) = SSTableRecord::decode(&data[offset..])
                .map_err(|e| e.to_string())?;
            let ret = records.push(record, threadhold);
            if ret.is_err() {
                break;
            }
            offset += record_size;
        }
        Ok((records, offset))
    }

    fn iter(&self) -> SSTableRecordsIterator {
        SSTableRecordsIterator {
            iter: self.0.iter(),
        }
    }

    fn len(&self) -> usize {
        self.0.len()
    }

    fn size(&self) -> usize {
        self.0.iter().fold(0, |acc, record| {
            acc + record.size()
        })
    }

    fn get(&self, key: &Key) -> Option<&Value> {
        self.binary_search_get(key)
    }

    // [left, right)
    fn binary_search_get(&self, key: &Key) -> Option<&Value> {
        let mut left = 0;
        let mut right = self.0.len();
        while left < right {
            let mid = (left + right) / 2;
            let record = &self.0[mid];
            if record.0 == *key {
                return Some(record.value());
            }
            if record.0 < *key {
                left = mid + 1;
            } else {
                right = mid;
            }
        }
        None
    }

    fn push(&mut self, record: SSTableRecord, threadhold: usize) -> Result<(), String> {
        if self.size() >= threadhold {
            return Err("page is full".to_owned());
        }
        self.0.push(record);
        Ok(())
    }
}

impl fmt::Debug for SSTableRecords {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for record in &self.0 {
            writeln!(f, "{:?}", record)?;
        }
        Ok(())
    }
}

impl Index<usize> for SSTableRecords {
    type Output = SSTableRecord;

    fn index(&self, index: usize) -> &Self::Output {
        &self.0[index]
    }
}

impl Index<&usize> for SSTableRecords {
    type Output = SSTableRecord;

    fn index(&self, index: &usize) -> &Self::Output {
        &self.0[*index]
    }
}

impl Index<u64> for SSTableRecords {
    type Output = SSTableRecord;

    fn index(&self, index: u64) -> &Self::Output {
        &self.0[index as usize]
    }
}

impl Index<&u64> for SSTableRecords {
    type Output = SSTableRecord;

    fn index(&self, index: &u64) -> &Self::Output {
        &self.0[*index as usize]
    }
}

pub struct SSTableRecordsIterator<'a> {
    iter: std::slice::Iter<'a, SSTableRecord>,
}

impl<'a> Iterator for SSTableRecordsIterator<'a> {
    type Item = &'a SSTableRecord;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct SSTableRecord(Key, Value);

impl SSTableRecord {
    fn new(key: Key, value: Value) -> SSTableRecord {
        SSTableRecord(key, value)
    }

    fn key(&self) -> &Key {
        &self.0
    }

    fn value(&self) -> &Value {
        &self.1
    }

    fn encode(&self) -> Vec<u8> {
        let default_value = "".to_owned();
        let value = self.1.as_ref().unwrap_or(&default_value);
        let mut buf = Vec::new();
        buf.extend_from_slice(&self.0.len().to_ne_bytes());
        buf.extend_from_slice(self.0.as_bytes());
        buf.extend_from_slice(&value.len().to_ne_bytes());
        buf.extend_from_slice(value.as_bytes());
        buf
    }

    fn decode(data: &[u8]) -> Result<(SSTableRecord, usize), String> {
        let key_len = u64::from_ne_bytes(data[0..8]
            .try_into()
            .map_err(|e: std::array::TryFromSliceError| e.to_string())?);
        let key = String::from_utf8(data[8..(8 + key_len as usize)].to_vec())
            .map_err(|e| e.to_string())?;
        let value_len = u64::from_ne_bytes(data[(8 + key_len as usize)..(16 + key_len as usize)]
            .try_into()
            .map_err(|e: std::array::TryFromSliceError| e.to_string())?);
        let value = String::from_utf8(data[(16 + key_len as usize)..(16 + key_len as usize + value_len as usize)].to_vec())
            .map(|s| {
                if s == "" {
                    None
                } else {
                    Some(s)
                }
            })
            .map_err(|e| e.to_string())?;
        let len = std::mem::size_of::<u64>() * 2 + key_len as usize + value_len as usize;
        Ok((SSTableRecord(key, value), len))
    }

    fn size(&self) -> usize {
        self.0.len()
            + self.1.as_ref().map_or(0, |v| v.len())
            + std::mem::size_of::<u64>() * 2
    }
}

#[cfg(test)]
mod tests;
