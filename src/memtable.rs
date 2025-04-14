use std::{collections::BTreeMap, fmt::Display};

type Key = String;
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
    Data(String, u64), // (value, timestamp)
    /*
    Tombstone: 削除されたデータを表す
        1, 0, 0, 0, 0, 0, 0, 0, // key_len: 1
        0,
    empty string:
        0, 0, 0, 0, 0, 0, 0, 0, // key_len: 0
     */
    Tombstone(u64),
}

impl Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Data(value, timestamp) => write!(f, "value: {}, timestamp: {}", value, timestamp),
            Value::Tombstone(timestamp) => write!(f, "Tombstone, timestamp: {}", timestamp),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemTable {
    data: BTreeMap<Key, Value>, 
}

impl MemTable {
    pub fn new() -> MemTable {
        MemTable {
            data: BTreeMap::new(),
        }
    }

    pub fn put(&mut self, key: &str, value: &str, timestamp: u64) -> Option<Value> {
        self.data.insert(key.to_string(), Value::Data(value.to_string(), timestamp))
    }

    pub fn delete(&mut self, key: &str, timestamp: u64) -> Option<Value> {
        self.data.insert(key.to_string(), Value::Tombstone(timestamp))
    }

    pub fn get(&self, key: &str) -> Option<Value> {
        self.data.get(key).map(|value| {
            value.clone()
        })
    }

    pub fn clear(&mut self) {
        self.data.clear();
    }

    pub fn len(&self) -> usize {
        self.data.iter().map(|(key, value)| {
            let key_len = key.len() + std::mem::size_of::<u64>(); // key_len + timestamp
            match value {
                Value::Data(value, _) => key_len + value.len(),
                Value::Tombstone(_) => key_len,
            }
        }).sum()
    }

    // Memtableの責務かどうかは微妙
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        for (key, value) in self.data.iter() {
            let (value, timestamp) = match value {
                Value::Data(value, timestamp) => (Some(value), *timestamp),
                Value::Tombstone(timestamp) => (None, *timestamp),
            };
            buf.extend_from_slice(
                &Self::encode_key_value(key, value.as_deref().map(|x| x.as_str()), timestamp),
            );
        }
        buf
    }

    pub fn encode_key_value(key: &str, value: Option<&str>, timestamp: u64) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&key.len().to_ne_bytes());
        buf.extend_from_slice(key.as_bytes());
        if let Some(value) = value {
            buf.extend_from_slice(&value.len().to_ne_bytes());
            buf.extend_from_slice(value.as_bytes());
        } else {
            buf.extend_from_slice(&0u64.to_ne_bytes());
        }
        buf.extend_from_slice(&timestamp.to_ne_bytes());
        buf
    }

    pub fn iter(&self) -> MemtableIterator {
        MemtableIterator {
            iter: self.data.iter(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

pub struct MemtableIterator<'a> {
    pub iter: std::collections::btree_map::Iter<'a, Key, Value>,
}

impl<'a> Iterator for MemtableIterator<'a> {
    type Item = (Key, Value);

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|(key, value)| (key.clone(), value.clone()))
    }
}


#[cfg(test)]
mod tests;
