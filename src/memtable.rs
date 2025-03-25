use std::{collections::BTreeMap, fmt::Display};

type Key = String;
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Data(String),
    /*
    Tombstone: 削除されたデータを表す
        1, 0, 0, 0, 0, 0, 0, 0, // key_len: 1
        0,
    empty string:
        0, 0, 0, 0, 0, 0, 0, 0, // key_len: 0
     */
    Tombstone,
}

impl Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Data(data) => write!(f, "{}", data),
            Value::Tombstone => write!(f, "\u{0}"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct MemTable {
    data: BTreeMap<Key, Value>, 
}

impl MemTable {
    pub fn new() -> MemTable {
        MemTable {
            data: BTreeMap::new(),
        }
    }

    pub fn put(&mut self, key: &str, value: &str) -> Option<Value> {
        self.data.insert(key.to_string(), Value::Data(value.to_string()))
    }

    pub fn delete(&mut self, key: &str) -> Option<Value> {
        self.data.insert(key.to_string(), Value::Tombstone)
    }

    pub fn get(&self, key: &str) -> Option<Value> {
        self.data.get(key).map(|value| {
            value.clone()
        })
    }

    pub fn len(&self) -> usize {
        let iter = MemtableIterator {
            iter: self.data.iter(),
        };
        iter.fold(0, |acc, val| acc + val.0.len() + val.1.to_string().len())
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        for (key, value) in self.data.iter() {
            buf.extend_from_slice(&Self::encode_key_value(key, &value.to_string()));
        }
        buf
    }

    pub fn encode_key_value(key: &str, value: &str) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&key.len().to_ne_bytes());
        buf.extend_from_slice(key.as_bytes());
        buf.extend_from_slice(&value.len().to_ne_bytes());
        buf.extend_from_slice(value.as_bytes());
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