use std::collections::BTreeMap;

type Key = String;
type Value = String;

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
        self.data.insert(key.to_string(), value.to_string())
    }

    pub fn delete(&mut self, key: &str) -> Option<Value> {
        self.data.remove(key)
    }

    pub fn get(&self, key: &str) -> Option<Value> {
        self.data.get(key).map(|value| value.to_string())
    }

    pub fn len(&self) -> usize {
        let iter = MemtableIterator {
            iter: self.data.iter(),
        };
        iter.fold(0, |acc, val| acc + val.0.len() + val.1.len())
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        for (key, value) in self.data.iter() {
            buf.extend_from_slice(&Self::encode_key_value(key, value));
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