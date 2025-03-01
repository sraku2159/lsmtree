use std::collections::BTreeMap;

#[derive(Debug, Clone)]
pub struct MemTable {
    data: BTreeMap<String, String>, 
}

impl MemTable {
    pub fn new() -> MemTable {
        MemTable {
            data: BTreeMap::new(),
        }
    }

    pub fn put(&mut self, key: &str, value: &str) -> Option<String> {
        self.data.insert(key.to_string(), value.to_string())
    }

    pub fn delete(&mut self, key: &str) -> Option<String> {
        self.data.remove(key)
    }

    pub fn get(&self, key: &str) -> Option<String> {
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
            buf.extend_from_slice(&key.len().to_ne_bytes());
            buf.extend_from_slice(key.as_bytes());
            buf.extend_from_slice(&value.len().to_ne_bytes());
            buf.extend_from_slice(value.as_bytes());
        }
        buf
    }
}

struct MemtableIterator<'a> {
    pub iter: std::collections::btree_map::Iter<'a, String, String>,
}

impl<'a> Iterator for MemtableIterator<'a> {
    type Item = (String, String);

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|(key, value)| (key.clone(), value.clone()))
    }
}


#[cfg(test)]
mod tests;