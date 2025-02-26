use std::collections::BTreeMap;

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

    pub fn get(&self, key: &str) -> Option<String> {
        self.data.get(key).map(|value| value.to_string())
    }
}