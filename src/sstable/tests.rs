use std::{fs::{self, metadata}, vec};

use crate::memtable;

use super::*;

#[test]
fn test_sst_index_from() {
    let mut memtable = memtable::MemTable::new();
    memtable.put("a", "1");
    memtable.put("b", "2");
    memtable.put("c", "3");
    let sst_index = SSTableIndex::from(&memtable);
    assert_eq!(sst_index.0.len(), 1);
    assert_eq!(sst_index.get(&"a".to_owned()).unwrap(), &0);
}

#[test]
fn test_sst_index_from_page_size_data() {
    let mut memtable = memtable::MemTable::new();
    for i in 0..4 {
        let value = "a".repeat(get_page_size() - 17); // 17 is the (bits of length of key and value) + (key length)
        println!("Length of value: {}", value.len());
        memtable.put(
            &i.to_string(), 
            &value
        );
    }
    let sst_index = SSTableIndex::from(&memtable);
    assert_eq!(sst_index.0.len(), 4);
    assert_eq!(sst_index.get(&"0".to_owned()).unwrap(), &0);
    assert_eq!(sst_index.get(&"1".to_owned()).unwrap(), &(get_page_size() as u64));
    assert_eq!(sst_index.get(&"2".to_owned()).unwrap(), &(get_page_size() as u64 * 2));
    assert_eq!(sst_index.get(&"3".to_owned()).unwrap(), &(get_page_size() as u64 * 3));
}

#[test]
fn test_sst_index_from_data_crossing_page_size() {
    let mut memtable = MemTable::new();
    memtable.put("1", "a".repeat(get_page_size()).as_str());
    memtable.put("2", "b".repeat(get_page_size() / 2).as_str());
    memtable.put("3", "c".repeat(get_page_size()).as_str());
    memtable.put("4", "d");

    let sst_index = SSTableIndex::from(&memtable);
    assert_eq!(sst_index.0.len(), 3);
    assert_eq!(
        sst_index.get(&"1".to_owned()).unwrap(), 
        &0
    );
    assert_eq!(
        sst_index.get(&"2".to_owned()).unwrap(), 
        &(get_page_size() as u64 + 17u64)
    );
    assert_eq!(
        sst_index.get(&"4".to_owned()).unwrap(),
        &(get_page_size() as u64 * 2 + 51u64 + get_page_size() as u64 / 2u64)
    );
}