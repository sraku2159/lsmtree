use std::fs;

use super::*;

#[test]
fn test_sst_write_impl() {
    let mut memtable = MemTable::new();
    memtable.put("key1", "value1");
    let path = "/tmp/test.sst";
    assert!(SSTable::write_impl(&memtable, path).is_ok());
    
    let content = fs::read_to_string(path).unwrap();
    assert_eq!(
        content, 
        "\u{4}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}key1\u{6}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}value1"
    );
    fs::remove_file(path).unwrap();
}

#[test]
fn test_sst_create_dir() {
    let path = "/tmp/test";
    assert!(SSTable::create_dir(path).is_ok());
    fs::remove_dir(path).unwrap();
}