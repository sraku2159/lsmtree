use super::*;

#[test]
fn test_len() {
    let mut memtable = MemTable::new();
    assert_eq!(memtable.len(), 0);
    memtable.put("key1", "value1");
    assert_eq!(memtable.len(), 10);
    memtable.put("key2", "value2");
    assert_eq!(memtable.len(), 20);
    memtable.delete("key1");
    assert_eq!(memtable.len(), 10);
}

#[test]
fn test_encode() {
    let mut memtable = MemTable::new();
    memtable.put("1", "a");
    memtable.put("2", "b");
    assert_eq!(
        memtable.encode(),
        vec![

        ]
    );
}