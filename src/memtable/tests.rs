use super::*;

#[test]
fn test_mt_len() {
    let mut memtable = MemTable::new();
    assert_eq!(memtable.len(), 0);
    memtable.put("key1", "value1");
    assert_eq!(memtable.len(), 10);
    memtable.put("key2", "value3");
    assert_eq!(memtable.len(), 20);
    memtable.delete("key1");
    assert_eq!(memtable.len(), 10);
}

#[test]
fn test_mt_len_dup() {
    let mut memtable = MemTable::new();
    memtable.put("key1", "value2");
    assert_eq!(memtable.len(), 10);
    memtable.put("key1", "value3");
    assert_eq!(memtable.len(), 10);
}

#[test]
fn test_mt_len_multi_byte() {
    let mut memtable = MemTable::new();
    memtable.put("キー", "バリュー");
    assert_eq!(memtable.len(), 18);
}

#[test]
fn test_mt_encode() {
    let mut memtable = MemTable::new();
    memtable.put("1", "a");
    memtable.put("234", "bcd");
    memtable.put("キー", "バリュー");
    assert_eq!(
        memtable.encode(),
        vec![
            1, 0, 0, 0, 0, 0, 0, 0,                                    // key_len: 1
            49,                                                        // key: "1"
            1, 0, 0, 0, 0, 0, 0, 0,                                    // value_len: 1
            97,                                                        // value: "a"

            3, 0, 0, 0, 0, 0, 0, 0,                                    // key_len: 3
            50, 51, 52,                                                // key: "234"
            3, 0, 0, 0, 0, 0, 0, 0,                                    // value_len: 3
            98, 99, 100,                                               // value: "bcd"

            6, 0, 0, 0, 0, 0, 0, 0,                                    // key_len: 6
            227, 130, 173, 227, 131, 188,                              // key: "キー"
            12, 0, 0, 0, 0, 0, 0, 0,                                   // value_len: 9  
            227, 131, 144, 227, 131, 170, 227, 131, 165, 227, 131, 188 // value: "バリュー"
        ]
    );
}