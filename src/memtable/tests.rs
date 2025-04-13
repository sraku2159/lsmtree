use super::*;

#[test]
fn test_mt_len() {
    let timestamp = crate::utils::get_timestamp() as u64;
    let mut memtable = MemTable::new();
    assert_eq!(memtable.len(), 0);
    memtable.put("key1", "value1", timestamp);
    assert_eq!(memtable.len(), 18);
    memtable.put("key2", "value3", timestamp);
    assert_eq!(memtable.len(), 36);
    memtable.delete("key1", timestamp);
    assert_eq!(memtable.len(), 30);
}

#[test]
fn test_mt_len_empty() {
    let timestamp = crate::utils::get_timestamp() as u64;
    let mut memtable = MemTable::new();
    memtable.put("key1", "", timestamp);
    let encoded = memtable.encode();
    println!("{:?}", encoded);

    memtable.delete("key1", timestamp);
    let encoded = memtable.encode();
    println!("{:?}", encoded);
}


#[test]
fn test_mt_len_dup() {
    let timestamp = crate::utils::get_timestamp() as u64;
    let mut memtable = MemTable::new();
    memtable.put("key1", "value2", timestamp);
    assert_eq!(memtable.len(), 18);
    memtable.put("key1", "value3", timestamp);
    assert_eq!(memtable.len(), 18);
}

#[test]
fn test_mt_len_multi_byte() {
    let timestamp = crate::utils::get_timestamp() as u64;
    let mut memtable = MemTable::new();
    memtable.put("キー", "バリュー", timestamp);
    assert_eq!(memtable.len(), 26);
}

#[test]
fn test_mt_encode() {
    let timestamp = crate::utils::get_timestamp() as u64;
    let mut memtable = MemTable::new();
    memtable.put("1",   "a", timestamp);
    memtable.put("234", "bcd", timestamp);
    memtable.put("キー", "バリュー", timestamp);
    
    let encoded = memtable.encode();
    
    // タイムスタンプ以外の部分を検証
    assert_eq!(&encoded[0..26], &[
        1u64.to_ne_bytes().to_vec(), // key_len: 1
        "1".as_bytes().to_vec(),                        // key: "1"
        1u64.to_ne_bytes().to_vec(), // key_len: 1
        "a".as_bytes().to_vec(),                        // value: "a"
        timestamp.to_ne_bytes().to_vec(), // timestamp: 8 bytes
    ].concat());
    
    // 2番目のレコードの検証（タイムスタンプを除く）
    assert_eq!(&encoded[26..56], &[
        3u64.to_ne_bytes().to_vec(), // key_len: 3
        "234".as_bytes().to_vec(),                // key: "234"
        3u64.to_ne_bytes().to_vec(), // value_len: 3
        "bcd".as_bytes().to_vec(),                // value: "bcd"
        timestamp.to_ne_bytes().to_vec(), // timestamp: 8 bytes
    ].concat());

    // 3番目のレコードの検証（タイムスタンプを除く）
    assert_eq!(&encoded[56..98], &[
        6u64.to_ne_bytes().to_vec(), // key_len: 6
        "キー".as_bytes().to_vec(),                // key: "キー"
        12u64.to_ne_bytes().to_vec(), // value_len: 12
        "バリュー".as_bytes().to_vec(),                // value: "バリュー"
        timestamp.to_ne_bytes().to_vec(), // timestamp: 8 bytes
    ].concat());
    
    memtable.delete("1", timestamp + 1);
    let encoded = memtable.encode();
    
    // 削除後のエンコードも検証
    assert_eq!(&encoded[0..25], &[
        1u64.to_ne_bytes().to_vec(), // key_len: 1
        "1".as_bytes().to_vec(),                        // key: "1"
        0u64.to_ne_bytes().to_vec(), // value_len: 0
        (timestamp + 1).to_ne_bytes().to_vec(), // timestamp: 8 bytes
    ].concat());
}

#[test]
fn test_mt_delete() {
    let timestamp = crate::utils::get_timestamp() as u64;
    let mut memtable = MemTable::new();
    memtable.put("key1", "value1", timestamp);
    assert_eq!(memtable.get("key1"), Some(Value::Data("value1".to_owned(), timestamp)));

    memtable.delete("key1", timestamp);
    assert_eq!(memtable.get("key1"), Some(Value::Tombstone(timestamp)));
}
