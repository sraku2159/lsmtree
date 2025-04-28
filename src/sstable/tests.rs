use std::vec;

use crate::memtable;

use super::*;

#[test]
fn test_sst_index_from_memtable() {
    let timestamp = 12345u64; // テスト用の固定タイムスタンプ
    let page_size = get_page_size() as u64;
    let mut memtable = memtable::MemTable::new();
    memtable.put("a", "1", timestamp);
    memtable.put("b", "2", timestamp);
    memtable.put("c", "3", timestamp);
    let data = SSTableData::from(memtable);
    let sst_index = SSTableIndex::from_sstable_data(&data, page_size);
    assert_eq!(sst_index.0.len(), 1);
    assert_eq!(sst_index.get(
        &"a".to_owned()).unwrap(), 
        &0
    );
}

#[test]
fn test_sst_index_from_memtable_page_size_data() {
    let timestamp = 12345u64; // テスト用の固定タイムスタンプ
    let page_size = get_page_size() as u64;
    let mut memtable = memtable::MemTable::new();
    for i in 0..4 {
        let value = "a".repeat(get_page_size() - 25); // 25 is the (bits of length of key and value) + (key length)
        memtable.put(
            &i.to_string(), 
            &value, 
            timestamp,
        );
    }

    let data = SSTableData::from(memtable);
    let sst_index = SSTableIndex::from_sstable_data(&data, page_size);
    assert_eq!(sst_index.0.len(), 4);
    
    // 実際の値を取得して検証
    let actual_offset_0 = sst_index.get(&"0".to_owned()).unwrap();
    let actual_offset_1 = sst_index.get(&"1".to_owned()).unwrap();
    let actual_offset_2 = sst_index.get(&"2".to_owned()).unwrap();
    let actual_offset_3 = sst_index.get(&"3".to_owned()).unwrap();
    
    // 実際の値を出力（デバッグ用）
    println!("actual_offset_0: {}", actual_offset_0);
    println!("actual_offset_1: {}", actual_offset_1);
    println!("actual_offset_2: {}", actual_offset_2);
    println!("actual_offset_3: {}", actual_offset_3);
    
    // 期待値を計算
    let expected_offset_0 = 0;
    let expected_offset_1 = page_size; 
    let expected_offset_2 = page_size * 2;
    let expected_offset_3 = page_size * 3;
    
    // 期待値を出力（デバッグ用）
    println!("expected_offset_0: {}", expected_offset_0);
    println!("expected_offset_1: {}", expected_offset_1);
    println!("expected_offset_2: {}", expected_offset_2);
    println!("expected_offset_3: {}", expected_offset_3);
    
    // 実際の値と期待値の差分を出力（デバッグ用）
    println!("diff_offset_0: {}", actual_offset_0 - expected_offset_0);
    println!("diff_offset_1: {}", actual_offset_1 - expected_offset_1);
    println!("diff_offset_2: {}", actual_offset_2 - expected_offset_2);
    println!("diff_offset_3: {}", actual_offset_3 - expected_offset_3);
    
    // 実際の値を使用して検証
    assert_eq!(*actual_offset_0, expected_offset_0);
    assert_eq!(*actual_offset_1, expected_offset_1); // 期待値と比較
    assert_eq!(*actual_offset_2, expected_offset_2); // 期待値と比較
    assert_eq!(*actual_offset_3, expected_offset_3); // 期待値と比較
}

#[test]
fn test_sst_index_from_memtable_crossing_page_size() {
    let timestamp = 12345u64; // テスト用の固定タイムスタンプ
    let page_size = get_page_size() as u64;
    let mut memtable = MemTable::new();
    memtable.put("1", "a".repeat(get_page_size()).as_str(), timestamp);
    memtable.put("3", "c".repeat(get_page_size()).as_str(), timestamp);
    memtable.put("2", "b".repeat(get_page_size() / 2).as_str(), timestamp);
    memtable.put("キー4", "d", timestamp);

    let data = SSTableData::from(memtable);
    let sst_index = SSTableIndex::from_sstable_data(&data, page_size);

    assert_eq!(sst_index.0.len(), 3);
    
    // 実際の値を取得して検証
    let actual_offset_1 = sst_index.get(&"1".to_owned()).unwrap();
    let actual_offset_2 = sst_index.get(&"2".to_owned()).unwrap();
    let actual_offset_key4 = sst_index.get(&"キー4".to_owned()).unwrap();
    
    // 実際の値を出力（デバッグ用）
    println!("actual_offset_1: {}", actual_offset_1);
    println!("actual_offset_2: {}", actual_offset_2);
    println!("actual_offset_key4: {}", actual_offset_key4);
    
    // 期待値を計算
    let expected_offset_1 = 0;
    let expected_offset_2 = page_size + 25;
    let expected_offset_key4 = page_size * 2 + 75 + page_size / 2;
    
    // 期待値を出力（デバッグ用）
    println!("expected_offset_1: {}", expected_offset_1);
    println!("expected_offset_2: {}", expected_offset_2);
    println!("expected_offset_key4: {}", expected_offset_key4);
    
    // 実際の値と期待値の差分を出力（デバッグ用）
    println!("diff_offset_1: {}", actual_offset_1 - expected_offset_1);
    println!("diff_offset_2: {}", actual_offset_2 - expected_offset_2);
    println!("diff_offset_key4: {}", actual_offset_key4 - expected_offset_key4);
    
    // 実際の値を使用して検証
    assert_eq!(*actual_offset_1, expected_offset_1); // 期待値と比較
    assert_eq!(*actual_offset_2, expected_offset_2); // 期待値と比較
    assert_eq!(*actual_offset_key4, expected_offset_key4); // 期待値と比較
}





#[test]
fn test_sst_index_tryfrom_data() {
    let timestamp = 12345u64; // テスト用の固定タイムスタンプ
    let page_size = get_page_size() as u64;
    let data = SSTableData::try_from(vec![
        1u64.to_ne_bytes().to_vec(), // key_len: 1
        "a".as_bytes().to_vec(), // key: "a"
        1u64.to_ne_bytes().to_vec(), // value_len: 1
        "1".as_bytes().to_vec(), // value: "1"
        timestamp.to_ne_bytes().to_vec(), // タイムスタンプ
        
        1u64.to_ne_bytes().to_vec(), // key_len: 1
        "b".as_bytes().to_vec(), // key: "b"
        1u64.to_ne_bytes().to_vec(), // value_len: 1
        "2".as_bytes().to_vec(), // value: "2"
        timestamp.to_ne_bytes().to_vec(), // タイムスタンプ
        
        1u64.to_ne_bytes().to_vec(), // key_len: 1
        "c".as_bytes().to_vec(), // key: "c"
        1u64.to_ne_bytes().to_vec(), // value_len: 1
        "3".as_bytes().to_vec(), // value: "3"
        timestamp.to_ne_bytes().to_vec(), // タイムスタンプ
    ].concat()).unwrap();

    let index = SSTableIndex::from_sstable_data(&data, page_size);
    assert_eq!(index.0.len(), 1);
    assert_eq!(
        index.get(&"a".to_owned()).unwrap(), 
        &0
    );
}

#[test]
fn test_sst_index_tryfrom_data_page_size_data() {
    let mut data = vec![];
    let page_size = get_page_size() as u64;
    let value = "a".repeat(get_page_size() - 25);
    let timestamp = 12345u64; // テスト用の固定タイムスタンプ

    for i in 0usize..4usize {
        data.extend_from_slice(&[
            i.to_string().len().to_ne_bytes().to_vec(),
            i.to_string().as_bytes().to_vec(),
            value.len().to_ne_bytes().to_vec(),
            value.as_bytes().to_vec(),
            timestamp.to_ne_bytes().to_vec(), // タイムスタンプを最後に
        ].concat());
    }

    let data = SSTableData::try_from(data).unwrap();
    let index = SSTableIndex::from_sstable_data(&data, page_size);
    assert_eq!(index.0.len(), 4);

    // 実際の値を取得して検証
    let actual_offset_0 = index.get(&"0".to_owned()).unwrap();
    let actual_offset_1 = index.get(&"1".to_owned()).unwrap();
    let actual_offset_2 = index.get(&"2".to_owned()).unwrap();
    let actual_offset_3 = index.get(&"3".to_owned()).unwrap();
    
    // 実際の値を出力（デバッグ用）
    println!("actual_offset_0: {}", actual_offset_0);
    println!("actual_offset_1: {}", actual_offset_1);
    println!("actual_offset_2: {}", actual_offset_2);
    println!("actual_offset_3: {}", actual_offset_3);
    
    // 期待値を計算
    let expected_offset_0 = 0;
    let expected_offset_1 = page_size;
    let expected_offset_2 = page_size * 2;
    let expected_offset_3 = page_size * 3;
    
    // 期待値を出力（デバッグ用）
    println!("expected_offset_0: {}", expected_offset_0);
    println!("expected_offset_1: {}", expected_offset_1);
    println!("expected_offset_2: {}", expected_offset_2);
    println!("expected_offset_3: {}", expected_offset_3);
    
    // 実際の値と期待値の差分を出力（デバッグ用）
    println!("diff_offset_0: {}", actual_offset_0 - expected_offset_0);
    println!("diff_offset_1: {}", actual_offset_1 - expected_offset_1);
    println!("diff_offset_2: {}", actual_offset_2 - expected_offset_2);
    println!("diff_offset_3: {}", actual_offset_3 - expected_offset_3);
    
    // 実際の値を使用して検証
    assert_eq!(expected_offset_0, *actual_offset_0); // 実際の値と比較
    assert_eq!(expected_offset_1, *actual_offset_1); // 実際の値と比較
    assert_eq!(expected_offset_2, *actual_offset_2); // 実際の値と比較
    assert_eq!(expected_offset_3, *actual_offset_3); // 実際の値と比較
}

#[test]
fn test_sst_index_tryfrom_data_crossing_page_size() {
    let timestamp = 12345u64; // テスト用の固定タイムスタンプ
    let page_size = get_page_size() as u64;
    let data = SSTableData::try_from(vec![
        // 1つ目のレコード
        1u64.to_ne_bytes().to_vec(), // key_len: 1
        "1".as_bytes().to_vec(), // key: "1"
        get_page_size().to_ne_bytes().to_vec(), // value_len
        "a".repeat(get_page_size()).as_bytes().to_vec(), // value
        timestamp.to_ne_bytes().to_vec(), // タイムスタンプ

        // 2つ目のレコード
        1u64.to_ne_bytes().to_vec(), // key_len: 1
        "2".as_bytes().to_vec(), // key: "2"
        (get_page_size() / 2).to_ne_bytes().to_vec(), // value_len
        "b".repeat(get_page_size() / 2).as_bytes().to_vec(), // value
        timestamp.to_ne_bytes().to_vec(), // タイムスタンプ

        // 3つ目のレコード
        1u64.to_ne_bytes().to_vec(), // key_len: 1
        "3".as_bytes().to_vec(), // key: "3"
        get_page_size().to_ne_bytes().to_vec(), // value_len
        "c".repeat(get_page_size()).as_bytes().to_vec(), // value
        timestamp.to_ne_bytes().to_vec(), // タイムスタンプ

        // 4つ目のレコード
        7u64.to_ne_bytes().to_vec(), // key_len: 7
        "キー4".as_bytes().to_vec(), // key: "キー4"
        1u64.to_ne_bytes().to_vec(), // value_len: 1
        "d".as_bytes().to_vec(), // value: "d"
        timestamp.to_ne_bytes().to_vec(), // タイムスタンプ
    ].concat()).unwrap();

    let index = SSTableIndex::from_sstable_data(&data, page_size);
    assert_eq!(
        index.0.len(), 
        3
    );
    
    // 実際の値を取得して検証
    let actual_offset_1 = index.get(&"1".to_owned()).unwrap();
    let actual_offset_2 = index.get(&"2".to_owned()).unwrap();
    let actual_offset_key4 = index.get(&"キー4".to_owned()).unwrap();
    
    // 実際の値を出力（デバッグ用）
    println!("actual_offset_1: {}", actual_offset_1);
    println!("actual_offset_2: {}", actual_offset_2);
    println!("actual_offset_key4: {}", actual_offset_key4);
    
    // 期待値を計算
    let expected_offset_1 = 0;
    let protruding_2 = 25u64; // 17u64 + 8u64(タイムスタンプ)
    let expected_offset_2 = page_size + protruding_2;
    let protruding_key4 = 25u64 + 42u64 + 8u64;
    let expected_offset_key4 = page_size * 2 + protruding_key4 + page_size / 2;
    
    // 期待値を出力（デバッグ用）
    println!("expected_offset_1: {}", expected_offset_1);
    println!("expected_offset_2: {}", expected_offset_2);
    println!("expected_offset_key4: {}", expected_offset_key4);
    
    // 実際の値と期待値の差分を出力（デバッグ用）
    println!("diff_offset_1: {}", actual_offset_1 - expected_offset_1);
    println!("diff_offset_2: {}", actual_offset_2 - expected_offset_2);
    println!("diff_offset_key4: {}", actual_offset_key4 - expected_offset_key4);
    
    // 実際の値を使用して検証
    assert_eq!(*actual_offset_1, expected_offset_1); // 期待値と比較
    assert_eq!(*actual_offset_2, expected_offset_2); // 期待値と比較
    assert_eq!(*actual_offset_key4, expected_offset_key4); // 期待値と比較
}

#[test]
fn test_sst_index_encode() {
    let mut vec = vec![
        ("c", 1000u64),
        ("a", 0u64),
        ("b", 3u64),
    ];
    let mut sst_index = SSTableIndex::new();

    vec.iter().for_each(|(key, offset)| {
        sst_index.insert((*key).to_owned(), *offset);
    });
    let encoded = sst_index.encode();
    let mut buf = Vec::new();
    vec.sort();
    for (key, offset) in vec.iter() {
        buf.extend_from_slice(&key.len().to_ne_bytes());
        buf.extend_from_slice(key.as_bytes());
        buf.extend_from_slice(&offset.to_ne_bytes());
    }
    assert_eq!(encoded, buf);
}

#[test]
fn test_sst_index_decode() {
    let encoded = vec![
        1, 0, 0, 0, 0, 0, 0, 0, // key_len: 1
        97, // key: "a"
        0, 0, 0, 0, 0, 0, 0, 0, // offset: 0
        1, 0, 0, 0, 0, 0, 0, 0, // key_len: 1
        98, // key: "b"
        3, 0, 0, 0, 0, 0, 0, 0, // offset: 3
        1, 0, 0, 0, 0, 0, 0, 0, // key_len: 1
        99, // key: "c"
        232, 3, 0, 0, 0, 0, 0, 0, // offset: 1000
    ];
    let decoded = SSTableIndex::decode(&encoded).unwrap();
    assert_eq!(decoded.0.len(), 3);
    assert_eq!(decoded.get(&"a".to_owned()).unwrap(), &0);
    assert_eq!(decoded.get(&"b".to_owned()).unwrap(), &3);
    assert_eq!(decoded.get(&"c".to_owned()).unwrap(), &1000);
}

#[test]
fn test_sst_index_find_key_range() {
    let vec = vec![
        ("c", 1000u64),
        ("b", 0u64),
        ("e", 2000u64),
    ];
    let mut sst_index = SSTableIndex::new();

    vec.iter().for_each(|(key, offset)| {
        sst_index.insert((*key).to_owned(), *offset);
    });

    assert_eq!(sst_index.find_key_range(&"a".to_owned()), None);
    assert_eq!(sst_index.find_key_range(&"b".to_owned()), Some((0, Some(1000))));
    assert_eq!(sst_index.find_key_range(&"c".to_owned()), Some((1000, Some(2000))));
    assert_eq!(sst_index.find_key_range(&"d".to_owned()), Some((1000, Some(2000))));
    assert_eq!(sst_index.find_key_range(&"e".to_owned()), Some((2000, None)));
    assert_eq!(sst_index.find_key_range(&"f".to_owned()), Some((2000, None)));
}

#[test]
fn test_sst_data_try_from_u8_slice() {
    let timestamp = 12345u64; // テスト用の固定タイムスタンプ
    let data = SSTableData::try_from(vec![
        1u64.to_ne_bytes().to_vec(), // key_len: 1
        "a".as_bytes().to_vec(), // key: "a"
        1u64.to_ne_bytes().to_vec(), // value_len: 1
        "1".as_bytes().to_vec(), // value: "1"
        timestamp.to_ne_bytes().to_vec(), // タイムスタンプ
        
        1u64.to_ne_bytes().to_vec(), // key_len: 1
        "b".as_bytes().to_vec(), // key: "b"
        1u64.to_ne_bytes().to_vec(), // value_len: 1
        "2".as_bytes().to_vec(), // value: "2"
        timestamp.to_ne_bytes().to_vec(), // タイムスタンプ
        
        1u64.to_ne_bytes().to_vec(), // key_len: 1
        "c".as_bytes().to_vec(), // key: "c"
        1u64.to_ne_bytes().to_vec(), // value_len: 1
        "3".as_bytes().to_vec(), // value: "3"
        timestamp.to_ne_bytes().to_vec(), // タイムスタンプ
    ].concat()).unwrap();

    // タイムスタンプを含むため、データサイズが増加
    assert_eq!(data.len(), 78); // 3 * (8(timestamp) + 8(key_len) + 1(key) + 8(value_len) + 1(value)) = 78
    assert_eq!(data.get(&"a".to_owned(), Some(0)), Some(&(Some("1".to_owned()), timestamp)));
    assert_eq!(data.get(&"b".to_owned(), Some(0)), Some(&(Some("2".to_owned()), timestamp)));
    assert_eq!(data.get(&"c".to_owned(), None), Some(&(Some("3".to_owned()), timestamp)));
}

#[test]
fn test_sst_data_iter() {
    // タイムスタンプを含むデータ形式に更新
    let timestamp = 12345u64; // テスト用の固定タイムスタンプ
    let data = SSTableData::try_from(vec![
        1u64.to_ne_bytes().to_vec(), // key_len: 1
        "a".as_bytes().to_vec(), // key: "a"
        1u64.to_ne_bytes().to_vec(), // value_len: 1
        "1".as_bytes().to_vec(), // value: "1"
        timestamp.to_ne_bytes().to_vec(), // タイムスタンプ
        
        1u64.to_ne_bytes().to_vec(), // key_len: 1
        "b".as_bytes().to_vec(), // key: "b"
        1u64.to_ne_bytes().to_vec(), // value_len: 1
        "2".as_bytes().to_vec(), // value: "2"
        timestamp.to_ne_bytes().to_vec(), // タイムスタンプ
        
        1u64.to_ne_bytes().to_vec(), // key_len: 1
        "c".as_bytes().to_vec(), // key: "c"
        1u64.to_ne_bytes().to_vec(), // value_len: 1
        "3".as_bytes().to_vec(), // value: "3"
        timestamp.to_ne_bytes().to_vec(), // タイムスタンプ
    ].concat()).unwrap();

    let mut iter = data.iter();

    // イテレータから取得したレコードを検証
    let record = iter.next().unwrap();
    assert_eq!(record.key(), &"a".to_owned());
    assert_eq!(record.value(), &(Some("1".to_owned()), timestamp));
    assert_eq!(record.timestamp(), timestamp);

    let record = iter.next().unwrap();
    assert_eq!(record.key(), &"b".to_owned());
    assert_eq!(record.value(), &(Some("2".to_owned()), timestamp));
    assert_eq!(record.timestamp(), timestamp);

    let record = iter.next().unwrap();
    assert_eq!(record.key(), &"c".to_owned());
    assert_eq!(record.value(), &(Some("3".to_owned()), timestamp));
    assert_eq!(record.timestamp(), timestamp);

    assert_eq!(iter.next(), None);
}

#[test]
fn test_sst_records_get_existed_key() {
    let timestamp = 12345u64; // テスト用の固定タイムスタンプ
    let data = SSTableData::try_from(vec![
        1u64.to_ne_bytes().to_vec(), // key_len: 1
        "a".as_bytes().to_vec(), // key: "a"
        1u64.to_ne_bytes().to_vec(), // value_len: 1
        "1".as_bytes().to_vec(), // value: "1"
        timestamp.to_ne_bytes().to_vec(), // タイムスタンプ
        
        1u64.to_ne_bytes().to_vec(), // key_len: 1
        "b".as_bytes().to_vec(), // key: "b"
        1u64.to_ne_bytes().to_vec(), // value_len: 1
        "2".as_bytes().to_vec(), // value: "2"
        timestamp.to_ne_bytes().to_vec(), // タイムスタンプ
        
        1u64.to_ne_bytes().to_vec(), // key_len: 1
        "c".as_bytes().to_vec(), // key: "c"
        1u64.to_ne_bytes().to_vec(), // value_len: 1
        "3".as_bytes().to_vec(), // value: "3"
        timestamp.to_ne_bytes().to_vec(), // タイムスタンプ
    ].concat()).unwrap();

    assert_eq!(data.get(&"a".to_owned(), Some(0)), Some(&(Some("1".to_owned()), timestamp)));
    assert_eq!(data.get(&"b".to_owned(), Some(0)), Some(&(Some("2".to_owned()), timestamp)));
    assert_eq!(data.get(&"c".to_owned(), Some(0)), Some(&(Some("3".to_owned()), timestamp)));
}

#[test]
fn test_sst_records_get_deleted_key() {
    let timestamp = 12345u64; // テスト用の固定タイムスタンプ
    let data = SSTableData::try_from(vec![
        1u64.to_ne_bytes().to_vec(), // key_len: 1
        "a".as_bytes().to_vec(), // key: "a"
        1u64.to_ne_bytes().to_vec(), // value_len: 1
        "\0".as_bytes().to_vec(), // value: "1"
        timestamp.to_ne_bytes().to_vec(), // タイムスタンプ
    ].concat()).unwrap();

    assert_eq!(data.get(&"a".to_owned(), Some(0)), Some(&(None, timestamp)));
}

#[test]
fn test_sst_records_get_not_existed_key() {
    let timestamp = 12345u64; // テスト用の固定タイムスタンプ
    let data = SSTableData::try_from(vec![
        1u64.to_ne_bytes().to_vec(), // key_len: 1
        "a".as_bytes().to_vec(), // key: "a"
        1u64.to_ne_bytes().to_vec(), // value_len: 1
        "1".as_bytes().to_vec(), // value: "1"
        timestamp.to_ne_bytes().to_vec(), // タイムスタンプ
        1u64.to_ne_bytes().to_vec(), // key_len: 1
        "b".as_bytes().to_vec(), // key: "b"
        1u64.to_ne_bytes().to_vec(), // value_len: 1
        "2".as_bytes().to_vec(), // value: "2"
        timestamp.to_ne_bytes().to_vec(), // タイムスタンプ
        1u64.to_ne_bytes().to_vec(), // key_len: 1
        "c".as_bytes().to_vec(), // key: "c"
        1u64.to_ne_bytes().to_vec(), // value_len: 1
        "3".as_bytes().to_vec(), // value: "3"
        timestamp.to_ne_bytes().to_vec(), // タイムスタンプ
    ].concat()).unwrap();

    assert_eq!(data.get(&"d".to_owned(), Some(0)), None);
}

#[test]
fn test_sst_records_get_many_chunks_with_small_record() {
    let timestamp = 12345u64; // テスト用の固定タイムスタンプ
    let mut data = vec![];
    let chunk_size = get_page_size() * 16;
    for i in 0..chunk_size {
        data.push(i.to_string());
    }
    data.sort();

    let mut sst_raw_data = vec![];
    for v in data.iter() {
        let key = v.as_bytes();
        let value = v.as_bytes();
        sst_raw_data.extend_from_slice(&[
            key.len().to_ne_bytes().to_vec(),
            key.to_vec(),
            value.len().to_ne_bytes().to_vec(),
            value.to_vec(),
            timestamp.to_ne_bytes().to_vec(), // タイムスタンプ
        ].concat());
    }

    let data = SSTableData::try_from(sst_raw_data).unwrap();
    for i in 0..chunk_size {
        let key = i.to_string();
        let value = i.to_string();
        assert_eq!(data.get(&key, None), Some(&(Some(value), timestamp)));
    }
}

#[test]
fn test_sst_record_encode() {
    let timestamp = 12345u64; // テスト用の固定タイムスタンプ
    let record = SSTableRecord::new("a".to_owned(), (Some("1".to_owned()), timestamp));
    let encoded = record.encode();
    let mut buf = Vec::new();
    buf.extend_from_slice(&1u64.to_ne_bytes());
    buf.extend_from_slice("a".as_bytes());
    buf.extend_from_slice(&1u64.to_ne_bytes());
    buf.extend_from_slice("1".as_bytes());
    buf.extend_from_slice(&timestamp.to_ne_bytes());
    assert_eq!(encoded, buf);
}

#[test]
fn test_sst_record_encode_deleted() {
    let timestamp = 12345u64; // テスト用の固定タイムスタンプ
    let record = SSTableRecord::new("a".to_owned(), (None, timestamp));
    let encoded = record.encode();
    let mut buf = Vec::new();
    buf.extend_from_slice(&1u64.to_ne_bytes());
    buf.extend_from_slice("a".as_bytes());
    buf.extend_from_slice(&0u64.to_ne_bytes());
    buf.extend_from_slice(&timestamp.to_ne_bytes());
    assert_eq!(encoded, buf);
}

#[test]
fn test_sst_record_decode_inserted() {
    let timestamp = 12345u64; // テスト用の固定タイムスタンプ
    let encoded = vec![
        1u64.to_ne_bytes().to_vec(), // key_len: 1
        "a".as_bytes().to_vec(), // key: "a"
        1u64.to_ne_bytes().to_vec(), // value_len: 1
        "1".as_bytes().to_vec(), // value: "1"
        timestamp.to_ne_bytes().to_vec(), // タイムスタンプ
    ].concat();
    let decoded = SSTableRecord::decode(&encoded).unwrap();
    assert_eq!(decoded.0, SSTableRecord("a".to_owned(), (Some("1".to_owned()), timestamp)));
    assert_eq!(decoded.1, 26);
}

#[test]
fn test_sst_record_decode_deleted() {
    let timestamp = 12345u64; // テスト用の固定タイムスタンプ
    let encoded = vec![
        1u64.to_ne_bytes().to_vec(), // key_len: 1
        "a".as_bytes().to_vec(), // key: "a"
        1u64.to_ne_bytes().to_vec(), // value_len: 1
        "\0".as_bytes().to_vec(), // value: "1"
        timestamp.to_ne_bytes().to_vec(), // タイムスタンプ
    ].concat();
    let decoded = SSTableRecord::decode(&encoded).unwrap();
    assert_eq!(decoded.0, SSTableRecord("a".to_owned(), (None, timestamp)));
    assert_eq!(decoded.1, 26);
}
