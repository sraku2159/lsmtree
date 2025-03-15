use std::{fs::{self, metadata}, vec};

use crate::memtable;

use super::*;

const HEADER_SIZE: u64 = 16u64;

fn get_key_and_offset_len(num: usize) -> usize {
    num * 16
}

#[test]
fn test_sst_index_from_memtable() {
    let mut memtable = memtable::MemTable::new();
    memtable.put("a", "1");
    memtable.put("b", "2");
    memtable.put("c", "3");
    let sst_index = SSTableIndex::from(&memtable);
    assert_eq!(sst_index.0.len(), 1);
    let index_size = get_key_and_offset_len(1) + "a".len();
    assert_eq!(sst_index.get(
        &"a".to_owned()).unwrap(), 
        &(0 + HEADER_SIZE + index_size as u64
    ));
}

#[test]
fn test_sst_index_from_memtable_page_size_data() {
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
    let index_size = get_key_and_offset_len(4) + "0123".len();
    assert_eq!(sst_index.get(
        &"0".to_owned()).unwrap(), 
        &(0 + HEADER_SIZE + index_size as u64)
    );
    assert_eq!(sst_index.get(
        &"1".to_owned()).unwrap(), 
        &(get_page_size() as u64 + HEADER_SIZE + index_size as u64)
    );
    assert_eq!(sst_index.get(
        &"2".to_owned()).unwrap(), 
        &(get_page_size() as u64 * 2 + HEADER_SIZE + index_size as u64)
    );
    assert_eq!(sst_index.get(
        &"3".to_owned()).unwrap(), 
        &(get_page_size() as u64 * 3 + HEADER_SIZE + index_size as u64)
    );
}

#[test]
fn test_sst_index_from_memtable_crossing_page_size() {
    let mut memtable = MemTable::new();
    memtable.put("1", "a".repeat(get_page_size()).as_str());
    memtable.put("3", "c".repeat(get_page_size()).as_str());
    memtable.put("2", "b".repeat(get_page_size() / 2).as_str());
    memtable.put("キー4", "d");

    let sst_index = SSTableIndex::from(&memtable);

    assert_eq!(sst_index.0.len(), 3);
    let index_size = get_key_and_offset_len(3) + "12キー4".len();
    assert_eq!(
        sst_index.get(&"1".to_owned()).unwrap(), 
        &(0 + HEADER_SIZE + index_size as u64)
    );
    assert_eq!(
        sst_index.get(&"2".to_owned()).unwrap(), 
        &(get_page_size() as u64 + 17u64 + HEADER_SIZE + index_size as u64)
    );
    assert_eq!(
        sst_index.get(&"キー4".to_owned()).unwrap(),
        &(
            get_page_size() as u64 * 2 
                + 51u64 + get_page_size() as u64 / 2u64 
                + HEADER_SIZE + index_size as u64)
    );
}

#[test]
fn test_sst_index_tryfrom_data() {
    let mut data = SSTableData::new();
    data.extend_from_slice(&[
        1, 0, 0, 0, 0, 0, 0, 0, // key_len: 1
        97, // key: "a"
        1, 0, 0, 0, 0, 0, 0, 0, // value_len: 1
        49, // value: "1"
        1, 0, 0, 0, 0, 0, 0, 0, // key_len: 1
        98, // key: "b"
        1, 0, 0, 0, 0, 0, 0, 0, // value_len: 1
        50, // value: "2"
        1, 0, 0, 0, 0, 0, 0, 0, // key_len: 1
        99, // key: "c"
        1, 0, 0, 0, 0, 0, 0, 0, // value_len: 1
        51, // value: "3"
    ]);

    let index = SSTableIndex::try_from(&data).unwrap();
    let index_size = get_key_and_offset_len(1) + "a".len();
    assert_eq!(index.0.len(), 1);
    assert_eq!(index.get(
        &"a".to_owned()).unwrap(), 
        &(0 + HEADER_SIZE + index_size as u64)
    );
}

#[test]
fn test_sst_index_tryfrom_data_page_size_data() {
    let mut data = SSTableData::new();
    let value = "a".repeat(get_page_size() - 17); // 17 is the (bits of length of key and value) + (key length)
    
    for i in 0usize..4usize {
        data.extend_from_slice(&[
            i.to_string().len().to_ne_bytes().to_vec(),
            i.to_string().as_bytes().to_vec(),
            value.len().to_ne_bytes().to_vec(),
            value.as_bytes().to_vec(),
        ].concat());
    }

    let index = SSTableIndex::try_from(&data).unwrap();
    assert_eq!(index.0.len(), 4);
    let index_size = get_key_and_offset_len(4) + "0123".len();

    assert_eq!(index.get(
        &"0".to_owned()).unwrap(), 
        &(0 + HEADER_SIZE + index_size as u64)
    );
    assert_eq!(index.get(
        &"1".to_owned()).unwrap(), 
        &(get_page_size() as u64 + HEADER_SIZE + index_size as u64)
    );
    assert_eq!(index.get(
        &"2".to_owned()).unwrap(), 
        &(get_page_size() as u64 * 2 + HEADER_SIZE + index_size as u64)
    );
    assert_eq!(index.get(
        &"3".to_owned()).unwrap(), 
        &(get_page_size() as u64 * 3 + HEADER_SIZE + index_size as u64)
    );
}

#[test]
fn test_sst_index_tryfrom_data_crossing_page_size() {
    let data = SSTableData::from(vec![
        vec![
            1, 0, 0, 0, 0, 0, 0, 0, // key_len: 1
            49, // key: "1"
        ],
        get_page_size().to_ne_bytes().to_vec(),
        "a".repeat(get_page_size()).as_bytes().to_vec(),
        vec![
            1, 0, 0, 0, 0, 0, 0, 0, // key_len: 1
            51, // key: "3"
        ],
        get_page_size().to_ne_bytes().to_vec(),
        "c".repeat(get_page_size()).as_bytes().to_vec(),
        vec![
            1, 0, 0, 0, 0, 0, 0, 0, // key_len: 1
            50, // key: "2"
        ],
        (get_page_size() / 2).to_ne_bytes().to_vec(),
        "b".repeat(get_page_size() / 2).as_bytes().to_vec(),
        vec![
            6, 0, 0, 0, 0, 0, 0, 0, // key_len: 6
            227, 130, 173, 227, 131, 188, // key: "キー"
            1, 0, 0, 0, 0, 0, 0, 0, // value_len: 1
            100, // value: "d"
        ],
    ].concat());

    let index = SSTableIndex::try_from(&data).unwrap();
    let index_size = get_key_and_offset_len(3) + "12キー4".len() ;
    assert_eq!(
        index.0.len(), 
        3
    );
    assert_eq!(
        index.get(&"1".to_owned()).unwrap(), 
        &(0 + HEADER_SIZE + index_size as u64)
    );
    assert_eq!(
        index.get(
            &"2".to_owned()).unwrap(), 
            &(get_page_size() as u64 + 17u64 + HEADER_SIZE + index_size as u64)
        );
    assert_eq!(
        index.get(
            &"キー4".to_owned()).unwrap(), 
            &(get_page_size() as u64 * 2 
                + 51u64 + get_page_size() as u64 / 2u64 
                + HEADER_SIZE + index_size as u64)
        );
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
fn test_sst_data_iter() {
    let mut data = SSTableData::new();
    data.extend_from_slice(&[
        1, 0, 0, 0, 0, 0, 0, 0, // key_len: 1
        97, // key: "a"
        1, 0, 0, 0, 0, 0, 0, 0, // value_len: 1
        49, // value: "1"
        1, 0, 0, 0, 0, 0, 0, 0, // key_len: 1
        98, // key: "b"
        1, 0, 0, 0, 0, 0, 0, 0, // value_len: 1
        50, // value: "2"
        1, 0, 0, 0, 0, 0, 0, 0, // key_len: 1
        99, // key: "c"
        1, 0, 0, 0, 0, 0, 0, 0, // value_len: 1
        51, // value: "3"
    ]);

    let mut iter = data.iter();

    assert_eq!(
        iter.next().map(|(k, v)| (
            String::from_utf8(k.to_vec()).unwrap(), 
            String::from_utf8(v.to_vec()).unwrap())
        ),
        Some(("a".to_owned(), "1".to_owned()))
    );
    assert_eq!(
        iter.next().map(|(k, v)| (
            String::from_utf8(k.to_vec()).unwrap(), 
            String::from_utf8(v.to_vec()).unwrap())
        ),
        Some(("b".to_owned(), "2".to_owned()))
    );
    assert_eq!(
        iter.next().map(|(k, v)| (
            String::from_utf8(k.to_vec()).unwrap(), 
            String::from_utf8(v.to_vec()).unwrap())
        ),
        Some(("c".to_owned(), "3".to_owned()))
    );
    assert_eq!(iter.next(), None);
}