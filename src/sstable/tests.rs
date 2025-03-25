use std::vec;

use crate::memtable;

use super::*;

const HEADER_SIZE: u64 = 16u64;

// key size: u64
// offset size: u64
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
    let data = SSTableData::try_from(vec![
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
    ]).unwrap();

    let index = SSTableIndex::try_from(&data).unwrap();
    let index_size = get_key_and_offset_len(1) + "a".len();
    assert_eq!(index.0.len(), 1);
    assert_eq!(
        index.get(&"a".to_owned()).unwrap(), 
        &(0 + HEADER_SIZE + index_size as u64)
    );
}

#[test]
fn test_sst_index_tryfrom_data_page_size_data() {
    let mut data = vec![];
    let value = "a".repeat(get_page_size() - 17); // 17 is the (bits of length of key and value) + (key length)

    for i in 0usize..4usize {
        data.extend_from_slice(&[
            i.to_string().len().to_ne_bytes().to_vec(),
            i.to_string().as_bytes().to_vec(),
            value.len().to_ne_bytes().to_vec(),
            value.as_bytes().to_vec(),
        ].concat());
    }

    let data = SSTableData::try_from(data).unwrap();
    let index = SSTableIndex::try_from(&data).unwrap();
    assert_eq!(index.0.len(), 4);
    let index_size = get_key_and_offset_len(4) + "0123".len();

    assert_eq!(
        index.get(&"0".to_owned()).unwrap(), 
        &(0 + HEADER_SIZE + index_size as u64)
    );
    assert_eq!(
        index.get(&"1".to_owned()).unwrap(), 
        &(get_page_size() as u64 + HEADER_SIZE + index_size as u64)
    );
    assert_eq!(
        index.get(&"2".to_owned()).unwrap(), 
        &(get_page_size() as u64 * 2 + HEADER_SIZE + index_size as u64)
    );
    assert_eq!(
        index.get(&"3".to_owned()).unwrap(), 
        &(get_page_size() as u64 * 3 + HEADER_SIZE + index_size as u64)
    );
}

#[test]
fn test_sst_index_tryfrom_data_crossing_page_size() {
    let data = SSTableData::try_from(vec![
        vec![
            1, 0, 0, 0, 0, 0, 0, 0, // key_len: 1
            49, // key: "1"
        ],
        get_page_size().to_ne_bytes().to_vec(),
        "a".repeat(get_page_size()).as_bytes().to_vec(),

        vec![
            1, 0, 0, 0, 0, 0, 0, 0, // key_len: 1
            50, // key: "2"
        ],
        (get_page_size() / 2).to_ne_bytes().to_vec(),
        "b".repeat(get_page_size() / 2).as_bytes().to_vec(),

        vec![
            1, 0, 0, 0, 0, 0, 0, 0, // key_len: 1
            51, // key: "3"
        ],
        get_page_size().to_ne_bytes().to_vec(),
        "c".repeat(get_page_size()).as_bytes().to_vec(),

        vec![
            7, 0, 0, 0, 0, 0, 0, 0, // key_len: 7
            227, 130, 173, 227, 131, 188, 52,// key: "キー4"
            1, 0, 0, 0, 0, 0, 0, 0, // value_len: 1
            100, // value: "d"
        ],
    ].concat()).unwrap();

    let index = SSTableIndex::try_from(&data).unwrap();
    let index_size = get_key_and_offset_len(3) + "12キー4".len();
    assert_eq!(
        index.0.len(), 
        3
    );
    assert_eq!(
        index.get(&"1".to_owned()).unwrap(), 
        &(0 + HEADER_SIZE + index_size as u64)
    );

    let protruding = 17u64;
    assert_eq!(
        index.get(
            &"2".to_owned()).unwrap(), 
            &(get_page_size() as u64 + protruding + HEADER_SIZE + index_size as u64)
        );

    let protruding = 17u64 + 34u64;
    assert_eq!(
        index.get(
            &"キー4".to_owned()).unwrap(), 
            &(get_page_size() as u64 * 2 
                + protruding + get_page_size() as u64 / 2u64 
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
        ("a", 0u64),
        ("e", 2000u64),
    ];
    let mut sst_index = SSTableIndex::new();

    vec.iter().for_each(|(key, offset)| {
        sst_index.insert((*key).to_owned(), *offset);
    });

    assert_eq!(sst_index.find_key_range(&"a".to_owned()), (0, Some(1000)));
    assert_eq!(sst_index.find_key_range(&"b".to_owned()), (0, Some(1000)));
    assert_eq!(sst_index.find_key_range(&"c".to_owned()), (1000, Some(2000)));
    assert_eq!(sst_index.find_key_range(&"d".to_owned()), (1000, Some(2000)));
    assert_eq!(sst_index.find_key_range(&"e".to_owned()), (2000, None));
    assert_eq!(sst_index.find_key_range(&"f".to_owned()), (2000, None));
}

#[test]
fn test_sst_data_try_from_u8_slice() {
    let data = SSTableData::try_from(vec![
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
    ]).unwrap();

    assert_eq!(data.len(), 54);
    assert_eq!(data.get(&"a".to_owned(), Some(0)), Some(&Some("1".to_owned())));
    assert_eq!(data.get(&"b".to_owned(), Some(0)), Some(&Some("2".to_owned())));
    assert_eq!(data.get(&"c".to_owned(), None), Some(&Some("3".to_owned())));
}

#[test]
fn test_sst_data_iter() {
    let data = SSTableData::try_from(vec![
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
    ]).unwrap();

    let mut iter = data.iter();

    assert_eq!(
        iter.next(),
        Some(&SSTableRecord("a".to_owned(), Some("1".to_owned())))
    );
    assert_eq!(
        iter.next(),
        Some(&SSTableRecord("b".to_owned(), Some("2".to_owned())))
    );
    assert_eq!(
        iter.next(),
        Some(&SSTableRecord("c".to_owned(), Some("3".to_owned())))
    );
    assert_eq!(iter.next(), None);
}

#[test]
fn test_sst_records_get_existed_key() {
    let data = SSTableData::try_from(vec![
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
    ]).unwrap();

    assert_eq!(data.get(&"a".to_owned(), Some(0)), Some(&Some("1".to_owned())));
    assert_eq!(data.get(&"b".to_owned(), Some(0)), Some(&Some("2".to_owned())));
    assert_eq!(data.get(&"c".to_owned(), Some(0)), Some(&Some("3".to_owned())));
}

#[test]
fn test_sst_records_get_deleted_key() {
    let data = SSTableData::try_from(vec![
        1, 0, 0, 0, 0, 0, 0, 0, // key_len: 1
        97, // key: "a"
        0, 0, 0, 0, 0, 0, 0, 0, // value_len: 1
    ]).unwrap();

    assert_eq!(data.get(&"a".to_owned(), Some(0)), Some(&None));
}

#[test]
fn test_sst_records_get_not_existed_key() {
    let data = SSTableData::try_from(vec![
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
    ]).unwrap();

    assert_eq!(data.get(&"d".to_owned(), Some(0)), None);
}

#[test]
fn test_sst_records_get_many_chunks_with_small_record() {
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
        ].concat());
    }

    let data = SSTableData::try_from(sst_raw_data).unwrap();
    for i in 0..chunk_size {
        let key = i.to_string();
        let value = i.to_string();
        assert_eq!(data.get(&key, None), Some(&Some(value)));
    }
}

#[test]
fn test_sst_record_encode() {
    let record = SSTableRecord::new("a".to_owned(), Some("1".to_owned()));
    let encoded = record.encode();
    let mut buf = Vec::new();
    buf.extend_from_slice(&1u64.to_ne_bytes());
    buf.extend_from_slice("a".as_bytes());
    buf.extend_from_slice(&1u64.to_ne_bytes());
    buf.extend_from_slice("1".as_bytes());
    assert_eq!(encoded, buf);
}

#[test]
fn test_sst_record_encode_deleted() {
    let record = SSTableRecord::new("a".to_owned(), None);
    let encoded = record.encode();
    let mut buf = Vec::new();
    buf.extend_from_slice(&1u64.to_ne_bytes());
    buf.extend_from_slice("a".as_bytes());
    buf.extend_from_slice(&0u64.to_ne_bytes());
    assert_eq!(encoded, buf);
}

#[test]
fn test_sst_record_decode_inserted() {
    let encoded = vec![
        1, 0, 0, 0, 0, 0, 0, 0, // key_len: 1
        97, // key: "a"
        1, 0, 0, 0, 0, 0, 0, 0, // value_len: 1
        49, // value: "1"
    ];
    let decoded = SSTableRecord::decode(&encoded).unwrap();
    assert_eq!(decoded.0, SSTableRecord("a".to_owned(), Some("1".to_owned())));
    assert_eq!(decoded.1, 18);
}

#[test]
fn test_sst_record_decode_deleted() {
    let encoded = vec![
        1, 0, 0, 0, 0, 0, 0, 0, // key_len: 1
        97, // key: "a"
        0, 0, 0, 0, 0, 0, 0, 0, // value_len: 0
    ];
    let decoded = SSTableRecord::decode(&encoded).unwrap();
    assert_eq!(decoded.0, SSTableRecord("a".to_owned(), None));
    assert_eq!(decoded.1, 17);
}