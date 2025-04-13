use crate::{memtable::MemTable, sstable::SSTableData};

fn create_sstable_data(data: Vec<(&str, &str, u64)>) -> SSTableData {
    let mut memtable = MemTable::new();
    for (key, value, timestamp) in data.iter() {
        memtable.put(key, value, *timestamp);
    }
    SSTableData::from(memtable)
}

#[test]
fn test_merge_simple() {
    let left_data = vec![
        ("key1", "value1", 1),
        ("key2", "value2", 2),
    ];
    let left = create_sstable_data(left_data);

    let right_data = vec![
        ("key3", "value3", 3),
        ("key4", "value4", 4),
    ];
    let right = create_sstable_data(right_data);

    let expected = vec![
        ("key1", "value1", 1),
        ("key2", "value2", 2),
        ("key3", "value3", 3),
        ("key4", "value4", 4),
    ];
    let expected = create_sstable_data(expected);
    let size_tiered_compaction = super::SizeTieredCompaction::new();
    let merged = size_tiered_compaction.merge(&left, &right);
    assert_eq!(merged, expected);
}

#[test]
fn test_merge_duplicated() {
    let left_data = vec![
        ("key1", "value1", 1),
        ("key2", "value2", 2),
    ];
    let left = create_sstable_data(left_data);

    let right_data = vec![
        ("key1", "value3", 3),
        ("key4", "value4", 4),
    ];
    let right = create_sstable_data(right_data);

    let expected = vec![
        ("key1", "value3", 3),
        ("key2", "value2", 2),
        ("key4", "value4", 4),
    ];
    let expected = create_sstable_data(expected);
    let size_tiered_compaction = super::SizeTieredCompaction::new();
    let merged = size_tiered_compaction.merge(&left, &right);
    assert_eq!(merged, expected);
}

