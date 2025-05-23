use std::{collections::BTreeMap, fs, path, sync::Arc};

use libc::sleep;

use crate::{SharedSSTableReader, memtable::MemTable, sstable::{compaction::Compaction, reader::SSTableReaderManager, SSTableData, SSTableWriter}, utils::get_page_size};

use super::SizeTieredCompaction;

fn create_sstable_data(data: Vec<(&str, &str, u64)>) -> SSTableData {
    let mut memtable = MemTable::new();
    for (key, value, timestamp) in data.iter() {
        memtable.put(key, value, *timestamp);
    }
    SSTableData::from(memtable)
}

#[test]
fn test_merge_impl_simple() {
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
    let size_tiered_compaction = super::SizeTieredCompaction::new(
        get_page_size(),
        None,
        None,
        None,
    );
    let merged = size_tiered_compaction.merge_impl(&left, &right);
    assert_eq!(merged, expected);
}

#[test]
fn test_merge_impl_duplicated() {
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
    let size_tiered_compaction = super::SizeTieredCompaction::new(
        get_page_size(),
        None,
        None,
        None,
    );
    let merged = size_tiered_compaction.merge_impl(&left, &right);
    assert_eq!(merged, expected);
}

#[test]
fn test_merge_impl_interleaved() {
    let left_data = vec![
        ("key1", "value1", 1),
        ("key3", "value3", 3),
    ];
    let left = create_sstable_data(left_data);

    let right_data = vec![
        ("key2", "value2", 2),
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
    let size_tiered_compaction = super::SizeTieredCompaction::new(
        get_page_size(),
        None,
        None,
        None,
    );
    let merged = size_tiered_compaction.merge_impl(&left, &right);
    assert_eq!(merged, expected);
}

#[test]
fn test_merge() {
    let data1 = vec![
        ("key1", "value1", 1),
        ("key2", "value10", 10),
    ];

    let data2 = vec![
        ("key3", "value9", 9),
        ("key4", "value3", 3),
    ];

    let data3 = vec![
        ("key1", "value4", 4),
        ("key2", "value5", 5),
        ("key3", "value6", 6),
        ("key4", "value7", 7),
    ];

    let vec = vec![
        create_sstable_data(data1),
        create_sstable_data(data2),
        create_sstable_data(data3),
    ];

    let expected = vec![
        ("key1", "value4", 4),
        ("key2", "value10", 10),
        ("key3", "value9", 9),
        ("key4", "value7", 7),
    ];
    let expected = create_sstable_data(expected);

    let size_tiered_compaction = super::SizeTieredCompaction::new(
        get_page_size(),
        None,
        None,
        None,
    );
    let merged = size_tiered_compaction.merge(vec);
    assert_eq!(merged, expected);
}

#[test]
fn test_get_interesting_bucket_with_med() {
    let ssts = SizeTieredCompaction::new(
        get_page_size(),
        Some(0.5),
        Some(1.5),
        None,
    );

    let file_1 = "get_interesting_bucket_med1";
    let file_2 = "get_interesting_bucket_med2";
    let file_3 = "get_interesting_bucket_med3";
    let file_4 = "get_interesting_bucket_med4";
    let file_5 = "get_interesting_bucket_med5";
    let file_6 = "get_interesting_bucket_med6";
    let file_7 = "get_interesting_bucket_med7";
    let file_8 = "get_interesting_bucket_med8";

    fs::write(file_1, "1".repeat(10)).unwrap();
    fs::write(file_2, "1".repeat(11)).unwrap();

    fs::write(file_3, "1".repeat(20)).unwrap();
    fs::write(file_4, "1".repeat(23)).unwrap();
    fs::write(file_5, "1".repeat(27)).unwrap();
    fs::write(file_6, "1".repeat(30)).unwrap();
    
    fs::write(file_7, "1".repeat(100)).unwrap();
    fs::write(file_8, "1".repeat(110)).unwrap();

    let vec = vec![
        file_1,
        file_2,
        file_3,
        file_4,
        file_5,
        file_6,
        file_7,
        file_8,
    ];

    vec.iter().for_each(|v| {
        fs::File::create(&(v.to_string() + ".idx")).unwrap();
    });

    let sstables = vec.iter().map(|v| {
        Arc::new(SSTableReaderManager::new(
        v,
        &(v.to_string() + ".idx")
        ).unwrap())
    }).collect::<Vec<Arc<SSTableReaderManager>>>();


    let actual = ssts.get_interesting_bucket(&sstables);
    for i in 0..actual.len() {
        assert_eq!(
            actual[i].reader(),
            sstables[i + 2].reader(),
        );
    }

    sstables.iter().for_each(|sstable| {
        fs::remove_file(&sstable.file()).unwrap();
        fs::remove_file(&sstable.reader().index_file).unwrap();
    });
}

#[test]
fn test_get_interesting_bucket_with_min() {
    let ssts = SizeTieredCompaction::new(
        get_page_size(),
        Some(0.5),
        Some(1.5),
        None,
    );

    let file_1 = "get_interestring_bucket_min1";
    let file_2 = "get_interestring_bucket_min2";
    let file_3 = "get_interestring_bucket_min3";
    let file_4 = "get_interestring_bucket_min4";
    let file_5 = "get_interestring_bucket_min5";
    let file_6 = "get_interestring_bucket_min6";
    let file_7 = "get_interestring_bucket_min7";
    let file_8 = "get_interestring_bucket_min8";

    fs::write(file_1, "1".repeat(10)).unwrap();
    fs::write(file_2, "1".repeat(11)).unwrap();
    fs::write(file_3, "1".repeat(12)).unwrap();
    fs::write(file_4, "1".repeat(13)).unwrap();

    fs::write(file_5, "1".repeat(27)).unwrap();
    fs::write(file_6, "1".repeat(30)).unwrap();
    
    fs::write(file_7, "1".repeat(100)).unwrap();
    fs::write(file_8, "1".repeat(110)).unwrap();

    let vec = vec![
        file_1,
        file_2,
        file_3,
        file_4,
        file_5,
        file_6,
        file_7,
        file_8,
    ];

    vec.iter().for_each(|v| {
        fs::File::create(&(v.to_string() + ".idx")).unwrap();
    });

    // let sstables = vec.iter().map(|v| {
    //     SSTableReader::new(
    //         v,
    //         &(v.to_string() + ".idx")
    //     ).unwrap()
    // }).collect::<Vec<SSTableReader>>();
    let sstables = vec.iter().map(|v| {
        Arc::new(SSTableReaderManager::new(
        v,
        &(v.to_string() + ".idx")
        ).unwrap())
    }).collect::<Vec<Arc<SSTableReaderManager>>>();

    let actual = ssts.get_interesting_bucket(&sstables);
    for i in 0..actual.len() {
        assert_eq!(
            actual[i].reader(),
            sstables[i].reader(),
        );
    }
    // let actual = ssts.get_interesting_bucket(&sstables);
    // assert_eq!(&actual, &sstables[0..4]);

    sstables.iter().for_each(|sstable| {
        fs::remove_file(&sstable.file()).unwrap();
        fs::remove_file(&sstable.reader().index_file).unwrap();
    });
}

#[test]
fn test_get_interesting_bucket_with_max() {
    let ssts = SizeTieredCompaction::new(
        get_page_size(),
        Some(0.5),
        Some(1.5),
        None,
    );

    let file_1 = "get_interestring_bucket_max1";
    let file_2 = "get_interestring_bucket_max2";
    let file_3 = "get_interestring_bucket_max3";
    let file_4 = "get_interestring_bucket_max4";
    let file_5 = "get_interestring_bucket_max5";
    let file_6 = "get_interestring_bucket_max6";
    let file_7 = "get_interestring_bucket_max7";
    let file_8 = "get_interestring_bucket_max8";

    fs::write(file_1, "1".repeat(10)).unwrap();
    fs::write(file_2, "1".repeat(11)).unwrap();

    fs::write(file_3, "1".repeat(50)).unwrap();
    fs::write(file_4, "1".repeat(60)).unwrap();

    fs::write(file_5, "1".repeat(100)).unwrap();
    fs::write(file_6, "1".repeat(120)).unwrap();
    fs::write(file_7, "1".repeat(100)).unwrap();
    fs::write(file_8, "1".repeat(110)).unwrap();

    let vec = vec![
        file_1,
        file_2,
        file_3,
        file_4,
        file_5,
        file_6,
        file_7,
        file_8,
    ];

    vec.iter().for_each(|v| {
        fs::File::create(&(v.to_string() + ".idx")).unwrap();
    });


    let sstables = vec.iter().map(|v| {
        Arc::new(SSTableReaderManager::new(
        v,
        &(v.to_string() + ".idx")
        ).unwrap())
    }).collect::<Vec<Arc<SSTableReaderManager>>>();

    let actual = ssts.get_interesting_bucket(&sstables);
    for i in 0..actual.len() {
        assert_eq!(
            actual[i].reader(),
            sstables[i + 4].reader(),
        );
    }

    sstables.iter().for_each(|sstable| {
        fs::remove_file(&sstable.file()).unwrap();
        fs::remove_file(&sstable.reader().index_file).unwrap();
    });
}

#[test]
fn test_window_usage() {
    let vec = vec![1, 2, 3, 4, 5];
    for v in vec.windows(2) {
        println!("{:?}", v);
    }
}

#[test]
fn test_chunk_usage() {
    let vec = vec![1, 2, 3, 4, 5];
    for v in vec.chunks(2) {
        println!("{:?}", v);
    }
}

#[test]
fn test_compact_simple() {
    let path = ".test_compact_test_simple";
    let data = vec![
        ("key1", "value1", 1),
        ("key2", "value2", 2),
        ("key3", "value3", 3),
        ("key4", "value4", 4),
        ("key3", "value5", 5),
        ("key5", "value6", 6),
        ("key6", "value7", 7),
        ("key3", "value8", 8),
        ("key7", "value9", 9),
        ("key8", "value10", 10),
        ("key9", "value11", 11),
        ("key3", "value12", 12),
        ("key10", "value13", 13),
    ];
    if path::Path::new(path).exists() {
        fs::remove_dir_all(path).unwrap();
    }
    fs::create_dir(&path).unwrap();

    let writer = SSTableWriter::new(&path).unwrap(); 
    let sstable_data = create_sstable_data(data[0..3].to_vec());
    writer.write_with_index(&sstable_data, 34).unwrap();

    unsafe { sleep(1); }

    let writer = SSTableWriter::new(&path).unwrap(); 
    let sstable_data2 = create_sstable_data(data[3..6].to_vec());
    writer.write_with_index(&sstable_data2, 34).unwrap();

    unsafe { sleep(1); }

    let writer = SSTableWriter::new(&path).unwrap(); 
    let sstable_data3 = create_sstable_data(data[6..9].to_vec());
    writer.write_with_index(&sstable_data3, 34).unwrap();

    unsafe { sleep(1); }

    let writer = SSTableWriter::new(&path).unwrap(); 
    let sstable_data4 = create_sstable_data(data[9..12].to_vec());
    writer.write_with_index(&sstable_data4, 34).unwrap();

    unsafe { sleep(1); }

    let writer = SSTableWriter::new(&path).unwrap(); 
    let sstable_data5 = create_sstable_data(data[12..].to_vec());
    writer.write_with_index(&sstable_data5, 34).unwrap();

    let tables = fs::read_dir(&path).unwrap();
    let shared_sstable = SharedSSTableReader::new(
        path,
        "idx",
    );

    for table in tables {
        let table = table.unwrap();
        let path = table.path();
        if path.is_file() && path.extension().unwrap() == "sst" {
            shared_sstable.add_reader(path.to_str().unwrap());
        }
    }

    let size_tiered_compaction = super::SizeTieredCompaction::new(
        get_page_size(),
        Some(0.5),
        Some(1.5),
        Some(4)
    );

    let writer = SSTableWriter::new(&path).unwrap();
    assert!(size_tiered_compaction.compact(
        shared_sstable.clone(),
        writer
    ).is_ok());

    let tables = fs::read_dir(&path).unwrap().filter(|v| {
        v.as_ref().unwrap().path().extension().unwrap() == "sst"
    })
    .map(|v| v.unwrap())
    .collect::<Vec<_>>();

    let compacted = tables.iter().max_by(|a, b| {
        a.path().cmp(&b.path())
    }).unwrap();

    let mut unique = BTreeMap::new();
    for (k, v, ts) in data[0..12].iter() {
        if let Some((prev_v, prev_ts)) = unique.get_mut(*k) {
            if *ts > *prev_ts {
                *prev_v = *v;
                *prev_ts = *ts;
            }
        } else {
            unique.insert(*k, (*v, *ts));
        }
    }
    assert_eq!(
        compacted.metadata().unwrap().len(),
        unique.iter().fold(0, 
            |acc, (k, (v, _))| acc + k.len() + v.len() + 8 * 3
        ) as u64 
    );

    let files = fs::read_dir(&path).unwrap();
    for file in files {
        let file = file.unwrap().path();
        shared_sstable.drop_resource(file.to_str().unwrap());
    }

    let tables = fs::read_dir(&path).unwrap();
    assert_eq!(
        tables.filter(|v| v.as_ref().unwrap().path().extension().unwrap() == "sst").count(), 
        2
    );
    fs::remove_dir_all(&path).unwrap();
}