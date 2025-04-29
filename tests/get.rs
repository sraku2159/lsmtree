use std::{fs, sync::Arc};

use lsmtree::{sstable::{compaction::{size_tiered_compaction::SizeTieredCompaction, Compaction}, SSTableWriter}, utils::get_page_size, LSMTree, LSMTreeConf, SharedSSTableReader};

#[derive(Debug, Clone)]
pub struct MockCompaction {}

impl Compaction for MockCompaction {
    fn compact(
        &self, 
        sstables: Arc<SharedSSTableReader>, 
        writer: SSTableWriter) -> Result<(), String> {
        let _ = writer;
        let _ = sstables;
        unimplemented!("MockCompaction::compact is not implemented");
    }
}

struct MockTimeStampGenerator {
    monotonic: u64,
}

impl MockTimeStampGenerator {
    pub fn new() -> Self {
        MockTimeStampGenerator { monotonic: 0 }
    }
}

impl lsmtree::TimeStampGenerator for MockTimeStampGenerator {
    fn get_timestamp(&mut self) -> u64 {
        self.monotonic += 1;
        self.monotonic
    }
}

fn tear_down(sst_dir: &str, commitlog_dir: &str) {
    std::fs::remove_dir_all(sst_dir).unwrap();
    std::fs::remove_dir_all(commitlog_dir).unwrap();
}

#[test]
fn test_get_with_size_tiered() {
    let data = [("key1", "value1"), ("key2", "value2"), ("key3", "value3")];
    let sst_dir = "./.test_get_with_size_tiered_sst";
    let commitlog_dir = "./.test_get_with_size_tiered_commitlog";
    let index_interval = get_page_size();
    let mut lsm_tree = LSMTree::new(
        LSMTreeConf::new(
            SizeTieredCompaction::new(
                get_page_size(),
                Some(0.5),
                Some(1.5),
                Some(4),
            ),
            MockTimeStampGenerator::new(),
            Some(sst_dir.to_owned()),
            Some(commitlog_dir.to_owned()),
            None,
            Some(index_interval),
            Some("idx".to_owned()),
            Some(true),
    )).unwrap();
    for (key, value) in data.iter() {
        assert!(lsm_tree.put(*key, Some(*value)).is_ok());
    }
    for (key, value) in data.iter() {
        assert_eq!(lsm_tree.get(*key), Ok(Some(value.to_string())));
    }
    tear_down(sst_dir, commitlog_dir);
}


#[test]
fn test_get_big_quantity() {
    let sst_dir = "./.test_get_big_quantity_sst";
    let commitlog_dir = "./.test_get_big_quantity_commitlog";
    let index_interval = get_page_size();

    if fs::exists(sst_dir).unwrap() {
        fs::remove_dir_all(sst_dir).unwrap();
    }
    if fs::exists(commitlog_dir).unwrap() {
        fs::remove_dir_all(commitlog_dir).unwrap();
    }

    let mut lsm_tree = LSMTree::new(
        LSMTreeConf::new(
            MockCompaction {},
            MockTimeStampGenerator::new(),
            Some(sst_dir.to_owned()),
            Some(commitlog_dir.to_owned()),
            None,
            Some(index_interval),
            Some("idx".to_owned()),
            Some(false),       // コンパクションを無効化
    )).unwrap();
    for i in 0..104857 {
        assert!(lsm_tree.put(&format!("key{}", i), Some(&format!("value{}", i))).is_ok());
    }
    let now = std::time::Instant::now();
    assert_eq!(lsm_tree.get("not_exist_key"), Ok(None));
    for i in 0..104857 {
        assert_eq!(lsm_tree.get(&format!("key{}", i)), Ok(Some(format!("value{}", i))));
    }
    println!("Elapsed time: {:?}", now.elapsed());
    tear_down(sst_dir, commitlog_dir);
}

#[test]
fn test_get_mid_quantity_with_ssts() {
    let sst_dir = "./.test_get_mid_quantity_sst_with_ssts";
    let commitlog_dir = "./.test_get_mid_quantity_with_ssts_commitlog";
    let index_interval = get_page_size();

    if fs::exists(sst_dir).unwrap() {
        fs::remove_dir_all(sst_dir).unwrap();
    }
    if fs::exists(commitlog_dir).unwrap() {
        fs::remove_dir_all(commitlog_dir).unwrap();
    }

    let mut lsm_tree = LSMTree::new(
        LSMTreeConf::new(
            SizeTieredCompaction::new(
                get_page_size(),
                Some(0.5),
                Some(1.5),
                Some(4),
            ),
            MockTimeStampGenerator::new(),
            Some(sst_dir.to_owned()),
            Some(commitlog_dir.to_owned()),
            None,
            Some(index_interval),
            Some("idx".to_owned()),
            Some(true),
    )).unwrap();

    for i in 0..48000 {
        assert!(lsm_tree.put(&format!("key{}", i), Some(&format!("value{}", i))).is_ok());
    }
    let _ = lsm_tree.put("key1", None);
    for i in 0..48000 {
        assert!(lsm_tree.put(&format!("key{}", i), Some(&format!("value{}", i + 48000))).is_ok());
    }
    let _ = lsm_tree.put("key2", None);
    let _ = lsm_tree.put("key12000", None);
    let _ = lsm_tree.put("key48000", None);
    let now = std::time::Instant::now();
    assert_eq!(lsm_tree.get("not_exist_key"), Ok(None));
    for i in 0..=48000 {
        if i == 2 || i == 12000 || i == 48000 {
            assert_eq!(lsm_tree.get(&format!("key{}", i)), Ok(None));
        } else {
            assert_eq!(lsm_tree.get(&format!("key{}", i)), Ok(Some(format!("value{}", i + 48000))));
        }
    }
    println!("Elapsed time: {:?}", now.elapsed());
    tear_down(sst_dir, commitlog_dir);
}
