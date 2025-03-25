use lsmtree::{sstable::compaction::{leveled_compaction::LeveledCompaction, size_tiered_compaction::SizeTieredCompaction}, LSMTree, LSMTreeConf};

fn tear_down(sst_dir: &str, commitlog_dir: &str) {
    std::fs::remove_dir_all(sst_dir).unwrap();
    std::fs::remove_dir_all(commitlog_dir).unwrap();
}

#[test]
fn test_get_with_size_tiered() {
    let data = [("key1", "value1"), ("key2", "value2"), ("key3", "value3")];
    let sst_dir = "./.test_get_with_size_tiered_sst";
    let commitlog_dir = "./.test_get_with_size_tiered_commitlog";
    let mut lsm_tree = LSMTree::new(
        LSMTreeConf::new(
            SizeTieredCompaction::new(),
            Some(sst_dir.to_owned()),
            Some(commitlog_dir.to_owned()),
            None,
    )).unwrap();
    for (key, value) in data.iter() {
        assert_eq!(lsm_tree.put(*key, *value).unwrap(), None);
    }
    for (key, value) in data.iter() {
        assert_eq!(lsm_tree.get(*key), Ok(Some(value.to_string())));
    }
    tear_down(sst_dir, commitlog_dir);
}

#[test]
fn test_get_with_size_leveled() {
    let data = [("key1", "value1"), ("key2", "value2"), ("key3", "value3")];
    let sst_dir = "./.test_get_with_leveled_sst";
    let commitlog_dir = "./.test_get_with_leveled_commitlog";
    let mut lsm_tree = LSMTree::new(
        LSMTreeConf::new(
            LeveledCompaction::new(),
            Some(sst_dir.to_owned()),
            Some(commitlog_dir.to_owned()),
            None,
    )).unwrap();
    for (key, value) in data.iter() {
        assert_eq!(lsm_tree.put(*key, *value).unwrap(), None);
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
    let mut lsm_tree = LSMTree::new(
        LSMTreeConf::new(
            SizeTieredCompaction::new(),
            Some(sst_dir.to_owned()),
            Some(commitlog_dir.to_owned()),
            None,
    )).unwrap();
    /*
        * 大体1MBのデータを入れる
        * 1MB = 1024KB = 1024 * 1024B = 1048576B
        * 1key ≒ 4B, 1value ≒ 6B
        * 1entry ≒ 10B
        * 1048576B / 10B ≒ 104857
     */
    for i in 0..104857 {
        assert_eq!(lsm_tree.put(&format!("key{}", i), &format!("value{}", i)).unwrap(), None);
    }
    for i in 0..104857 {
        assert_eq!(lsm_tree.get(&format!("key{}", i)), Ok(Some(format!("value{}", i))));
    }
    tear_down(sst_dir, commitlog_dir);
}