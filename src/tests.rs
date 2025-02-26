use crate::{CommitLog, LSMTree, LSMTreeConf, LevelCompaction, MemTable, SSTable, SizeTieredCompaction};

#[test]
fn test_put() {
    let data = [("key1", "value1"), ("key2", "value2"), ("key3", "value3")];
    let mut lsm_tree = LSMTree::new(LSMTreeConf::new(
        SizeTieredCompaction::new(),
        None,
    ));
    for (key, value) in data.iter() {
        assert_eq!(lsm_tree.put(*key, *value), None);
    }
    for (key, value) in data.iter() {
        assert_eq!(lsm_tree.put(*key, *value), Some(value.to_string()));
    }
}

#[test]
fn test_put_big_quantity() {
    let data = (0..usize::MAX).collect::<Vec<_>>().into_iter()
        .map(|i| (format!("key{}", i), format!("value{}", i)))
        .collect::<Vec<(String, String)>>();

    let mut lsm_tree = LSMTree::new(LSMTreeConf::new(
        SizeTieredCompaction::new(),
        None,
    ));
    for (key, value) in data.iter() {
        assert_eq!(lsm_tree.put(key, value), None);
    }
    for (key, value) in data.iter() {
        assert_eq!(lsm_tree.put(key, value), Some(value.to_string()));
    }
}

#[test]
fn test_get() {
    let data = [("key1", "value1"), ("key2", "value2"), ("key3", "value3")];
    let mut lsm_tree = LSMTree::new(LSMTreeConf::new(
        SizeTieredCompaction::new(),
        None,
    ));
    for (key, value) in data.iter() {
        assert_eq!(lsm_tree.put(*key, *value), None);
    }
    for (key, value) in data.iter() {
        assert_eq!(lsm_tree.get(*key), Some(value.to_string()));
    }
}