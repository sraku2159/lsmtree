use lsmtree::{sstable::compaction::{leveled_compaction::LeveledCompaction, size_tiered_compaction::SizeTieredCompaction}, LSMTree, LSMTreeConf};

#[test]
fn test_put_with_size_tiered() {
    let data = [("key1", "value1"), ("key2", "value2"), ("key3", "value3")];
    let mut lsm_tree = LSMTree::new(
        LSMTreeConf::new(
            SizeTieredCompaction::new(),
            None,
            None,
            None,
    )).unwrap();
    for (key, value) in data.iter() {
        assert_eq!(lsm_tree.put(*key, *value), None);
    }
    for (key, value) in data.iter() {
        assert_eq!(lsm_tree.put(*key, *value), Some(value.to_string()));
    }
}

#[test]
fn test_put_with_leveled() {
    let data = [("key1", "value1"), ("key2", "value2"), ("key3", "value3")];
    let mut lsm_tree = LSMTree::new(
        LSMTreeConf::new(
            LeveledCompaction::new(),
            None,
            None,
            None,
    )).unwrap();
    for (key, value) in data.iter() {
        assert_eq!(lsm_tree.put(*key, *value), None);
    }
    for (key, value) in data.iter() {
        assert_eq!(lsm_tree.put(*key, *value), Some(value.to_string()));
    }
}

// #[test]
// fn test_put_big_quantity() {
//     let mut lsm_tree = LSMTree::new(
//         LSMTreeConf::new(
//             SizeTieredCompaction::new(),
//             None,
//             None,
//             None,
//     )).unwrap();
//     for i in 0..usize::MAX {
//         assert_eq!(lsm_tree.put(&format!("key{}", i), &format!("value{}", i)), None);
//     }
//     for i in 0..usize::MAX {
//         assert_eq!(lsm_tree.get(&format!("key{}", i)), Some(format!("value{}", i)));
//     }
// }
