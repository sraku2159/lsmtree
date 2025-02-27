// use lsmtree::{LSMTree, LSMTreeConf, sstable::SizeTieredCompaction};

// #[test]
// fn test_get() {
//     let data = [("key1", "value1"), ("key2", "value2"), ("key3", "value3")];
//     let mut lsm_tree = LSMTree::new(
//         LSMTreeConf::new(
//             SizeTieredCompaction::new(),
//             None,
//             None,
//             None,
//     )).unwrap();
//     for (key, value) in data.iter() {
//         assert_eq!(lsm_tree.put(*key, *value), None);
//     }
//     for (key, value) in data.iter() {
//         assert_eq!(lsm_tree.get(*key), Some(value.to_string()));
//     }
// }