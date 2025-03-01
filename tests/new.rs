use std::fs::remove_dir_all;

use lsmtree::{sstable::compaction::size_tiered_compaction::SizeTieredCompaction, utils::get_page_size, LSMTree, LSMTreeConf};

#[test]
fn test_new() {
    let conf = LSMTreeConf::new(
        SizeTieredCompaction::new(),
        None,
        None,
        None,
    );
    let lsm_tree = LSMTree::new(conf).unwrap();
    assert_eq!(lsm_tree.get_memtable_threshold(), get_page_size());
    assert_eq!(lsm_tree.get_sst_dir(), "./.sst");

    remove_dir_all("./.sst").unwrap();
    remove_dir_all("./.commitlog").unwrap();
}