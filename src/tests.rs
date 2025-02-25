use crate::{LSMTree, MemTable, CommitLog, SSTable, SizeTieredCompaction};

#[test]
fn test_lsm_tree() {
    let lsm_tree = LSMTree::new(SizeTieredCompaction::new());
}