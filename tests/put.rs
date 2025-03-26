use std::fs::{self, read_dir};

use lsmtree::{sstable::compaction::{leveled_compaction::LeveledCompaction, size_tiered_compaction::SizeTieredCompaction, Compaction}, LSMTree, LSMTreeConf};

pub struct MockCompaction {}

impl Compaction for MockCompaction {
    fn compact(&self, _sstables: Vec<lsmtree::sstable::SSTableReader>) {
        unimplemented!("MockCompaction::compact is not implemented");
    }

    fn get_target_dir(&self) -> String {
        unimplemented!("MockCompaction::get_target_dir is not implemented");
    }

    fn get_sstables(&self, dir: &String) -> Vec<lsmtree::sstable::SSTableReader> {
        let mut sstables = Vec::new();
        let files = fs::read_dir(dir).unwrap();
        for file in files {
            let file = file.unwrap();
            let path = file.path();
            let sstable = lsmtree::sstable::SSTableReader::new(path.to_str().unwrap()).unwrap();
            sstables.push(sstable);
        }
        sstables
    }
}

fn tear_down(sst_dir: &str, commitlog_dir: &str) {
    std::fs::remove_dir_all(sst_dir).unwrap();
    std::fs::remove_dir_all(commitlog_dir).unwrap();
}

#[test]
fn test_put_big_quantity() {
    let sst_dir = "./.test_put_big_quantity_sst";
    let commitlog_dir = "./.test_put_big_quantity_commitlog";
    let mut lsm_tree = LSMTree::new(
        LSMTreeConf::new(
            MockCompaction{},
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
    let read_dir = read_dir(sst_dir).unwrap();
    read_dir.for_each(|entry| {
        let entry = entry.unwrap();
        assert!(entry.metadata().unwrap().len() > lsm_tree.get_memtable_threshold() as u64);
    });
    tear_down(&sst_dir, &commitlog_dir);
}
