pub mod memtable;
pub mod commitlog;
pub mod sstable;
pub mod utils;

use std::{fmt::Debug, thread::spawn};

use memtable::MemTable;
use commitlog::CommitLog;
use sstable::{SSTable, compaction::Compaction};

use utils::*;

use std::io::ErrorKind;

// 今後、もしmemtableやsstableで異なるデータ型を使用する場合、この型をアダプターとして差分を吸収する
pub type Key = String;
pub type Value = String;

#[derive(Debug)]
pub struct LSMTreeConf<T>
where
    T: Compaction + Debug + Send,
{
    compaction: T,
    sst_dir: String,
    commitlog_dir: String,
    memtable_threshold: usize,
}

impl<T: Compaction + Debug + Send> LSMTreeConf<T> {
    pub fn new(
        compaction: T,
        sst_dir: Option<String>,
        commitlog_dir: Option<String>,
        memtable_threshold: Option<usize>, 
        ) -> LSMTreeConf<T>
    {
        let sst_dir = sst_dir.unwrap_or("./.sst".to_string());
        let commitlog_dir = commitlog_dir.unwrap_or("./.commitlog".to_string());
        let memtable_threshold = memtable_threshold.unwrap_or(get_page_size());
        LSMTreeConf {
            compaction,
            sst_dir,
            commitlog_dir,
            memtable_threshold,
        }
    }
}

#[derive(Debug)]
pub struct LSMTree<T>
where
    T: Compaction + Debug + Send
{
    memtable: MemTable,
    memtable_threshold: usize,
    commitlog: CommitLog,
    sst_dir: String,
    compaction: T,
}

impl<T: Compaction + Debug + Send> LSMTree<T> {
    pub fn new(conf: LSMTreeConf<T>) -> Result<LSMTree<T>, String> {
        Self::create_dir(&conf.sst_dir)?;
        Self::create_dir(&conf.commitlog_dir)?;

        Ok(LSMTree {
            memtable: MemTable::new(),
            memtable_threshold: conf.memtable_threshold,
            commitlog: CommitLog::new(&conf.commitlog_dir)?,
            sst_dir: conf.sst_dir,
            compaction: conf.compaction,
        })
    }

    fn create_dir(path: &str) -> Result<(), String> {
        match std::fs::metadata(path).map(|m| m.is_dir()){
            Ok(false) => {
                println!("Create dir for SSTable: {:?}", path);
                utils::create_dir(path)
            },
            Err(e) if e.kind() == ErrorKind::NotFound => {
                println!("Create dir for SSTable: {:?}", path);
                utils::create_dir(path)
            },
            Ok(true) => {
                println!("Already exists");
                Ok(())
            },
            Err(e) => Err(e.to_string()),
        }
    }

    pub fn put(&mut self, key: &str, value: &str) -> Result<Option<Value>, String> {
        let ret = self.memtable.put(key, value);
        self.commitlog.write_put(key, value);
        if self.memtable.len() >= self.memtable_threshold {    
            // 大きいデータ構造なのでディープコピーの場合、パフォーマンスが悪い
            // ただ、現在の実装だと借用状態なので、ムーブができない
            let memtable = self.memtable.clone();              
            let dir = self.sst_dir.clone();
            let commitlog = self.commitlog.try_clone()?;
            self.memtable = MemTable::new();
            self.commitlog = CommitLog::new(self.commitlog.get_dir())?;
            spawn( move || {
                let ret = Self::flush_memtable(&dir, memtable);
                match ret {
                    Ok(_) => {
                        println!("Flushed memtable");
                        match commitlog.delete_log() {
                            Err(e) => eprintln!("ERROR: delete {} Error because of: {}", commitlog.get_file_path(), e),
                            _ => println!("INFO: {} is deleted", commitlog.get_file_path()),
                        }
                    },
                    Err(e) => eprintln!("ERROR: flush_memtable Error because of: {}", e),
                }
            });
        }
        Ok(ret) 
    }

    fn flush_memtable(dir: &str, memtable: MemTable) -> Result<(), String> {
        let sstable = SSTable::new(dir)?;
        sstable.write(&memtable)?;
        Ok(())
    }

    pub fn get(&self, key: &str) -> Option<Value> {
        self.memtable.get(key)
    }

    pub fn get_memtable(&self) -> &MemTable {
        &self.memtable
    }

    pub fn get_sst_dir(&self) -> &str {
        &self.sst_dir
    }

    pub fn get_commitlog(&self) -> &CommitLog {
        &self.commitlog
    }

    pub fn get_memtable_threshold(&self) -> usize {
        self.memtable_threshold
    }
}