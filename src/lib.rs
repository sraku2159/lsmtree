pub mod memtable;
pub mod commitlog;
pub mod sstable;
pub mod utils;

use std::{fmt::Debug, thread::spawn};

use memtable::MemTable;
use commitlog::CommitLog;
use sstable::{compaction::Compaction, SSTableReader, SSTableWriter};

use utils::*;

use std::io::ErrorKind;

// 今後、もしmemtableやsstableで異なるデータ型を使用する場合、この型をアダプターとして差分を吸収する
pub type Key = String;
pub type Value = String;

#[derive(Debug)]
pub struct LSMTreeConf<T, U = DefaultTimeStampGenerator>
where
    T: Compaction,
    U: TimeStampGenerator,
{
    compaction: T,
    timestamp_generator: U,
    sst_dir: String,
    commitlog_dir: String,
    memtable_threshold: usize,
    index_interval: usize,
    index_file_suffix: String,
}

impl<T: Compaction, U: TimeStampGenerator> LSMTreeConf<T, U> {
    pub fn new(
        compaction: T,
        timestamp_generator: U,
        sst_dir: Option<String>,
        commitlog_dir: Option<String>,
        memtable_threshold: Option<usize>, 
        index_interval: Option<usize>,
        index_file_suffix: Option<String>,
        ) -> LSMTreeConf<T, U>
    {
        let sst_dir = sst_dir.unwrap_or("./.sst".to_string());
        let commitlog_dir = commitlog_dir.unwrap_or("./.commitlog".to_string());
        let memtable_threshold = memtable_threshold.unwrap_or(get_page_size());
        let index_interval = index_interval.unwrap_or(get_page_size());
        let index_file_suffix = index_file_suffix.unwrap_or("idx".to_string());
        LSMTreeConf {
            compaction,
            timestamp_generator,
            sst_dir,
            commitlog_dir,
            memtable_threshold,
            index_interval,
            index_file_suffix,
        }
    }
}

#[derive(Debug)]
pub struct LSMTree<T, U = DefaultTimeStampGenerator>
where
    T: Compaction,
    U: TimeStampGenerator,
{
    memtable: MemTable,
    memtable_threshold: usize,
    index_interval: usize,
    index_file_suffix: String,
    commitlog: CommitLog,
    sst_dir: String,
    compaction: T,
    timestamp_generator: U,
}

impl<T: Compaction, U: TimeStampGenerator> LSMTree<T, U> {
    pub fn new(conf: LSMTreeConf<T, U>) -> Result<LSMTree<T, U>, String> {
        Self::create_dir(&conf.sst_dir)?;
        Self::create_dir(&conf.commitlog_dir)?;

        Ok(LSMTree {
            memtable: MemTable::new(),
            memtable_threshold: conf.memtable_threshold,
            index_interval: conf.index_interval,
            index_file_suffix: conf.index_file_suffix,
            commitlog: CommitLog::new(&conf.commitlog_dir)?,
            sst_dir: conf.sst_dir,
            compaction: conf.compaction,
            timestamp_generator: conf.timestamp_generator,
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
        let timestamp = self.timestamp_generator.get_timestamp();
        let ret = self.memtable.put(key, value, timestamp);
        self.commitlog.write_put(key, value, timestamp);
        if self.memtable.len() >= self.memtable_threshold {    
            // 大きいデータ構造なのでディープコピーの場合、パフォーマンスが悪い
            // ただ、現在の実装だと借用状態なので、ムーブができない
            let memtable = self.memtable.clone();              
            let dir = self.sst_dir.clone();
            let commitlog = self.commitlog.try_clone()?;
            let index_interval = self.index_interval;
            self.memtable = MemTable::new();
            self.commitlog = CommitLog::new(self.commitlog.get_dir())?;
            spawn( move || {
                let ret = Self::flush_memtable(&dir, memtable, index_interval);
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
        Ok(ret.map(|v| v.to_string()))
    }

    fn flush_memtable(dir: &str, memtable: MemTable, index_interval: usize) -> Result<(), String> {
        let sstable = SSTableWriter::new(dir)?;
        sstable.write(&memtable, index_interval)?;
        Ok(())
    }

    pub fn get(&self, key: &str) -> Result<Option<Value>, String> {
        match self.memtable.get(key) {
            Some(value) => {
                match value {
                    memtable::Value::Data(value, _) => Ok(Some(value.to_string())),
                    memtable::Value::Tombstone(_) => Ok(None),
                }
            },
            None => {
                self.get_from_sstable(key)
            }
        }
    }

    fn get_from_sstable(&self, key: &str) -> Result<Option<Value>, String> {
        let mut candidate = vec![];
        for reader in self.reader_iter() {
            match reader.read(key) {
                Ok(None) => continue,
                Ok(value) => {
                    candidate.push(value.unwrap());
                },
                Err(e) => return Err(e),
            }
        }
        if candidate.is_empty() {
            return Ok(None);
        }
        candidate.sort_by(|a, b| {
            let a = a.1;
            let b = b.1;
            if a == b {
                return std::cmp::Ordering::Equal;
            }
            if a > b {
                return std::cmp::Ordering::Greater;
            }
            std::cmp::Ordering::Less
        });
        Ok(candidate.last().unwrap().0.clone())
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

    fn reader_iter(&self) -> SSTableReaderIter {
        SSTableReaderIter::new(
            &self.sst_dir, 
            &self.index_file_suffix,
        )
    }
}

pub trait TimeStampGenerator {
    fn get_timestamp(&self) -> u64;
}

pub struct DefaultTimeStampGenerator {}
impl TimeStampGenerator for DefaultTimeStampGenerator {
    fn get_timestamp(&self) -> u64 {
        utils::get_timestamp()
    }
}

struct SSTableReaderIter<'a, 'b> {
    root_dir: &'a String,
    idx_file_suffix: &'b String,
    sstables: Vec<SSTableReader>,
    index: usize,
}

impl<'a, 'b> SSTableReaderIter<'a, 'b> {
    fn new(root_dir: &'a String, idx_file_suffix: &'b String) -> SSTableReaderIter<'a, 'b> {
        let sstables = Self::get_sstables(root_dir, idx_file_suffix);
        SSTableReaderIter {
            sstables,
            index: 0,
            root_dir,
            idx_file_suffix,
        }
    }

    fn get_sstables(root_dir: &'a String, idx_file_suffix: &'b String) -> Vec<SSTableReader> {
        let mut sstables = Vec::new();
        let dir = std::fs::read_dir(root_dir).unwrap();
        for entry in dir {
            let entry = entry.unwrap();
            let path = entry.path();
            if path.is_file() {
                let file_name = path.file_name().unwrap().to_str().unwrap();
                if file_name.ends_with(".sst") {
                    let idx_file_name = format!("{}.{}", file_name, idx_file_suffix);
                    let idx_path = path.with_file_name(idx_file_name);
                    if idx_path.exists() {
                        let reader = SSTableReader::new(
                            path.to_str().unwrap(), 
                            idx_path.to_str().unwrap()).unwrap();
                        sstables.push(reader);
                    }
                }
            }
        }
        sstables
    }
}

impl<'a, 'b> Iterator for SSTableReaderIter<'a, 'b> {
    type Item = SSTableReader;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.sstables.len() {
            return None;
        }
        let mut reader = self.sstables[self.index].clone();
        if !reader.is_file_exists() {
            self.sstables = Self::get_sstables(self.root_dir, self.idx_file_suffix);
            reader = self.sstables[self.index].clone();
        }
        self.index += 1;
        Some(reader)
    }
}