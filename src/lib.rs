pub mod memtable;
pub mod commitlog;
pub mod sstable;
pub mod utils;
mod thread_pool;

use std::{collections::HashMap, sync::{Arc, Mutex, RwLock}, thread::{self, sleep, spawn}};

use memtable::MemTable;
use commitlog::CommitLog;
use sstable::{compaction::Compaction, reader::SSTableReaderManager, SSTableWriter};

use utils::*;

use std::io::ErrorKind;

pub type Key = String;
pub type Value = String;

#[derive(Debug)]
pub struct SharedSSTableReader {
    inner: Mutex<HashMap<String, Arc<SSTableReaderManager>>>,
    pub sst_dir: String,
    pub index_file_suffix: String,
}

impl SharedSSTableReader {
    pub fn new(sst_dir: &str, index_file_suffix: &str) -> Arc<Self> {
        let inner = HashMap::new();
        Arc::new(SharedSSTableReader {
            inner: Mutex::new(inner),
            sst_dir: sst_dir.to_string(),
            index_file_suffix: index_file_suffix.to_string(),
        })
    }

    pub fn drop_resource(self: &Arc<Self>, file: &str) {
        let mut inner = self.inner.lock().unwrap();
        let resource = inner.get(file);
        if resource.is_none() {
            return;
        }
        let resource = resource.unwrap();
        let strong_count = Arc::strong_count(resource);
        dbg!("strong_count: {:?}", strong_count);
        if strong_count > 1 {
            return;
        }
        if let Some(reader) = inner.remove(file) {
            dbg!("ここにはきてる");
            drop(reader);
        }
    }

    pub fn get_reader(self: &Arc<Self>, file: &str) -> Option<Arc<SSTableReaderManager>> {
        let inner = self.inner.lock().unwrap();
        let resource = inner.get(file);
        if let Some(resource) = resource {
            if resource.is_deleted() {
                return None;
            }
            return Some(resource.clone());
        }
        drop(inner);
        Some(self.add_reader(file))
    }

    pub fn get_all(self: &Arc<Self>) -> Vec<Arc<SSTableReaderManager>> {
        let mut result = vec![];
        let dir = std::fs::read_dir(self.sst_dir.clone()).unwrap();
        for entry in dir {
            let entry = entry.unwrap();
            let path = entry.path();
            if path.is_file() {
                let file_name = path.file_name().unwrap().to_str().unwrap();
                if file_name.ends_with(".sst") {
                    let idx_file_name = format!("{}.{}", file_name, self.index_file_suffix);
                    let idx_path = path.with_file_name(idx_file_name);
                    if !idx_path.exists() {
                        continue;
                    }
                    if let Some(reader) = self.get_reader(path.to_str().unwrap()) {
                        result.push(reader.clone());
                    }
                }
            }
        }
        result
    }

    pub fn add_reader(self: &Arc<Self>, file: &str) -> Arc<SSTableReaderManager> {
        let mut inner = self.inner.lock().unwrap();
        let resource = inner.get(file);
        if resource.is_some() {
            return resource.unwrap().clone();
        }
        let index_file = format!("{}.{}", file, self.index_file_suffix);
        let reader = SSTableReaderManager::new(file, &index_file).unwrap();
        let reader = Arc::new(reader);
        inner.insert(file.to_string(), reader.clone());
        reader
    }

    pub fn to_vec(self: &Arc<Self>) -> Vec<Arc<SSTableReaderManager>> {
        let inner = self.inner.lock().unwrap();
        let mut result = vec![];
        for (_, reader) in inner.iter() {
            result.push(reader.clone());
        }
        result
    }
}


#[derive(Debug)]
pub struct LSMTreeConf<T, U = DefaultTimeStampGenerator>
where
    T: Compaction + Clone + Send + Sync + 'static,
    U: TimeStampGenerator + Send + Sync + 'static,
{
    compaction: T,
    timestamp_generator: U,
    sst_dir: String,
    commitlog_dir: String,
    memtable_threshold: usize,
    index_interval: usize,
    index_file_suffix: String,
    enable_compaction: bool,   // コンパクションを有効にするかどうか
}

impl<T: Compaction + Clone + Send + Sync + 'static, U: TimeStampGenerator + Send + Sync + 'static> LSMTreeConf<T, U> {
    pub fn new(
        compaction: T,
        timestamp_generator: U,
        sst_dir: Option<String>,
        commitlog_dir: Option<String>,
        memtable_threshold: Option<usize>, 
        index_interval: Option<usize>,
        index_file_suffix: Option<String>,
        enable_compaction: Option<bool>,
        ) -> LSMTreeConf<T, U>
    {
        let sst_dir = sst_dir.unwrap_or("./.sst".to_string());
        let commitlog_dir = commitlog_dir.unwrap_or("./.commitlog".to_string());
        let memtable_threshold = memtable_threshold.unwrap_or(get_page_size());
        let index_interval = index_interval.unwrap_or(get_page_size());
        let index_file_suffix = index_file_suffix.unwrap_or("idx".to_string());
        let enable_compaction = enable_compaction.unwrap_or(true);    // デフォルトは有効
        
        LSMTreeConf {
            compaction,
            timestamp_generator,
            sst_dir,
            commitlog_dir,
            memtable_threshold,
            index_interval,
            index_file_suffix,
            enable_compaction,
        }
    }
}

#[derive(Debug)]
pub struct LSMTree<T, U = DefaultTimeStampGenerator>
where
    T: Compaction,
    U: TimeStampGenerator,
{
    memtable: Mutex<MemTable>,
    commitlog: Mutex<CommitLog>,
    memtable_threshold: usize,
    index_interval: Arc<usize>,
    sst_dir: Arc<String>,
    shared_sstables: Arc<SharedSSTableReader>,
    compaction: T,
    timestamp_generator: U,
    rwlock_for_sstable_reader: Arc<RwLock<()>>,
    thread_pool: thread_pool::ThreadPool,
}

impl<T: Compaction + Clone + Send + Sync + 'static, U: TimeStampGenerator +  Send + Sync + 'static> LSMTree<T, U> {
    pub fn new(conf: LSMTreeConf<T, U>) -> Result<LSMTree<T, U>, String> {
        Self::create_dir(&conf.sst_dir)?;
        Self::create_dir(&conf.commitlog_dir)?;
        let sst_dir = Arc::new(conf.sst_dir.clone());
        let rwlock_for_sstable_reader = Arc::new(RwLock::new(()));

        let shared_sstable = SharedSSTableReader::new(
                sst_dir.as_ref(),
                &conf.index_file_suffix
            );
        let _ = if conf.enable_compaction {
            Some(Self::start_compaction_thread(
                sst_dir.clone(),
                conf.compaction.clone(),
                rwlock_for_sstable_reader.clone(),
                shared_sstable.clone(),
            ))
        } else {
            None
        };

        let lsm_tree = LSMTree {
            memtable: Mutex::new(MemTable::new()),
            memtable_threshold: conf.memtable_threshold,
            index_interval: Arc::new(conf.index_interval),
            commitlog: Mutex::new(CommitLog::new(&conf.commitlog_dir)?),
            shared_sstables: shared_sstable,
            sst_dir: sst_dir,
            compaction: conf.compaction,
            timestamp_generator: conf.timestamp_generator,
            rwlock_for_sstable_reader: rwlock_for_sstable_reader,
            thread_pool: thread_pool::ThreadPool::new(100),
        };

        Ok(lsm_tree)
    }

    fn start_compaction_thread(
        sst_dir: Arc<String>,
        compaction: T,
        rwlock_for_sstable_reader: Arc<RwLock<()>>,
        shared_sstable: Arc<SharedSSTableReader>,
    ) -> thread::JoinHandle<()> {
        let sst_dir = sst_dir.clone();
        let rwl = rwlock_for_sstable_reader.clone();

        spawn(move || {
            loop {
                sleep(std::time::Duration::from_secs(1));
                
                match SSTableWriter::new(&sst_dir) {
                    Ok(writer) => {
                        let _unused = rwl.write().unwrap();
                        match compaction.compact(
                            shared_sstable.clone(),
                            writer
                        ) {
                            Ok(_) => println!("compaction completed successfully"),
                            Err(e) => eprintln!("ERROR: compaction failed: {}", e),
                        }
                    },
                    Err(e) => eprintln!("ERROR: Failed to create SSTableWriter for compaction: {}", e),
                }
            }
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

    pub fn put(&mut self, key: &str, value: Option<&str>) -> Result<(), String> {
        let timestamp = self.timestamp_generator.get_timestamp();
        let ret = self.atomic_write_memtable(key, value, timestamp)?;
        if let Some((memtable, commitlog)) = ret {
            let dir = self.sst_dir.clone();
            let index_interval = self.index_interval.clone();

            self.thread_pool.execute(move || {
                Self::flush_memtable(
                    dir.as_ref(),
                    memtable, 
                    commitlog, 
                    *index_interval.as_ref(),
                );
            });
        }
        Ok(())
    }

    fn atomic_write_memtable(&mut self, key: &str, value: Option<&str>, timestamp: u64) -> Result<Option<(MemTable, CommitLog)>, String> {
        let mut commitlog = self.commitlog.lock().map_err(|e| e.to_string())?;
        let mut memtable = self.memtable.lock().map_err(|e| e.to_string())?;

        let _ = match value {
            Some(value) => {
                commitlog.write_put(key, value, timestamp);
                memtable.put(key, value, timestamp)
            },
            None => {
                commitlog.write_delete(key, timestamp);
                memtable.delete(key, timestamp)
            }
        };

        let ret = if memtable.len() >= self.memtable_threshold {
            let cloned_memtable = memtable.clone();
            let cloned_commitlog = commitlog.try_clone()?;
            drop(memtable);
            self.memtable = Mutex::new(MemTable::new());
            drop(commitlog);
            self.commitlog = Mutex::new(CommitLog::new(&cloned_commitlog.get_dir()).unwrap());

            Some((cloned_memtable, cloned_commitlog))
        } else {
            None
        };

        Ok(ret)
    }

    pub fn launch_compaction(&self) -> Result<(), String> {
        // let sstables: Vec<SSTableReader> = self.reader_iter().collect();
        let sstables = self.shared_sstables.to_vec();
        if sstables.len() <= 1 {
            return Ok(());
        }
        
        let writer = SSTableWriter::new(&self.sst_dir)?;
        self.compaction.compact(
            Arc::clone(&self.shared_sstables),
            writer,
        )
    }

    fn flush_memtable(
        dir: &String, 
        memtable: MemTable, 
        commitlog: CommitLog, 
        index_interval: usize,
    ) {
        let sstable = SSTableWriter::new(&dir).unwrap();
        let ret = sstable.write(&memtable, index_interval);
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
    }

    pub fn get(&self, key: &str) -> Result<Option<Value>, String> {
        let value = {
            self.memtable.lock().map_err(|e| e.to_string()).and_then(|memtable| {
                Ok(memtable.get(key))
            })?
        };

        match value {
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

    fn get_from_sstable(
        &self, 
        key: &str
    ) -> Result<Option<Value>, String> {
        let mut candidate = vec![];
        let rwlock = self.rwlock_for_sstable_reader.read().map_err(|e| e.to_string())?;
        for reader in self.readers() {
            match reader.read(key) {
                Ok(None) => continue,
                Ok(value) => {
                    candidate.push(value.unwrap());
                },
                Err(e) => {
                    dbg!("ERROR: get_from_sstable Error because of: {}", &e);
                    return Err(e)
                },
            }
        }
        drop(rwlock);

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
        let last = candidate.last().unwrap();
        dbg!(last);
        Ok(candidate.last().unwrap().0.clone())
    }

    pub fn get_memtable(&self) -> MemTable {
        let memtable = self.memtable.lock().map_err(|e| e.to_string()).unwrap();
        memtable.clone()
    }

    pub fn get_sst_dir(&self) -> &str {
        &self.sst_dir
    }

    pub fn get_commitlog(&self) -> CommitLog {
        let commitlog = self.commitlog.lock().map_err(|e| e.to_string()).unwrap();
        commitlog.try_clone().unwrap()
    }

    pub fn get_memtable_threshold(&self) -> usize {
        self.memtable_threshold
    }

    fn readers(&self) -> Vec<Arc<SSTableReaderManager>> {
        self.shared_sstables.get_all()
    }
}

pub trait TimeStampGenerator {
    fn get_timestamp(&mut self) -> u64;
}

pub struct DefaultTimeStampGenerator {}
impl TimeStampGenerator for DefaultTimeStampGenerator {
    fn get_timestamp(&mut self) -> u64 {
        utils::get_timestamp()
    }
}
