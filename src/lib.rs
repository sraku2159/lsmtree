pub mod memtable;
pub mod commitlog;
pub mod sstable;
pub mod utils;
mod thread_pool;

use std::{fmt::Debug, sync::{mpsc::{self, Receiver, Sender}, Arc, Mutex, RwLock}, thread::{self, spawn}};

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
    compaction_interval: u64,  // コンパクションの実行間隔（秒）
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
        compaction_interval: Option<u64>,
        enable_compaction: Option<bool>,
        ) -> LSMTreeConf<T, U>
    {
        let sst_dir = sst_dir.unwrap_or("./.sst".to_string());
        let commitlog_dir = commitlog_dir.unwrap_or("./.commitlog".to_string());
        let memtable_threshold = memtable_threshold.unwrap_or(get_page_size());
        let index_interval = index_interval.unwrap_or(get_page_size());
        let index_file_suffix = index_file_suffix.unwrap_or("idx".to_string());
        let compaction_interval = compaction_interval.unwrap_or(300); // デフォルトは5分（300秒）
        let enable_compaction = enable_compaction.unwrap_or(true);    // デフォルトは有効
        
        LSMTreeConf {
            compaction,
            timestamp_generator,
            sst_dir,
            commitlog_dir,
            memtable_threshold,
            index_interval,
            index_file_suffix,
            compaction_interval,
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
    // memtable: Arc<Mutex<MemTable>>,
    memtable: Mutex<MemTable>,
    commitlog: Mutex<CommitLog>,
    memtable_threshold: usize,
    index_interval: Arc<usize>,
    index_file_suffix: Arc<String>,
    sst_dir: Arc<String>,
    compaction: T,
    timestamp_generator: U,
    thread: Arc<Option<thread::JoinHandle<()>>>,
    compaction_target: Arc<Vec<SSTableReader>>,
    rwlock_for_sstable_reader: Arc<RwLock<()>>,
    sender: Sender<Vec<SSTableReader>>,
    thread_pool: thread_pool::ThreadPool,
}

impl<T: Compaction + Clone + Send + Sync + 'static, U: TimeStampGenerator +  Send + Sync + 'static> LSMTree<T, U> {
    pub fn new(conf: LSMTreeConf<T, U>) -> Result<LSMTree<T, U>, String> {
        Self::create_dir(&conf.sst_dir)?;
        Self::create_dir(&conf.commitlog_dir)?;
        let compaction_target = Arc::new(vec![]);
        let sst_dir = Arc::new(conf.sst_dir);
        let rwlock_for_sstable_reader = Arc::new(RwLock::new(()));
        let (sender, reciver) = mpsc::channel();

        let thread = if conf.enable_compaction {
            Some(Self::start_compaction_thread(
                sst_dir.clone(),
                conf.compaction.clone(),
                rwlock_for_sstable_reader.clone(),
                reciver,    
            ))
        } else {
            None
        };

        let lsm_tree = LSMTree {
            memtable: Mutex::new(MemTable::new()),
            memtable_threshold: conf.memtable_threshold,
            index_interval: Arc::new(conf.index_interval),
            index_file_suffix: Arc::new(conf.index_file_suffix),
            commitlog: Mutex::new(CommitLog::new(&conf.commitlog_dir)?),
            sst_dir: sst_dir,
            compaction: conf.compaction,
            timestamp_generator: conf.timestamp_generator,
            thread: Arc::new(thread),
            compaction_target: compaction_target,
            rwlock_for_sstable_reader: rwlock_for_sstable_reader,
            thread_pool: thread_pool::ThreadPool::new(100),
            sender,
        };

        Ok(lsm_tree)
    }

    fn start_compaction_thread(
        sst_dir: Arc<String>,
        // sstables: &Arc<Vec<SSTableReader>>,
        compaction: T,
        rwlock_for_sstable_reader: Arc<RwLock<()>>,
        receiver: Receiver<Vec<SSTableReader>>,

    ) -> thread::JoinHandle<()> {
        let sst_dir = sst_dir.clone();
        // let sstables = sstables.clone();
        let rwl = rwlock_for_sstable_reader.clone();

        spawn(move || {
            loop {
                // thread::park();
                // println!("compaction started with {:?}", sstables);
                // let sstables = sstables.to_vec();
                
                match SSTableWriter::new(&sst_dir) {
                    Ok(writer) => {
                        dbg!("ここにはきてる");
                        let sstables = receiver.recv().unwrap();
                        let _unused = rwl.write().unwrap();
                        dbg!("ここにもきてる");
                        match compaction.compact(
                            sstables,
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

    fn get_sstables_for_compaction(dir: &str, idx_file_suffix: &str) -> Vec<SSTableReader> {
        let mut sstables = Vec::new();
        
        let dir_entries = match std::fs::read_dir(dir) {
            Ok(entries) => entries,
            Err(e) => {
                eprintln!("ERROR: Failed to read directory {}: {}", dir, e);
                return sstables;
            }
        };
        
        for entry_result in dir_entries {
            let entry = match entry_result {
                Ok(e) => e,
                Err(_) => continue,
            };
            
            let path = entry.path();
            
            if !path.is_file() {
                continue;
            }
            
            let file_name = match path.file_name().and_then(|n| n.to_str()) {
                Some(name) => name,
                None => continue,
            };
            
            if !file_name.ends_with(".sst") {
                continue;
            }
            
            let idx_file_name = format!("{}.{}", file_name, idx_file_suffix);
            let idx_path = path.with_file_name(idx_file_name);
            
            if !idx_path.exists() {
                continue;
            }
            
            match SSTableReader::new(
                path.to_str().unwrap(), 
                idx_path.to_str().unwrap()
            ) {
                Ok(reader) => sstables.push(reader),
                Err(_) => continue,
            }
        }
        
        sstables
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
            let thread = Arc::clone(&self.thread);
            let idx_file_suffix = self.index_file_suffix.clone();
            let rwlock = self.rwlock_for_sstable_reader.clone();
            let sender =self.sender.clone();

            self.thread_pool.execute(move || {
                Self::flush_memtable(dir.as_ref(), memtable, commitlog, *index_interval.as_ref());
                dbg!("ここにはきてる");
                // let _unused = rwlock.read().unwrap();
                match rwlock.read() {
                    Ok(_) => {},
                    Err(e) => {
                        dbg!(&e);
                        println!("err: {:?}", e);
                    }
                };
                dbg!("ここにもきてる");
                let sstables = Self::get_sstables_for_compaction(&dir, &idx_file_suffix);
                if sstables.len() > 1 {
                    if let Some(thread) = thread.as_ref() {
                        sender.send(sstables).unwrap();
                        // drop(read);
                        // dbg!("ここにはきてる");
                        // thread.thread().unpark();
                    }
                }
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

    // 手動でコンパクションを実行するためのメソッド（テスト用）
    pub fn launch_compaction(&self) -> Result<(), String> {
        let sstables: Vec<SSTableReader> = self.reader_iter().collect();
        if sstables.len() <= 1 {
            return Ok(());
        }
        
        let writer = SSTableWriter::new(&self.sst_dir)?;
        self.compaction.compact(
            sstables, 
            writer,
        )
    }

    fn flush_memtable(dir: &String, memtable: MemTable, commitlog: CommitLog, index_interval: usize) {
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
            // let memtable = Arc::clone(&self.memtable);
            // let memtable = memtable.lock().map_err(|e| e.to_string()).map_err(|e| e.to_string())?;
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
        dbg!("ここにはきてる");
        let rwlock = self.rwlock_for_sstable_reader.read().map_err(|e| e.to_string())?;
        dbg!("ここにもきてる");
        for reader in self.reader_iter() {
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

    fn reader_iter(&self) -> SSTableReaderIter {
        SSTableReaderIter::new(
            &self.sst_dir, 
            &self.index_file_suffix,
            self.rwlock_for_sstable_reader.clone(),
        )
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

struct SSTableReaderIter {
    sstables: Vec<SSTableReader>,
    rwlock: Arc<RwLock<()>>,
    index: usize,
}

impl SSTableReaderIter {
    fn new(
        root_dir: & String,
        idx_file_suffix: & String,
        rwlock: Arc<RwLock<()>>,
    ) -> SSTableReaderIter {
        let sstables = Self::get_sstables(
            root_dir, 
            idx_file_suffix,
            rwlock.clone(),
        );
        SSTableReaderIter {
            sstables,
            rwlock,
            index: 0,
        }
    }

    fn get_sstables(
        root_dir: & String,
        idx_file_suffix: & String,
        rwlock: Arc<RwLock<()>>
    ) -> Vec<SSTableReader> {
        let mut sstables = Vec::new();
        dbg!("ここにはきてる");
        let _unused = rwlock.read().unwrap();
        dbg!("ここにもきてる");
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

impl Iterator for SSTableReaderIter {
    type Item = SSTableReader;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.sstables.len() {
            return None;
        }
        dbg!("ここにはきてる", self.index);
        let _unused = self.rwlock.read().unwrap();
        dbg!("ここにもきてる", self.index);
        let mut reader = self.sstables.get(self.index);
        while reader.is_none() {
            if self.index >= self.sstables.len() {
                return None;
            }
            reader = self.sstables.get(self.index);
        }
        let reader = reader.unwrap();
        self.index += 1;
        Some(reader.clone())
    }
}
