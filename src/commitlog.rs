use core::time;
use std::{fs::{remove_file, File}, io::Write, time::SystemTime};

use crate::utils;

#[derive(Debug)]
pub struct CommitLog {
    dir: String,
    file_name: String,
    file: File,
}

impl CommitLog {
    pub fn new(dir: &str) -> Result<CommitLog, String> {
        let now = utils::get_timestamp();
        let file_name = format!("commit_{}.log", now);
        let filepath = format!("{}/{}", dir, &file_name);
        let file = File::create(&filepath).map_err(|e| e.to_string())?;
        Ok(CommitLog {
            dir: dir.to_string(),
            file_name,
            file,
        })
    }

    pub fn try_clone(&self) -> Result<Self, String> {
        let file = self.file.try_clone();
        match file {
            Ok(f) => Ok(CommitLog {
                dir: self.dir.clone(),
                file_name: self.file_name.clone(),
                file: f,
            }),
            Err(e) => Err(e.to_string()),
        }
    }

    // bufを入れてもいい
    // ページサイズを超えるようであれば、パディングを入れてもいい
    // 全てパフォーマンスを計測してから決める
    fn append(&mut self, entry: &CommitLogEntry, timestamp: u64) {
        let mut buf = entry.encode();
        buf.extend_from_slice(&timestamp.to_ne_bytes());
        self.file.write_all(&buf).map_err(|e| e.to_string()).unwrap();
    }

    pub fn write_put(&mut self, key: &str, value: &str, timestamp: u64) {
        let entry = CommitLogEntry::new("PUT", key, Some(value));
        self.append(&entry, timestamp);
    }

    pub fn delete_log(&self) -> Result<(), String> {
        remove_file(&self.get_file_path()).map_err(|e| e.to_string())
    }

    pub fn get_file_path(&self) -> String {
        format!("{}/{}", self.dir, self.file_name)
    }

    pub fn get_dir(&self) -> &str {
        &self.dir
    }

}

pub struct CommitLogEntry {
    pub cmd: CommitLogCmd,
    pub key: String,
    pub value: Option<String>,
}

impl CommitLogEntry {
    pub fn new(cmd: &str, key: &str, value: Option<&str>) -> CommitLogEntry {
        let cmd = match cmd {
            "PUT" => CommitLogCmd::Put,
            "DELETE" => CommitLogCmd::Delete,
            _ => panic!("Invalid command"),
        };
        CommitLogEntry {
            cmd,
            key: key.to_string(),
            value: value.map(|s| s.to_string()),
        }
    }

    pub fn to_string(&self) -> String {
        match self.cmd {
            CommitLogCmd::Put => format!("PUT {} {}", self.key, self.value.clone().unwrap()),
            CommitLogCmd::Delete => format!("DELETE {}", self.key),
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        match self.cmd {
            CommitLogCmd::Put => {
                let mut buf = Vec::new();
                buf.push(1u8);
                buf.extend_from_slice(&self.key.len().to_ne_bytes());
                buf.extend_from_slice(self.key.as_bytes());
                buf.extend_from_slice(&self.value.clone().unwrap().len().to_ne_bytes());
                buf.extend_from_slice(self.value.clone().unwrap().as_bytes());
                buf
            }
            CommitLogCmd::Delete => {
                let mut buf = Vec::new();
                buf.push(2u8);
                buf.extend_from_slice(&self.key.len().to_ne_bytes());
                buf.extend_from_slice(self.key.as_bytes());
                buf
            }
        }
    }
}

pub enum CommitLogCmd {
    Put = 1,
    Delete,
}

#[cfg(test)]
mod tests;