use std::{fs::{remove_file, File}, io::Write};

pub struct CommitLog {
    log_file: (String, File),
}

impl CommitLog {
    pub fn new(filename: Option<String>) -> Result<CommitLog, String> {
        let filename = filename.unwrap_or("commit.log".to_string());
        let file = File::create(&filename);
        match file {
            Ok(f) => Ok(CommitLog {
                log_file: (filename, f),
            }),
            Err(e) => Err(e.to_string()),
        }
    }

    // bufを入れてもいい
    // ページサイズを超えるようであれば、パディングを入れてもいい
    // 全てパフォーマンスを計測してから決める
    fn append(&mut self, entry: &CommitLogEntry) {
        let buf = entry.encode();
        self.log_file.1.write_all(&buf).unwrap();
    }

    pub fn write_put(&mut self, key: &str, value: &str) {
        let entry = CommitLogEntry::new("PUT", key, Some(value));
        self.append(&entry);
    }
}

impl Drop for CommitLog {
    fn drop(&mut self) {
        remove_file(self.log_file.0.clone()).unwrap();
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