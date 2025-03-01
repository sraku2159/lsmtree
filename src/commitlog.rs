use std::{fs::{remove_file, File}, io::Write, time::SystemTime};

#[derive(Debug, Clone)]
pub struct CommitLog {
    log_file: (String, File),
}

impl CommitLog {
    pub fn new(dir: &str) -> Result<CommitLog, String> {
        if std::fs::metadata(dir).map(|m| m.is_dir()).unwrap_or(false) {
            Self::create_dir(dir)?;
        }

        match SystemTime::now().elapsed() {
            Ok(n) => {
                let filename = format!("{}/commit_{}.log", dir, n.as_millis());
                let file = File::create(&filename).map_err(|e| e.to_string())?;

                Ok(CommitLog {
                log_file: (filename, file),
            })},
            Err(_) => Err("SystemTime before UNIX EPOCH!".to_string()),
        }
    }

    fn create_dir(dir: &str) -> Result<(), String> {
        std::fs::create_dir_all(dir).map_err(|e| e.to_string())
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

    pub fn delete_log() -> Result<(), String> {
        remove_file("commit.log").map_err(|e| e.to_string())
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