use libc::{sysconf, _SC_PAGESIZE};
// chronoをラップして、タイムスタンプを取得する

pub fn get_timestamp() -> u64 {
    let now = chrono::Utc::now();
    let timestamp = now.timestamp_micros() as u64;
    timestamp
}

pub fn get_page_size() -> usize {
    unsafe {
        sysconf(_SC_PAGESIZE) as usize
    }
}

pub fn create_dir(dir: &str) -> Result<(), String> {
    std::fs::create_dir_all(dir).map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests;