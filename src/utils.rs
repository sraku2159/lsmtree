use libc::{sysconf, _SC_PAGESIZE};

pub fn get_page_size() -> usize {
    unsafe {
        sysconf(_SC_PAGESIZE) as usize
    }
}
