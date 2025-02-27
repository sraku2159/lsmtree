use crate::commitlog::CommitLogEntry;

/*
------------------------------------------------------------------------
| cmd | arg0_len | arg0 | arg1_len | arg1 |...
------------------------------------------------------------------------
cmd:
PUT: 0
UPDATE: 1
DELETE: 2
0 < argN_len < U64::MAX
*/ 
#[cfg(target_pointer_width = "64")]
#[test]
fn test_put_encode() {
    let entry = CommitLogEntry::new("PUT", "key", Some("value"));
    let buf = entry.encode();
    assert_eq!(buf, vec![0, 3, 0, 0, 0, 0, 0, 0, 0, 107, 101, 121, 5, 0, 0, 0, 0, 0, 0, 0, 118, 97, 108, 117, 101]);
}

#[test]
fn test_delete_encode() {
    let entry = CommitLogEntry::new("DELETE", "key", None);
    let buf = entry.encode();
    assert_eq!(buf, vec![1, 3, 0, 0, 0, 0, 0, 0, 0, 107, 101, 121]);
}