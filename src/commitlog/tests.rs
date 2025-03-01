use crate::commitlog::CommitLogEntry;

/*
------------------------------------------------------------------------
| cmd | arg0_len | arg0 | arg1_len | arg1 |...
------------------------------------------------------------------------
cmd:
PUT: 1
DELETE: 2
0 < argN_len < U64::MAX
*/ 
#[cfg(target_pointer_width = "64")]
#[test]
fn test_cl_put_encode() {
    let entry = CommitLogEntry::new("PUT", "key", Some("value"));
    let buf = entry.encode();
    assert_eq!(buf, vec![
        1,                      // cmd: PUT 
        3, 0, 0, 0, 0, 0, 0, 0, // arg0_len: 3
        107, 101, 121,          // arg0: "key"
        5, 0, 0, 0, 0, 0, 0, 0, // arg1_len: 5  
        118, 97, 108, 117, 101  // arg1: "value"
    ]);
}

#[test]
fn test_cl_utf8() {
    let value = "バリュー";
    let value_bytes = value.as_bytes();

    assert_eq!(value_bytes, &[227, 131, 144, 227, 131, 170, 227, 131, 165, 227, 131, 188]);
}

#[cfg(target_pointer_width = "64")]
#[test]
fn test_cl_put_encode_key_utf8() {
    let entry = CommitLogEntry::new("PUT", "キー", Some("バリュー"));
    let buf = entry.encode();
    assert_eq!(buf, vec![
        1,                                                          // cmd: PUT 
        6, 0, 0, 0, 0, 0, 0, 0,                                     // arg0_len: 6
        227, 130, 173, 227, 131, 188,                               // arg0: "キー"
        12, 0, 0, 0, 0, 0, 0, 0,                                    // arg1_len: 9  
        227, 131, 144, 227, 131, 170, 227, 131, 165, 227, 131, 188 // arg1: "バリュー"
    ]);
}

#[cfg(target_pointer_width = "64")]
#[test]
fn test_cl_delete_encode() {
    let entry = CommitLogEntry::new("DELETE", "key", None);
    let buf = entry.encode();
    assert_eq!(buf, vec![
        2,                      // cmd: DELETE
        3, 0, 0, 0, 0, 0, 0, 0, // arg0_len: 3
        107, 101, 121           // arg0: "key"
    ]);
}

#[cfg(target_pointer_width = "64")]
#[test]
fn test_cl_delete_encode_key_utf8() {
    let entry = CommitLogEntry::new("DELETE", "キー", None);
    let buf = entry.encode();
    assert_eq!(buf, vec![
        2,                           // cmd: DELETE
        6, 0, 0, 0, 0, 0, 0, 0,      // arg0_len: 6
        227, 130, 173, 227, 131, 188 // arg0: "キー"
    ]);
}