use std::fs;

#[test]
fn test_local_timestamp() {
    println!("{:?}", chrono::Local::now())
}

#[test]
fn test_create_dir() {
    let path = "/tmp/test";
    assert!(super::create_dir(path).is_ok());
    fs::remove_dir(path).unwrap();
}

#[test]
fn test_create_dir_with_relative_path() {
    let path = "./tmp/test";
    assert!(super::create_dir(path).is_ok());
    fs::remove_dir_all(path).unwrap();
}