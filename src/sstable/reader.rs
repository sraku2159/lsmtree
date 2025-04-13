use std::{fs::{File, Metadata}, io::{Read, Seek}};

use super::{SSTableData, SSTableHeader, SSTableIndex, Value};

type Offset = usize;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SSTableReader {
    pub file: String,
    pub index_file: String,
    pub index: SSTableIndex,
    pub data: SSTableData,
}

impl SSTableReader {
    pub fn new(file: &str, index_file: &str) -> Result<SSTableReader, String> {
        // 1. fileの存在チェック
        if !std::path::Path::new(file).exists() {
            return Err(format!("{} not found", file));
        }
        if !std::path::Path::new(index_file).exists() {
            return Err(format!("{} not found", index_file));
        }

        // 3. index, dataの初期化
        let index = SSTableIndex::new();
        let data = SSTableData::new();
        Ok(
            SSTableReader {
                file: file.to_string(),
                index_file: index_file.to_string(),
                index,
                data,
            }
        )
    }

    pub fn metadata(&self) -> Result<Metadata, String> {
        std::fs::metadata(&self.file).map_err(|e| e.to_string())
    }

    pub fn data(&self) -> Result<SSTableData, String> {
        let mut buf = vec![0u8; 16];
        let mut f = File::open(&self.file).map_err(|e| e.to_string())?;
        let _ = f.read_exact(&mut buf).map_err(|e| e.to_string())?;
        let data = SSTableData::decode(&buf).map_err(|e| e.to_string())?;
        Ok(data)
    }

    pub fn is_file_exists(&self) -> bool {
        std::path::Path::new(&self.file).exists()
    }

    pub fn read(&self, key: &str) -> Result<Option<Value>, String> {
        Self::read_impl(&self.file, &self.index_file, key)
    }

    fn read_impl(file: &str, index_file: &str, key: &str) -> Result<Option<Value>, String> {
        // let (header, offset) = Self::read_header(file)?;
        let idx_file_size = std::fs::metadata(index_file).map_err(|e| e.to_string())?.len() as usize;
        let index = Self::read_index(index_file, 0, idx_file_size)?;
        if let Some((begin, end)) = index.find_key_range(&key.to_owned()) {
            let end = end.unwrap_or(
                File::open(file).map_err(|e| e.to_string())?.metadata().map_err(|e| e.to_string())?.len() as u64
            );
            let data = Self::read_data(file, begin, end)?;
            let value = data.get(&key.to_owned(), None).cloned();
            return Ok(value)
        }
        Ok(None)
    }

    // pub fn read_header(file: &str) -> Result<(SSTableHeader, Offset), String> {
    //     let mut buf = vec![0u8; 16];
    //     let mut f = File::open(file).map_err(|e| e.to_string())?;
    //     let _ = f.read_exact(&mut buf).map_err(|e| e.to_string())?;
    //     let header = SSTableHeader::decode(&buf).map_err(|e| e.to_string())?;
    //     Ok((header, 16))
    // }

    pub fn read_index(file: &str, offset: Offset, size: usize) -> Result<SSTableIndex, String> {
        let mut buf = vec![0u8; size];
        let mut f = File::open(file).map_err(|e| e.to_string())?;
        f.seek(std::io::SeekFrom::Start(offset as u64)).map_err(|e| e.to_string())?;
        let _ = f.read_exact(&mut buf).map_err(|e| e.to_string())?;
        let index = SSTableIndex::decode(&buf).map_err(|e| e.to_string())?;
        Ok(index)
    }

    // [begin, end)
    pub fn read_data(file: &str, begin: u64, end: u64) -> Result<SSTableData, String> {
        let mut f = File::open(file).map_err(|e| e.to_string())?;
        let mut buf = vec![0u8; (end - begin) as usize];
        f.seek(std::io::SeekFrom::Start(begin)).map_err(|e| e.to_string())?;
        let _ = f.read_exact(&mut buf).map_err(|e| e.to_string())?;
        let data = SSTableData::decode(&buf).map_err(|e| e.to_string())?;
        Ok(data)
    }
}

#[cfg(test)]
mod tests{
    use std::fs;

    use crate::sstable::{reader::SSTableReader, SSTableHeader};

    #[test]
    fn test_sst_reader_new() {
        let path = "/tmp/test_sst_reader_new.sst";
        let index_path = "/tmp/test_sst_reader_new.sst.idx";
        let header_size = 8u64;
        let index_size = 8u64;
        let data = vec![
            header_size.to_ne_bytes().to_vec(),
            index_size.to_ne_bytes().to_vec(),
        ].concat();

        fs::write(
            path, 
            data
        ).unwrap();

        fs::write(
            index_path, 
            "dummy_index_data"
        ).unwrap();

        let sst_reader = SSTableReader::new(path, index_path).unwrap();
        assert_eq!(sst_reader.file, path);
        assert_eq!(sst_reader.index_file, index_path);
    
        fs::remove_file(path).unwrap();
        fs::remove_file(index_path).unwrap();
    }

    #[test]
    fn test_sst_reader_simple_read() {
        let path = "/tmp/test_sst_reader_simple_read.sst";
        let idx_path = path.to_string() + ".idx";
        let kvs = vec![
            ("key1", "value1"),
            ("key2", "value2"),
            ("key3", "value3"),
        ];
        let timestamp = 12345u64; // テスト用の固定タイムスタンプ
        let data = vec![
            kvs.iter().map(|(k, v)| {
                let k = k.as_bytes();
                let v = v.as_bytes();
                let k_len = k.len() as u64;
                let v_len = v.len() as u64;
                vec![
                    k_len.to_ne_bytes().to_vec(),
                    k.to_vec(),
                    v_len.to_ne_bytes().to_vec(),
                    v.to_vec(),
                    timestamp.to_ne_bytes().to_vec(), // タイムスタンプを追加
                ].concat()
            }).flatten().collect::<Vec<u8>>(),
        ].concat();

        let index = vec![
            "key1".len().to_ne_bytes().to_vec(),
            "key1".as_bytes().to_vec(),
            0u64.to_ne_bytes().to_vec(),
        ].concat();

        fs::write(
            path,
            data 
        ).unwrap();
    
        fs::write(
            &idx_path, 
            index
        ).unwrap();
    
        let sst_reader = SSTableReader::new(path, &idx_path).unwrap();
        for (k, v) in kvs {
            let value = sst_reader.read(k).unwrap();
            assert_eq!(value, Some((Some(v.to_string()), timestamp)));
        }
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn test_sst_reader_read_deleted() {
        let path = "/tmp/test_sst_reader_read_deleted.sst";
        let idx_path = path.to_string() + ".idx";
        let kvs = vec![
            ("key1", Some("value1".to_string())),
            ("key2", None), // 削除されたキー
            ("key3", Some("value3".to_string())),
        ];
        
        // データ部分の作成
        let timestamp = 12345u64; // テスト用の固定タイムスタンプ
        let data = kvs.iter().map(|(k, v)| {
            let k = k.as_bytes();
            let v = v.as_ref().map_or("".as_bytes(), |v| v.as_bytes());
            let k_len = k.len() as u64;
            let v_len = v.len() as u64;
            vec![
                k_len.to_ne_bytes().to_vec(),
                k.to_vec(),
                v_len.to_ne_bytes().to_vec(),
                v.to_vec(),
                timestamp.to_ne_bytes().to_vec(), // タイムスタンプを追加
            ].concat()
        }).collect::<Vec<Vec<u8>>>().concat();
        
        // インデックス部分の作成
        let index = vec![
            "key1".len().to_ne_bytes().to_vec(),
            "key1".as_bytes().to_vec(),
            0u64.to_ne_bytes().to_vec(),
        ].concat();

        // ファイルの作成
        fs::write(
            path, 
            data
        ).unwrap();
    
        fs::write(
            &idx_path, 
            index
        ).unwrap();
    
        let sst_reader = SSTableReader::new(path, &idx_path).unwrap();
        
        // 削除されたキーの読み取りテスト
        let value = sst_reader.read("key2").unwrap();
        assert_eq!(value, Some((None, timestamp))); // 削除されたキーはNoneが返される
        
        // 通常のキーの読み取りテスト
        let value = sst_reader.read("key1").unwrap();
        assert_eq!(value, Some((Some("value1".to_string()), timestamp)));
        
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn test_sst_reader_read_not_exists() {
        let path = "/tmp/test_sst_reader_read_not_exists.sst";
        let idx_path = path.to_string() + ".idx";
        let kvs = vec![
            ("key1", "value1"),
            ("key3", "value3"),
        ];
        
        // データ部分の作成
        let timestamp = 12345u64; // テスト用の固定タイムスタンプ
        let data = kvs.iter().map(|(k, v)| {
            let k = k.as_bytes();
            let v = v.as_bytes();
            let k_len = k.len() as u64;
            let v_len = v.len() as u64;
            vec![
                k_len.to_ne_bytes().to_vec(),
                k.to_vec(),
                v_len.to_ne_bytes().to_vec(),
                v.to_vec(),
                timestamp.to_ne_bytes().to_vec(), // タイムスタンプを追加
            ].concat()
        }).collect::<Vec<Vec<u8>>>().concat();
        
        // インデックス部分の作成
        let index = vec![
            "key1".len().to_ne_bytes().to_vec(),
            "key1".as_bytes().to_vec(),
            0u64.to_ne_bytes().to_vec(),
        ].concat();

        // ファイルの作成
        fs::write(
            path, 
            data
        ).unwrap();

        fs::write(
            &idx_path, 
            index
        ).unwrap();

        let sst_reader = SSTableReader::new(path, &idx_path).unwrap();
        
        // 存在しないキーの読み取りテスト
        let value = sst_reader.read("key2").unwrap();
        assert_eq!(value, None); // 存在しないキーはNoneが返される
        
        // 存在するキーの読み取りテスト
        let value = sst_reader.read("key1").unwrap();
        assert_eq!(value, Some((Some("value1".to_string()), timestamp)));
        
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn test_sst_reader_read_big_data() {
        let path = "/tmp/test_sst_reader_read_big_data.sst";
        let idx_path = path.to_string() + ".idx";

        // 大きなデータの作成（10KBの文字列）
        let big_value = "a".repeat(10 * 1024);
        let kvs = vec![
            ("key1", "value1"),
            ("key2", &big_value),
            ("key3", "value3"),
        ];
        
        // データ部分の作成
        let timestamp = 12345u64; // テスト用の固定タイムスタンプ
        let data = kvs.iter().map(|(k, v)| {
            let k = k.as_bytes();
            let v = v.as_bytes();
            let k_len = k.len() as u64;
            let v_len = v.len() as u64;
            vec![
                k_len.to_ne_bytes().to_vec(),
                k.to_vec(),
                v_len.to_ne_bytes().to_vec(),
                v.to_vec(),
                timestamp.to_ne_bytes().to_vec(), // タイムスタンプを追加
            ].concat()
        }).collect::<Vec<Vec<u8>>>().concat();
        
        // インデックス部分の作成
        let index = vec![
            "key1".len().to_ne_bytes().to_vec(),
            "key1".as_bytes().to_vec(),
            0u64.to_ne_bytes().to_vec(),
        ].concat();

        // ファイルの作成
        fs::write(
            path, 
            data
        ).unwrap();
    
        fs::write(
            &idx_path, 
            index
        ).unwrap();
    
        let sst_reader = SSTableReader::new(path, &idx_path).unwrap();
        
        // 大きなデータの読み取りテスト
        let value = sst_reader.read("key2").unwrap();
        assert_eq!(value, Some((Some(big_value), timestamp))); // 大きなデータは正しく読み取れる

        // 通常のキーの読み取りテスト
        let value = sst_reader.read("key1").unwrap();
        assert_eq!(value, Some((Some("value1".to_string()), timestamp)));
        
        fs::remove_file(path).unwrap();
    }
}
