use std::{fs::{self, File}, io::{Read, Seek}, path::Path};

use super::{SSTableData, SSTableHeader, SSTableIndex, Value};

type Offset = usize;

#[derive(Debug, Clone)]
pub struct SSTableReader {
    pub file: String,
    header: SSTableHeader,
    pub index: SSTableIndex,
    pub data: SSTableData,
}

impl SSTableReader {
    pub fn new(file: &str) -> Result<SSTableReader, String> {
        let mut buf = vec![0u8; 16];
        // 1. fileの存在チェック
        if !std::path::Path::new(file).exists() {
            return Err(format!("{} not found", file));
        }
        // 2. hederを読み込む
        let mut f = std::fs::File::open(file).map_err(|e| e.to_string())?;
        let _ = f.read_exact(&mut buf).map_err(|e| e.to_string())?;
        let header = SSTableHeader::decode(&buf).map_err(|e| e.to_string())?;

        // 3. index, dataの初期化
        let index = SSTableIndex::new();
        let data = SSTableData::new();
        Ok(
            SSTableReader {
                file: file.to_string(),
                header,
                index,
                data,
            }
        )
    }

    pub fn read(&self, key: &str) -> Result<Value, String> {
        Self::read_impl(&self.file,  &self.header, key)
    }

    fn read_impl(file: &str, header: &SSTableHeader, key: &str) -> Result<Value, String> {
        unimplemented!();
        let (header, offset) = SSTableReader::read_header(file)?;
        // 1. ファイルのヘッダーからインデックスの位置 と サイズを取得
        let (index, offset) = SSTableReader::read_index(file, offset, header.index_size as usize)?;
        // 2. インデックスを2部探索でkeyを探す
        
        // 3. keyが見つかったら、データの位置とサイズを取得
        // 4. データを読み込む
        // 5. データをデコードして返す
    }

    pub fn read_header(file: &str) -> Result<(SSTableHeader, Offset), String> {
        let mut buf = vec![0u8; 16];
        let mut f = File::open(file).map_err(|e| e.to_string())?;
        let _ = f.read_exact(&mut buf).map_err(|e| e.to_string())?;
        let header = SSTableHeader::decode(&buf).map_err(|e| e.to_string())?;
        Ok((header, 16))
    }

    pub fn read_index(file: &str, offset: Offset, size: usize) -> Result<(SSTableIndex, Offset), String> {
        let mut buf = vec![0u8; size];
        let mut f = File::open(file).map_err(|e| e.to_string())?;
        f.seek(std::io::SeekFrom::Start(offset as u64)).map_err(|e| e.to_string())?;
        let _ = f.read_exact(&mut buf).map_err(|e| e.to_string())?;
        let index = SSTableIndex::decode(&buf).map_err(|e| e.to_string())?;
        Ok((index, offset + size))
    }

    pub fn read_data(file: &Path, offset: Offset) -> Result<SSTableData, String> {
        unimplemented!();
    }
}

#[cfg(test)]
mod tests{
    use std::fs;

    use crate::sstable::reader::SSTableReader;

    #[test]
    fn test_sst_reader_new() {
        let path = "/tmp/test_sst_reader_new.sst";
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

        let sst_reader = SSTableReader::new(path).unwrap();
        assert_eq!(sst_reader.file, path);
        assert_eq!(sst_reader.header.header_size, header_size);
        assert_eq!(sst_reader.header.index_size, index_size);
    
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn test_sst_reader_new_with_some_data() {
        let path = "/tmp/test_sst_reader_new_with_some_data.sst";
        let header_size = 8u64;
        let index_size = 8u64;
        let data = vec![
            header_size.to_ne_bytes().to_vec(),
            index_size.to_ne_bytes().to_vec(),
            (0..256).map(|i: i32| i.to_ne_bytes().to_vec()).flatten().collect::<Vec<u8>>(),
        ].concat();
    
        fs::write(
            path, 
            data
        ).unwrap();
    
        let sst_reader = SSTableReader::new(path).unwrap();
        assert_eq!(sst_reader.file, path);
        assert_eq!(sst_reader.header.header_size, header_size);
        assert_eq!(sst_reader.header.index_size, index_size);

        fs::remove_file(path).unwrap();
    }    

    #[test]
    fn test_sst_reader_simple_read() {
        let path = "/tmp/test_sst_reader_simple_read.sst";
        let kvs = vec![
            ("key1", "value1"),
            ("key2", "value2"),
            ("key3", "value3"),
        ];
        let data = vec![
            kvs.iter().map(|(k, v)| {
                let k = k.as_bytes();
                let v = v.as_bytes();
                let k_len = k.len() as u64;
                let v_len = v.len() as u64;
                vec![
                    k_len.to_ne_bytes().to_vec(),
                    v_len.to_ne_bytes().to_vec(),
                    k.to_vec(),
                    v.to_vec(),
                ].concat()
            }).flatten().collect::<Vec<u8>>(),
        ].concat();
        let index = vec![
            "key1".len().to_ne_bytes().to_vec(),
            "key1".as_bytes().to_vec(),
            0u64.to_ne_bytes().to_vec()
        ].concat();

        fs::write(
            path, 
            vec![
                8u64.to_ne_bytes().to_vec(),
                index.len().to_ne_bytes().to_vec(),
                index,
                data,
            ].concat()
        ).unwrap();
    
        let sst_reader = SSTableReader::new(path).unwrap();
        for (k, v) in kvs {
            let value = sst_reader.read(k).unwrap();
            assert_eq!(value, Some(v.to_string()));
        }
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn test_sst_reader_read_deleted() {}

    #[test]
    fn test_sst_reader_read_not_exists() {}

    #[test]
    fn test_sst_reader_read_read_big_data() {}
}