use std::io::{Read, Seek};

use super::{SSTableData, SSTableHeader, SSTableIndex, Value};

#[derive(Debug)]
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

    pub fn read(&self, key: &str) -> Result<Option<Value>, String> {
        Self::read_impl(&self.file,  &self.header, key)
    }

    fn read_impl(file: &str, header: &SSTableHeader, key: &str) -> Result<Option<Value>, String> {
        let file = std::fs::File::open(file)
            .map_err(|e| e.to_string())?
            .seek(std::io::SeekFrom::Start(header.header_size))
            .map_err(|e| e.to_string())?;
        unimplemented!();
        // 1. ファイルのヘッダーからインデックスの位置 と サイズを取得
        // 2. インデックスを2部探索でkeyを探す
        // 3. keyが見つかったら、データの位置とサイズを取得
        // 4. データを読み込む
        // 5. データをデコードして返す
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
}