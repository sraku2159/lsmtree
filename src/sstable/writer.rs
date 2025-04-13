use std::{fs::File, io::Write};

use crate::{memtable::MemTable, utils::{self, get_page_size}};

use super::{SSTableData, SSTableHeader, SSTableIndex};


#[derive(Debug)]
pub struct SSTableWriter {
    pub file: String,
}

impl SSTableWriter {
    pub fn new(dir: &str) -> Result<SSTableWriter, String> {
        Ok(SSTableWriter {
            file: format!("{}/{}.sst", dir, utils::get_timestamp()),
        })
    }

    pub fn write(&self, memtable: &MemTable, index_interval: usize) -> Result<(), String> {
        Self::write_impl(memtable, &self.file, index_interval)
    }

    fn write_impl(memtable: &MemTable, file: &str, index_interval: usize) -> Result<(), String> {
        let mut file = File::create(file).map_err(|e| e.to_string())?;
        let data = SSTableData::try_from(memtable.encode())?;
        let index = SSTableIndex::from_sstable_data(&data, index_interval as u64);
        // let data = SSTableData::try_from(memtable.encode())?;
        Self::write_header_impl(&mut file, &data)?;
        Self::write_data_impl(&mut file, &data)?;
        Self::write_index_impl(&mut file, &index)
    }

    fn write_header_impl(file: &mut File, data: &SSTableData) -> Result<(), String> {
        let index_size = data.len() as u64;
        let header = SSTableHeader::new(
            SSTableHeader::SIZE as u64,
            index_size
        );
        let header = header.encode();
        file.write_all(&header).map_err(|e| e.to_string())
    }

    fn write_index_impl(file: &mut File, index: &SSTableIndex) -> Result<(), String> {
        let mut index = index.encode();
        file.write_all(&mut index).map_err(|e| e.to_string())
    }

    // ページサイズごとに書き込むという方法との比較を時間計算量の観点で今後したい
    fn write_data_impl(file: &mut File, data: &SSTableData) -> Result<(), String> {
        let mut data =  data.encode().clone();
        file.write_all(&mut data).map_err(|e| e.to_string())
    }
}

#[cfg(all(test, target_endian = "little"))]
mod tests {
    use std::fs::{self, remove_file, File};

    use crate::{memtable::MemTable, sstable::{writer::SSTableWriter, SSTableData, SSTableHeader, SSTableIndex}, utils::get_page_size};

    #[test]
    fn test_sst_writer_wirte_impl() {
        let timestamp = crate::utils::get_timestamp() as u64; // 実際のタイムスタンプ
        let page_size = get_page_size();
        let mut memtable = MemTable::new();
        memtable.put("key1", "value1", timestamp);
        let path = "/tmp/test_sst_writer_wirte_impl.sst";
        assert!(SSTableWriter::write_impl(&memtable, path, page_size).is_ok());

        let content = fs::read(path).unwrap();
        // タイムスタンプを含むデータ形式に更新
        let timestamp = 12345u64; // テスト用の固定タイムスタンプ
        let mut data = vec![
            4, 0, 0, 0, 0, 0, 0, 0,
            107, 101, 121, 49,
            6, 0, 0, 0, 0, 0, 0, 0,
            118, 97, 108, 117, 101, 49,
        ];
        data.extend_from_slice(&timestamp.to_ne_bytes());
        let header = vec![
            (SSTableHeader::SIZE as u64).to_ne_bytes().to_vec(),
            (data.len() as u64).to_ne_bytes().to_vec(),
        ].concat();
        let index = vec![
            4, 0, 0, 0, 0, 0, 0, 0,
            107, 101, 121, 49,
            36, 0, 0, 0, 0, 0, 0, 0,
        ];
        // タイムスタンプは動的に生成されるため、長さのみを検証
        let expected_content = vec![
            header,
            data,
            index,
        ].concat();
        assert_eq!(content.len(), expected_content.len());

        // タイムスタンプ以外の部分を検証
        assert_eq!(&content[0..36], &expected_content[0..36]); // ヘッダーとインデックス
        assert_eq!(&content[36..64], &expected_content[36..64]); // データ（タイムスタンプを除く）
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn test_sst_writer_write_header_impl() {
        let mut index = SSTableIndex::new();
        index.insert("a".to_owned(), 0);
        index.insert("b".to_owned(), 3);
        index.insert("c".to_owned(), 1000);
        let timestamp = 12345u64; // テスト用の固定タイムスタンプ
        let mut data_vec = vec![
            1, 0, 0, 0, 0, 0, 0, 0, // key_len: 1
            97, // key: "a"
            1, 0, 0, 0, 0, 0, 0, 0, // value_len: 1
            49, // value: "1"
        ];
        data_vec.extend_from_slice(&timestamp.to_ne_bytes()); // タイムスタンプ
        let data = SSTableData::try_from(data_vec).unwrap();
        let path = "/tmp/test_sst_writer_write_header_impl.sst";
        let mut file = File::create(&path).unwrap();

        assert!(SSTableWriter::write_header_impl(&mut file, &data).is_ok());

        let index_size = index.size() as u64;
        let data_size = data.len() as u64;
        let content = fs::read(&path).unwrap();
        assert_eq!(
            content,
            vec![
                (SSTableHeader::SIZE as u64).to_ne_bytes().to_vec(),
                index_size.to_ne_bytes().to_vec(),
            ].concat()
        );
        remove_file(&path).unwrap();
    }

    #[test]
    fn test_fn_writer_write_index_impl() {
        let timestamp = 12345u64; // テスト用の固定タイムスタンプ
        let page_size = get_page_size() as u64;
        let mut memtable = MemTable::new();
        memtable.put("key1", "value1", timestamp);
        let path = "/tmp/test_sst_writer_write_index_impl.sst";
        let mut file = File::create(path).unwrap();
        assert!(SSTableWriter::write_index_impl(&mut file, &SSTableIndex::from_memtable(&memtable, page_size)).is_ok());

        let header_size = SSTableHeader::SIZE as usize;
        let index_size = "key1".len() + 16;
        let content = fs::read(path).unwrap();
        assert_eq!(
            content,
            vec![
                vec![
                    4, 0, 0, 0, 0, 0, 0, 0, // length of key1
                    107, 101, 121, 49       // key1
                ], 
                (header_size + index_size).to_ne_bytes().to_vec()
            ].concat());
        fs::remove_file(path).unwrap();
    }

    // 本当はページサイズを考慮して、ヘッダーとインデックスを考慮した設計にしたい。
    /// しかし、現状の設計だとデータはヘッダーやインデックスに依存させたくはないため無理。
    /// もしやるならファイルフォーマットから変更する必要がある。
    #[test]
    fn test_fn_writer_write_index_impl_complex() {
        let timestamp = crate::utils::get_timestamp() as u64; // 実際のタイムスタンプ
        let header_size = SSTableHeader::SIZE as usize;
        let page_size = get_page_size() as u64;
        let mut memtable = MemTable::new();

        memtable.put("key1", "value1", timestamp); // 8 + 4 + 8 + 6 + 8 = 34
        memtable.put("キー4", "c", timestamp);
        memtable.put("key3", "b".repeat(get_page_size() as usize).as_str(), timestamp); // これはページの先頭から始まる. 超過分: key_len(8) + 4 + value_len(8) + 1 + timestamp_len(8) = 29
        memtable.put("key2", "a".repeat(get_page_size() as usize - (header_size + 34 + 28)).as_str(), timestamp); // header_size(16) + index_size(63) + 34 + key_len(8) + 4 + value_len(8) + timestamp_len(8) 

        let path = "/tmp/test_fn_writer_write_index_impl_complex.sst";
        let mut file = File::create(path).unwrap();
        assert!(SSTableWriter::write_index_impl(&mut file, &SSTableIndex::from_memtable(&memtable, page_size)).is_ok());


        let content = fs::read(&path).unwrap();
        assert_eq!(
            content,
            vec![
                "key1".len().to_ne_bytes().to_vec(),
                "key1".as_bytes().to_vec(),
                (header_size as u64).to_ne_bytes().to_vec(),
                "key3".len().to_ne_bytes().to_vec(),
                "key3".as_bytes().to_vec(),
                (get_page_size() as u64).to_ne_bytes().to_vec(),
                "キー4".len().to_ne_bytes().to_vec(),
                "キー4".as_bytes().to_vec(),
                (get_page_size() as u64 * 2 + 29).to_ne_bytes().to_vec(),
            ].concat());

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn test_sst_writer_wirte_data_impl() {
        let timestamp = crate::utils::get_timestamp() as u64; // 実際のタイムスタンプ
        let mut memtable = MemTable::new();
        memtable.put("key1", "value1", timestamp);
        let path = "/tmp/test_sst_writer_wirte_data_impl.sst";
        let mut file = File::create(path).unwrap();
        let data = SSTableData::try_from(memtable.encode()).unwrap();
        assert!(SSTableWriter::write_data_impl(&mut file, &data).is_ok());
    
        // バイナリデータを含むため、read_to_stringではなくreadを使用
        let content = fs::read(path).unwrap();
        
        // タイムスタンプを含むデータ形式に更新
        let mut expected_data = Vec::new();
        expected_data.extend_from_slice(&4u64.to_ne_bytes()); // key_len: 4
        expected_data.extend_from_slice("key1".as_bytes()); // key: "key1"
        expected_data.extend_from_slice(&6u64.to_ne_bytes()); // value_len: 6
        expected_data.extend_from_slice("value1".as_bytes()); // value: "value1"
        expected_data.extend_from_slice(&timestamp.to_ne_bytes()); // タイムスタンプ
        
        // タイムスタンプは動的に生成されるため、長さのみを検証
        assert_eq!(content.len(), expected_data.len());
        
        // タイムスタンプ以外の部分を検証
        assert_eq!(&content[0..28], &expected_data[0..28]);
        fs::remove_file(path).unwrap();
    }
}
