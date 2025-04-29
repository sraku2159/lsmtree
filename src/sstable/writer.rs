use std::{fs::File, io::Write, thread, time::Duration};

use crate::{memtable::MemTable, utils};

use super::{SSTableData, SSTableIndex};


#[derive(Debug)]
pub struct SSTableWriter {
    pub file: String,
    pub index_file: String,
}

impl SSTableWriter {
    pub fn new(dir: &str) -> Result<SSTableWriter, String> {
        let file = format!("{}/{}.sst", dir, utils::get_timestamp());
        let index_file = format!("{}.idx", file);
        Ok(SSTableWriter {
            file,
            index_file,
        })
    }

    pub fn write(&self, memtable: &MemTable, index_interval: usize) -> Result<(), String> {
        thread::sleep(Duration::from_millis(1));
        Self::write_impl(memtable, &self.file, &self.index_file, index_interval)
    }

    fn write_impl(memtable: &MemTable, file: &str, index_file: &str, index_interval: usize) -> Result<(), String> {
        let mut file = File::create(file).map_err(|e| e.to_string())?;
        let mut index_file = File::create(index_file).map_err(|e| e.to_string())?;
        let data = SSTableData::try_from(memtable.encode())?;
        let index = SSTableIndex::from_sstable_data(&data, index_interval as u64);
        let data = SSTableData::try_from(memtable.encode())?;
        Self::write_data_impl(&mut file, &data)?;
        Self::write_index_impl(&mut index_file, &index)
    }

    pub fn write_with_index(&self, data: &SSTableData, index_interval: usize) -> Result<(), String> {
        let mut file = File::create(&self.file).map_err(|e| e.to_string())?;
        let mut index_file = File::create(&self.index_file).map_err(|e| e.to_string())?;
        let index = SSTableIndex::from_sstable_data(data, index_interval as u64);
        Self::write_data_impl(&mut file, data)?;
        Self::write_index_impl(&mut index_file, &index)
    }

    pub fn write_data(&self, data: &SSTableData) -> Result<(), String> {
        let mut file = File::create(&self.file).map_err(|e| e.to_string())?;
        Self::write_data_impl(&mut file, data)
    }

    pub fn write_index(&self, index: &SSTableIndex) -> Result<(), String> {
        let mut file = File::create(&self.index_file).map_err(|e| e.to_string())?;
        Self::write_index_impl(&mut file, index)
    }

    fn write_index_impl(file: &mut File, index: &SSTableIndex) -> Result<(), String> {
        let mut index = index.encode();
        file.write_all(&mut index).map_err(|e| e.to_string())
    }

    fn write_data_impl(file: &mut File, data: &SSTableData) -> Result<(), String> {
        let mut data =  data.encode().clone();
        dbg!(&data[&data.len() - 10..]);
        file.write_all(&mut data).map_err(|e| e.to_string())
    }
}

#[cfg(all(test, target_endian = "little"))]
mod tests {
    use std::fs::{self, File};

    use crate::{memtable::MemTable, sstable::{writer::SSTableWriter, SSTableData, SSTableIndex}, utils::get_page_size};

    #[test]
    fn test_sst_writer_wirte_impl() {
        let timestamp = crate::utils::get_timestamp() as u64; // 実際のタイムスタンプ
        let page_size = get_page_size();
        let mut memtable = MemTable::new();
        memtable.put("key1", "value1", timestamp);
        let path = "/tmp/test_sst_writer_wirte_impl.sst";
        let index_path = "/tmp/test_sst_writer_wirte_impl.sst.idx";
        assert!(SSTableWriter::write_impl(&memtable, path, index_path, page_size).is_ok());

        let mut expected_data = vec![
            4, 0, 0, 0, 0, 0, 0, 0,
            107, 101, 121, 49,
            6, 0, 0, 0, 0, 0, 0, 0,
            118, 97, 108, 117, 101, 49,
        ];
        expected_data.extend_from_slice(&timestamp.to_ne_bytes());
        let content = fs::read(path).unwrap();

        let expected_index = vec![
            4, 0, 0, 0, 0, 0, 0, 0,
            107, 101, 121, 49,
            0, 0, 0, 0, 0, 0, 0, 0,
        ];
        let index = fs::read(index_path).unwrap();

        assert_eq!(content.len(), expected_data.len());
        assert_eq!(index.len(), expected_index.len());
        assert_eq!(content, expected_data);
        assert_eq!(index, expected_index);
        fs::remove_file(path).unwrap();
        fs::remove_file(index_path).unwrap();
    }

    #[test]
    fn test_fn_writer_write_index_impl() {
        let timestamp = 12345u64; // テスト用の固定タイムスタンプ
        let page_size = get_page_size() as u64;
        let mut memtable = MemTable::new();
        memtable.put("key1", "value1", timestamp);
        let path = "/tmp/test_sst_writer_write_index_impl.sst.idx";
        let mut file = File::create(path).unwrap();
        let data = SSTableData::try_from(memtable.encode()).unwrap();
        assert!(SSTableWriter::write_index_impl(&mut file, &SSTableIndex::from_sstable_data(&data, page_size)).is_ok());

        let content = fs::read(path).unwrap();
        assert_eq!(
            content,
            vec![
                vec![
                    4, 0, 0, 0, 0, 0, 0, 0, // length of key1
                    107, 101, 121, 49       // key1
                ], 
                0u64.to_ne_bytes().to_vec(), // offset
            ].concat());
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn test_fn_writer_write_index_impl_complex() {
        let timestamp = crate::utils::get_timestamp() as u64; // 実際のタイムスタンプ
        let page_size = get_page_size() as u64;
        let mut memtable = MemTable::new();
        memtable.put("key1", "value1", timestamp); // 8 + 4 + 8 + 6 + 8 = 34
        memtable.put("キー4", "c", timestamp);
        memtable.put("key3", "b".repeat(get_page_size() as usize).as_str(), timestamp); // これはページの先頭から始まる. 超過分: key_len(8) + 4 + value_len(8)+ timestamp_len(8) = 28
        memtable.put("key2", "a".repeat(get_page_size() as usize - (34 + 28)).as_str(), timestamp); // 34 + key_len(8) + 4 + value_len(8) + timestamp_len(8) 

        let data = SSTableData::try_from(memtable.encode()).unwrap();
        let index = SSTableIndex::from_sstable_data(&data, page_size);
        let path = "/tmp/test_fn_writer_write_index_impl_complex.sst.idx";
        let mut file = File::create(path).unwrap();
        assert!(SSTableWriter::write_index_impl(&mut file, &index).is_ok());
        let content = fs::read(path).unwrap();

        assert_eq!(
            content,
            vec![
                "key1".len().to_ne_bytes().to_vec(),
                "key1".as_bytes().to_vec(),
                (0u64).to_ne_bytes().to_vec(),
                "key3".len().to_ne_bytes().to_vec(),
                "key3".as_bytes().to_vec(),
                (get_page_size() as u64).to_ne_bytes().to_vec(),
                "キー4".len().to_ne_bytes().to_vec(),
                "キー4".as_bytes().to_vec(),
                (get_page_size() as u64 * 2 + 28).to_ne_bytes().to_vec(),
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
        
        // タイムスタンプ以外の部分を検証
        assert_eq!(&content, &expected_data);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn test_sst_writer_wirte_data_impl_deleted() {
        let timestamp = crate::utils::get_timestamp() as u64; // 実際のタイムスタンプ
        let mut memtable = MemTable::new();
        memtable.delete("key1", timestamp);
        let path = "/tmp/test_sst_writer_wirte_data_impl_deleted.sst";
        let mut file = File::create(path).unwrap();
        let data = SSTableData::try_from(memtable.encode()).unwrap();
        assert!(SSTableWriter::write_data_impl(&mut file, &data).is_ok());
    
        // バイナリデータを含むため、read_to_stringではなくreadを使用
        let content = fs::read(path).unwrap();
        
        // タイムスタンプを含むデータ形式に更新
        let mut expected_data = Vec::new();
        expected_data.extend_from_slice(&4u64.to_ne_bytes()); // key_len: 4
        expected_data.extend_from_slice("key1".as_bytes()); // key: "key1"
        expected_data.extend_from_slice(&1u64.to_ne_bytes()); // value_len: 1
        expected_data.extend_from_slice("\0".as_bytes()); // value: "\0"
        expected_data.extend_from_slice(&timestamp.to_ne_bytes()); // タイムスタンプ
        
        assert_eq!(expected_data, memtable.encode());
        
        assert_eq!(&content, &expected_data);
        
        fs::remove_file(path).unwrap();
    }
}