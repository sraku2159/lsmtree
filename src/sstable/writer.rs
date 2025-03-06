use std::{fs::File, io::Write};

use crate::{memtable::MemTable, utils};

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

    pub fn write(&self, memtable: &MemTable) -> Result<(), String> {
        Self::write_impl(memtable, &self.file)
    }

    fn write_impl(memtable: &MemTable, file: &str) -> Result<(), String> {
        let mut file = File::create(file).map_err(|e| e.to_string())?;
        let index = SSTableIndex::from(memtable);
        let mut data = memtable.encode();
        Self::write_header_impl(&mut file, &index, &data)?;
        Self::write_index_impl(&mut file, &index)?;
        Self::write_data_impl(&mut file, &mut data)
    }

    // コンパクションを考えると、データから作成する方が良い
    fn write_index_impl(file: &mut File, index: &SSTableIndex) -> Result<(), String> {
        let mut index = index.encode();
        file.write_all(&mut index).map_err(|e| e.to_string())
    }

    fn write_header_impl(file: &mut File, index: &SSTableIndex, data: &SSTableData) -> Result<(), String> {
        let data_size = data.len() as u64;
        let index_size = index.size() as u64;
        let header = SSTableHeader::new(
            data_size,
            index_size
        );
        let header = header.encode();
        file.write_all(&header).map_err(|e| e.to_string())
    }

    fn write_data_impl(file: &mut File, data: &mut Vec<u8>) -> Result<(), String> {
        file.write_all(data).map_err(|e| e.to_string())
    }
}

#[cfg(all(test, target_endian = "little"))]
mod tests {
    use std::fs::{self, remove_file, File};

    use crate::{memtable::MemTable, sstable::{writer::SSTableWriter, SSTableData, SSTableIndex}, utils::get_page_size};

    #[test]
    fn test_sst_writer_wirte_impl() {
        let mut memtable = MemTable::new();
        memtable.put("key1", "value1");
        let path = "/tmp/test_sst_writer_wirte_impl.sst";
        assert!(SSTableWriter::write_impl(&memtable, path).is_ok());

        let content = fs::read_to_string(path).unwrap();
        let header = "\
            \u{1a}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\
            \u{14}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\
        ";
        let index = "\
            \u{4}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\
            key1\
            \u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\
        ";
        let data = "\
            \u{4}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\
            key1\
            \u{6}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\
            value1";
        assert_eq!(
            content, 
            header.to_owned() + &index.to_owned() + &data.to_owned()
        );
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn test_sst_writer_write_header_impl() {
        let mut index = SSTableIndex::new();
        index.insert("a".to_owned(), 0);
        index.insert("b".to_owned(), 3);
        index.insert("c".to_owned(), 1000);
        let mut data = SSTableData::new();
        data.extend_from_slice(&[1, 2, 3, 4]);
        let path = "/tmp/test_sst_writer_write_header_impl.sst";
        let mut file = File::create(&path).unwrap();

        assert!(SSTableWriter::write_header_impl(&mut file, &index, &data).is_ok());

        let index_size = index.size() as u64;
        let data_size = data.len() as u64;
        let content = fs::read(&path).unwrap();
        assert_eq!(
            content,
            vec![
                data_size.to_ne_bytes().to_vec(),
                index_size.to_ne_bytes().to_vec(),
            ].concat()
        );
        remove_file(&path).unwrap();
    }

    #[test]
    fn test_fn_writer_write_index_impl() {
        let mut memtable = MemTable::new();
        memtable.put("key1", "value1");
        let path = "/tmp/test_sst_writer_write_index_impl.sst";
        let mut file = File::create(path).unwrap();
        assert!(SSTableWriter::write_index_impl(&mut file, &SSTableIndex::from(&memtable)).is_ok());

        let content = fs::read_to_string(path).unwrap();
        assert_eq!(
            content,
            "\u{4}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\
            key1\
            \u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}"
        );
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn test_fn_writer_write_index_impl_complex() {
        let mut memtable = MemTable::new();
        memtable.put("key1", "value1"); // 8 + 4 + 8 + 6 = 26
        memtable.put("キー4", "c"); 
        memtable.put("key3", "b".repeat(get_page_size() as usize).as_str());
        memtable.put("key2", "a".repeat(get_page_size() as usize - 46).as_str()); // 8 + 4 + 8 + page_size - 46 = page_size - 26
        let path = "/tmp/test_fn_writer_write_index_impl_complex.sst";
        let mut file = File::create(path).unwrap();
        assert!(SSTableWriter::write_index_impl(&mut file, &SSTableIndex::from(&memtable)).is_ok());

        let content = fs::read(&path).unwrap();
        let entry1 = vec![
            4, 0, 0, 0, 0, 0, 0, 0, // length of key1
            107, 101, 121, 49, // key1
            0, 0, 0, 0, 0, 0, 0, 0, // offset of key1
        ];
        let entry2 = vec![
            4, 0, 0, 0, 0, 0, 0, 0,           // length of key3
            107, 101, 121, 51, // key3
        ].into_iter().chain(
            get_page_size().to_ne_bytes().into_iter()
        ).collect::<Vec<u8>>();

        let entry3 = vec![
            7, 0, 0, 0, 0, 0, 0, 0, // length of キー4
            0xe3, 0x82, 0xad, 0xe3, 0x83, 0xbc, 0x34, // キー4
        ].into_iter().chain(
            (get_page_size() * 2 + 20).to_ne_bytes().into_iter()
        ).collect::<Vec<u8>>();

        assert_eq!(
            content,
            vec![
                entry1,
                entry2,
                entry3,
            ].concat()
        );
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn test_sst_writer_wirte_data_impl() {
        let mut memtable = MemTable::new();
        memtable.put("key1", "value1");
        let path = "/tmp/test_sst_writer_wirte_data_impl.sst";
        let mut file = File::create(path).unwrap();
        let mut data = memtable.encode();
        assert!(SSTableWriter::write_data_impl(&mut file, &mut data).is_ok());
        
        let content = fs::read_to_string(path).unwrap();
        assert_eq!(
            content, 
            "\u{4}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}key1\u{6}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}value1"
        );
        fs::remove_file(path).unwrap();
    }
}