use std::{fs::File, io::Write};

use crate::{memtable::MemTable, utils};

use super::{SSTableData, SSTableIndex};


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
        let mut data = memtable.encode();
        Self::write_data_impl(&mut file, &mut data)
    }

    fn write_index_impl(file: &File, index: &SSTableIndex) -> Result<(), String> {
        unimplemented!();
    }

    fn write_header_impl(file: &File, index: &SSTableIndex, data: &SSTableData) -> Result<(), String> {
        unimplemented!();
    }

    fn write_data_impl(file: &mut File, data: &mut Vec<u8>) -> Result<(), String> {
        file.write_all(data).map_err(|e| e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use std::fs::{self, remove_file, File};

    use crate::{memtable::MemTable, sstable::{writer::SSTableWriter, SSTableData, SSTableIndex}};

    #[test]
    fn test_sst_writer_wirte_impl() {
        let mut memtable = MemTable::new();
        memtable.put("key1", "value1");
        let path = "/tmp/test_sst_writer_wirte_impl.sst";
        assert!(SSTableWriter::write_impl(&memtable, path).is_ok());
        
        let content = fs::read_to_string(path).unwrap();
        assert_eq!(
            content, 
            "\u{4}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}key1\u{6}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}value1"
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
        let file = File::create(&path).unwrap();

        assert!(SSTableWriter::write_header_impl(&file, &index, &data).is_ok());

        let content = fs::read(&path).unwrap();
        assert_eq!(
            content,
            vec![
                // TODO
            ]
        );
        remove_file(&path).unwrap();
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