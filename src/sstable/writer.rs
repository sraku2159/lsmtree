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
        // let data = SSTableData::try_from(memtable.encode())?;
        Self::write_header_impl(&mut file, &index)?;
        Self::write_index_impl(&mut file, &index)?;
        Self::write_data_impl(&mut file, &memtable)
    }

    fn write_header_impl(file: &mut File, index: &SSTableIndex) -> Result<(), String> {
        let index_size = index.size() as u64;
        let header = SSTableHeader::new(
            SSTableHeader::SIZE as u64,
            index_size
        );
        let header = header.encode();
        file.write_all(&header).map_err(|e| e.to_string())
    }

    // コンパクションを考えると、データから作成する方が良い
    fn write_index_impl(file: &mut File, index: &SSTableIndex) -> Result<(), String> {
        let mut index = index.encode();
        file.write_all(&mut index).map_err(|e| e.to_string())
    }

    // ページサイズごとに書き込むという方法との比較を時間計算量の観点で今後したい
    fn write_data_impl(file: &mut File, data: &MemTable) -> Result<(), String> {
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
        let mut memtable = MemTable::new();
        memtable.put("key1", "value1");
        let path = "/tmp/test_sst_writer_wirte_impl.sst";
        assert!(SSTableWriter::write_impl(&memtable, path).is_ok());

        let content = fs::read(path).unwrap();
        let header = vec![
            16, 0, 0, 0, 0, 0, 0, 0,
            20, 0, 0, 0, 0, 0, 0, 0,
        ];
        let index = vec![
            4, 0, 0, 0, 0, 0, 0, 0,
            107, 101, 121, 49,
            36, 0, 0, 0, 0, 0, 0, 0,
        ];
        let data = vec![
            4, 0, 0, 0, 0, 0, 0, 0,
            107, 101, 121, 49,
            6, 0, 0, 0, 0, 0, 0, 0,
            118, 97, 108, 117, 101, 49,
        ];
        assert_eq!(
            content, 
            vec![
                header,
                index,
                data,
            ].concat()
        );
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn test_sst_writer_write_header_impl() {
        let mut index = SSTableIndex::new();
        index.insert("a".to_owned(), 0);
        index.insert("b".to_owned(), 3);
        index.insert("c".to_owned(), 1000);
        let data = SSTableData::try_from(vec![
            1, 0, 0, 0, 0, 0, 0, 0, // key_len: 1
            97, // key: "a"
            1, 0, 0, 0, 0, 0, 0, 0, // value_len: 1
            49, // value: "1"
        ]).unwrap();
        let path = "/tmp/test_sst_writer_write_header_impl.sst";
        let mut file = File::create(&path).unwrap();

        assert!(SSTableWriter::write_header_impl(&mut file, &index).is_ok());

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
        let mut memtable = MemTable::new();
        memtable.put("key1", "value1");
        let path = "/tmp/test_sst_writer_write_index_impl.sst";
        let mut file = File::create(path).unwrap();
        assert!(SSTableWriter::write_index_impl(&mut file, &SSTableIndex::from(&memtable)).is_ok());

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
        let mut memtable = MemTable::new();
        let header_size = SSTableHeader::SIZE as usize;
        let index_size = ("key1".len() + "key3".len() + "キー4".len()) + 16 * 3;

        memtable.put("key1", "value1"); // 8 + 4 + 8 + 6 = 26
        memtable.put("キー4", "c");
        memtable.put("key3", "b".repeat(get_page_size() as usize).as_str()); // 8 + 4 + 8 + page_size = page_size + 20
        memtable.put("key2", "a".repeat(get_page_size() as usize - 46).as_str()); // 8 + 4 + 8 + page_size - 46 = page_size - 26

        let path = "/tmp/test_fn_writer_write_index_impl_complex.sst";
        let mut file = File::create(path).unwrap();
        assert!(SSTableWriter::write_index_impl(&mut file, &SSTableIndex::from(&memtable)).is_ok());


        let content = fs::read(&path).unwrap();
        let entry1 = vec![
            vec![
                4, 0, 0, 0, 0, 0, 0, 0, // length of key1
                107, 101, 121, 49,
            ], // key1
            (header_size + index_size).to_ne_bytes().to_vec()
        ].concat();

        let entry2 = vec![
            4, 0, 0, 0, 0, 0, 0, 0,           // length of key3
            107, 101, 121, 51, // key3
        ].into_iter().chain(
            (get_page_size() + header_size + index_size).to_ne_bytes().into_iter()
        ).collect::<Vec<u8>>();

        let entry3 = vec![
            7, 0, 0, 0, 0, 0, 0, 0, // length of キー4
            0xe3, 0x82, 0xad, 0xe3, 0x83, 0xbc, 0x34, // キー4
        ].into_iter().chain(
            (get_page_size() * 2 + 20 + header_size + index_size).to_ne_bytes().into_iter()
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
        assert!(SSTableWriter::write_data_impl(&mut file, &memtable).is_ok());
    
        let content = fs::read_to_string(path).unwrap();
        assert_eq!(
            content, 
            "\u{4}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}key1\u{6}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}value1"
        );
        fs::remove_file(path).unwrap();
    }
}