use crate::document::Document;
use crate::memtable::MemTable;
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

const SST_MAGIC: u32 = 0x53535401; // "SST\x01"

const FOOTER_SIZE: usize = 64;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct IndexEntry {
    pub id: u128,
    pub offset: u64,
    pub length: u32,
}

/// Footer layout (64 bytes, fixed size, manual serialization):
/// - min_id:               16 bytes (u128 big-endian)
/// - max_id:               16 bytes (u128 big-endian)
/// - index_section_offset: 8 bytes  (u64 big-endian)
/// - index_section_size:   8 bytes  (u64 big-endian)
/// - entry_count:          8 bytes  (u64 big-endian)
/// - magic_number:         4 bytes  (u32 big-endian)
/// - version:              4 bytes  (u32 big-endian)
#[derive(Debug, Clone)]
pub struct Footer {
    pub min_id: u128,
    pub max_id: u128,
    pub index_section_offset: u64,
    pub index_section_size: u64,
    pub entry_count: u64,
    pub magic: u32,
    pub version: u32,
}

impl Footer {
    pub fn new(
        min_id: u128,
        max_id: u128,
        index_section_offset: u64,
        index_section_size: u64,
        entry_count: u64,
    ) -> Self {
        Self {
            min_id,
            max_id,
            index_section_offset,
            index_section_size,
            entry_count,
            magic: SST_MAGIC,
            version: 1,
        }
    }

    pub fn to_bytes(&self) -> [u8; FOOTER_SIZE] {
        let mut buf = [0u8; FOOTER_SIZE];
        buf[0..16].copy_from_slice(&self.min_id.to_be_bytes());
        buf[16..32].copy_from_slice(&self.max_id.to_be_bytes());
        buf[32..40].copy_from_slice(&self.index_section_offset.to_be_bytes());
        buf[40..48].copy_from_slice(&self.index_section_size.to_be_bytes());
        buf[48..56].copy_from_slice(&self.entry_count.to_be_bytes());
        buf[56..60].copy_from_slice(&self.magic.to_be_bytes());
        buf[60..64].copy_from_slice(&self.version.to_be_bytes());
        buf
    }

    pub fn from_bytes(buf: &[u8; FOOTER_SIZE]) -> Self {
        Self {
            min_id: u128::from_be_bytes(buf[0..16].try_into().unwrap()),
            max_id: u128::from_be_bytes(buf[16..32].try_into().unwrap()),
            index_section_offset: u64::from_be_bytes(buf[32..40].try_into().unwrap()),
            index_section_size: u64::from_be_bytes(buf[40..48].try_into().unwrap()),
            entry_count: u64::from_be_bytes(buf[48..56].try_into().unwrap()),
            magic: u32::from_be_bytes(buf[56..60].try_into().unwrap()),
            version: u32::from_be_bytes(buf[60..64].try_into().unwrap()),
        }
    }
}

#[derive(Debug)]
pub enum SSTError {
    Io(std::io::Error),
    InvalidMagic,
    NotFound,
    DeserializeError(String),
}

impl From<std::io::Error> for SSTError {
    fn from(e: std::io::Error) -> Self {
        SSTError::Io(e)
    }
}

pub struct SSTManager {
    pub path: PathBuf,
}

impl SSTManager {
    pub fn new(path: PathBuf) -> Self {
        let path = path.join("data");
        Self { path }
    }

    pub fn write_memtable(
        &self,
        collection_name: &str,
        seq_no: u64,
        layer: u64,
        memtable: &dyn MemTable,
    ) -> std::io::Result<PathBuf> {
        // fp: root/{collection}/L{layer}/{seq_no}.sst
        let dir_path = self.path.join(collection_name).join(format!("L{}", layer));
        fs::create_dir_all(&dir_path)?;

        let fname = format!("{:06}.sst", seq_no);
        let fpath = dir_path.join(&fname);

        let file = File::create(&fpath)?;
        let mut writer = BufWriter::new(file);

        let mut data_section = Vec::new();
        let mut index_entries: Vec<IndexEntry> = Vec::with_capacity(memtable.size());
        let mut min_id = u128::MAX;
        let mut max_id = u128::MIN;

        // data section
        for doc in memtable.sorted_iter() {
            if doc.id < min_id {
                min_id = doc.id;
            }
            if doc.id > max_id {
                max_id = doc.id;
            }

            let current_offset = data_section.len() as u64;
            let serialized = doc.serialize().expect("Failed to serialize document");
            let length = serialized.len() as u32;

            index_entries.push(IndexEntry {
                id: doc.id,
                offset: current_offset,
                length,
            });

            data_section.extend(serialized);
        }

        writer.write_all(&data_section)?;
        let data_section_size = data_section.len() as u64;

        // index section
        let index_section_offset = data_section_size;
        let index_bytes =
            bincode::serialize(&index_entries).expect("Failed to serialize index entries");
        writer.write_all(&index_bytes)?;
        let index_section_size = index_bytes.len() as u64;

        // footer section
        let entry_count = index_entries.len() as u64;
        let footer = Footer::new(
            min_id,
            max_id,
            index_section_offset,
            index_section_size,
            entry_count,
        );
        writer.write_all(&footer.to_bytes())?;
        writer.flush()?;

        Ok(fpath)
    }

    pub fn read(
        &self,
        collection_name: &str,
        seq_no: u64,
        layer: u64,
        id: u128,
    ) -> Result<Document, SSTError> {
        let dir_path = self.path.join(collection_name).join(format!("L{}", layer));

        let fname = format!("{:06}.sst", seq_no);
        let file = File::open(dir_path.join(&fname))?;
        let _file_size = file.metadata()?.len();
        let mut reader = BufReader::new(file);

        // 1. read footer (last 64 bytes)
        reader.seek(SeekFrom::End(-(FOOTER_SIZE as i64)))?;
        let mut footer_buf = [0u8; FOOTER_SIZE];
        reader.read_exact(&mut footer_buf)?;
        let footer = Footer::from_bytes(&footer_buf);

        if footer.magic != SST_MAGIC {
            return Err(SSTError::InvalidMagic);
        }

        if id < footer.min_id || id > footer.max_id {
            return Err(SSTError::NotFound);
        }

        // 2. read index section
        reader.seek(SeekFrom::Start(footer.index_section_offset))?;
        let mut index_bytes = vec![0u8; footer.index_section_size as usize];
        reader.read_exact(&mut index_bytes)?;

        let index_entries: Vec<IndexEntry> = bincode::deserialize(&index_bytes)
            .map_err(|e| SSTError::DeserializeError(e.to_string()))?;

        // 3. binary search for id (index is sorted by id)
        let entry = index_entries
            .binary_search_by_key(&id, |e| e.id)
            .ok()
            .map(|idx| &index_entries[idx]);

        match entry {
            Some(index_entry) => {
                // 4. seek to data offset and read document
                reader.seek(SeekFrom::Start(index_entry.offset))?;
                let mut doc_bytes = vec![0u8; index_entry.length as usize];
                reader.read_exact(&mut doc_bytes)?;

                // 5. deserialize document
                let doc: Document = bincode::deserialize(&doc_bytes)
                    .map_err(|e| SSTError::DeserializeError(e.to_string()))?;

                Ok(doc)
            }
            None => Err(SSTError::NotFound),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collection::IndexType;
    use crate::memtable::get_memtable;
    use crate::test_utils::bulk_random_documents;
    use std::collections::HashMap;
    use tempfile::tempdir;

    #[test]
    fn test_sst_write_and_read_all_documents() {
        let dir = tempdir().expect("Failed to create temp dir");
        let sst_manager = SSTManager::new(dir.path().to_path_buf());

        let collection_name = "test_collection";
        let seq_no = 1;
        let layer = 0;

        let docs = bulk_random_documents(128, 100);

        let mut expected: HashMap<u128, (Vec<f32>, String)> = HashMap::new();
        for doc in &docs {
            expected.insert(doc.id, (doc.vector.clone(), doc.content.clone()));
        }

        let mut memtable = get_memtable(&IndexType::Flat);
        for doc in docs {
            memtable.upsert(doc);
        }

        sst_manager
            .write_memtable(collection_name, seq_no, layer, memtable.as_ref())
            .expect("Failed to write SST");

        for (id, (expected_vector, expected_content)) in expected.iter() {
            let doc = sst_manager
                .read(collection_name, seq_no, layer, *id)
                .expect(&format!("Failed to read document with id {}", id));

            assert_eq!(doc.id, *id, "ID mismatch");
            assert_eq!(
                doc.vector, *expected_vector,
                "Vector mismatch for id {}",
                id
            );
            assert_eq!(
                doc.content, *expected_content,
                "Content mismatch for id {}",
                id
            );
        }
    }

    #[test]
    fn test_sst_read_not_found() {
        let dir = tempdir().expect("Failed to create temp dir");
        let sst_manager = SSTManager::new(dir.path().to_path_buf());

        let collection_name = "test_collection";
        let seq_no = 1;
        let layer = 0;

        let docs = bulk_random_documents(64, 10);
        let mut memtable = get_memtable(&IndexType::Flat);
        for doc in docs {
            memtable.upsert(doc);
        }

        sst_manager
            .write_memtable(collection_name, seq_no, layer, memtable.as_ref())
            .expect("Failed to write SST");

        // Try to read a non-existent ID
        let result = sst_manager.read(collection_name, seq_no, layer, 12345678901234567890);
        assert!(matches!(result, Err(SSTError::NotFound)));
    }
}
