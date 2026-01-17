use bincode;
use std::fmt;
use std::io::{BufReader, BufWriter, Write};
use std::path::PathBuf;

use crate::Document;
use crate::WalError;
use serde::{Deserialize, Serialize};
use std::fs::File;

#[derive(Debug, Serialize, Deserialize)]
pub enum Operation {
    Insert,
    Delete,
}

impl fmt::Display for Operation {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Operation::Insert => write!(f, "insert"),
            Operation::Delete => write!(f, "delete"),
        }
    }
}

const INITIAL_SEQ_NO: u64 = 0;

pub struct WalManager {
    fpath: PathBuf,
    name: String,
    seq_no: u64,
    file: BufWriter<File>,
}

impl WalManager {
    pub fn new(fpath: &PathBuf, name: &str) -> Result<Self, WalError> {
        let fpath = fpath.join("wal");

        // TODO: implement WAL recovery mechanism

        std::fs::create_dir_all(&fpath)?;
        let file =
            std::fs::File::create(fpath.join(format!("{}_{:09}.wal", name, INITIAL_SEQ_NO)))?;
        Ok(WalManager {
            fpath: fpath,
            name: name.to_string(),
            seq_no: INITIAL_SEQ_NO,
            file: BufWriter::with_capacity(65536, file),
        })
    }

    pub fn write(&mut self, op: Operation, data: &Document) -> Result<(), WalError> {
        bincode::serialize_into(&mut self.file, &(op, data))?;

        // Flush to at least OS level
        self.file.flush()?;
        // Ensure data is on disk
        self.file.get_ref().sync_data()?;
        Ok(())
    }

    pub fn read(&self) -> Result<Vec<(Operation, Document)>, WalError> {
        let mut records = Vec::new();
        let file = File::open(
            self.fpath
                .join(format!("{}_{:09}.wal", self.name, self.seq_no)),
        )?;
        let mut reader = BufReader::new(file);

        loop {
            match bincode::deserialize_from(&mut reader) {
                Ok(record) => records.push(record),
                Err(_) => break,
            }
        }

        Ok(records)
    }

    pub fn rotate(&mut self) -> Result<(), WalError> {
        self.file.flush()?;
        self.file.get_ref().sync_data()?;

        self.seq_no = self.seq_no + 1;
        let file = std::fs::File::create(
            self.fpath
                .join(format!("{}_{:09}.wal", self.name, self.seq_no)),
        )?;

        self.file = BufWriter::with_capacity(65536, file);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn get_test_path(name: &str) -> PathBuf {
        let path = PathBuf::from(name);
        if path.exists() {
            let _ = std::fs::remove_dir_all(&path);
        }
        path
    }

    fn cleanup(path: &PathBuf) {
        if path.exists() {
            let _ = std::fs::remove_dir_all(path);
        }
    }

    #[test]
    fn test_write_and_read() {
        let path = get_test_path("./test_wal_rw");

        {
            let mut wal = WalManager::new(&path, "test").unwrap();
            let doc1 = Document::new(vec![1.0, 2.0], "doc1".to_string());
            let doc2 = Document::new(vec![3.0, 4.0], "doc2".to_string());

            wal.write(Operation::Insert, &doc1).unwrap();
            wal.write(Operation::Delete, &doc2).unwrap();

            let records = wal.read().unwrap();
            assert_eq!(records.len(), 2);

            match &records[0] {
                (Operation::Insert, d) => assert_eq!(d.content, "doc1"),
                _ => panic!("Expected Insert"),
            }

            match &records[1] {
                (Operation::Delete, d) => assert_eq!(d.content, "doc2"),
                _ => panic!("Expected Delete"),
            }
        }

        cleanup(&path);
    }

    #[test]
    fn test_rotate() {
        let path = get_test_path("./test_wal_rotate");

        {
            let mut wal = WalManager::new(&path, "test_rotate").unwrap();

            let doc1 = Document::new(vec![1.0], "doc1".to_string());
            wal.write(Operation::Insert, &doc1).unwrap();

            wal.rotate().unwrap();

            let doc2 = Document::new(vec![2.0], "doc2".to_string());
            wal.write(Operation::Insert, &doc2).unwrap();

            // After rotate, read() should read the new file (seq_no=1)
            let records = wal.read().unwrap();
            assert_eq!(records.len(), 1);
            assert_eq!(records[0].1.content, "doc2");
        }

        cleanup(&path);
    }

    #[test]
    fn test_performance() {
        let path = get_test_path("./test_wal_perf");
        let count = 100;

        {
            let mut wal = WalManager::new(&path, "test_perf").unwrap();
            let doc = Document::new(vec![1.0, 2.0], "data".to_string());

            let start = std::time::Instant::now();
            for _ in 0..count {
                wal.write(Operation::Insert, &doc).unwrap();
            }
            let duration = start.elapsed();
            println!(
                "Write {} ops in {:?}, {:.2} ops/sec",
                count,
                duration,
                count as f64 / duration.as_secs_f64()
            );
        }

        cleanup(&path);
    }

    #[test]
    fn test_large_write() {
        let path = get_test_path("./test_wal_large");

        {
            let mut wal = WalManager::new(&path, "test_large").unwrap();
            // 70KB data to exceed 64KB buffer
            let large_data = vec![1.0; 17500];
            let doc = Document::new(large_data.clone(), "large_doc".to_string());

            wal.write(Operation::Insert, &doc).unwrap();

            let records = wal.read().unwrap();
            assert_eq!(records.len(), 1);
            match &records[0] {
                (Operation::Insert, d) => {
                    assert_eq!(d.vector.len(), 17500);
                    assert_eq!(d.content, "large_doc");
                }
                _ => panic!("Expected Insert"),
            }
        }
        cleanup(&path);
    }
}
