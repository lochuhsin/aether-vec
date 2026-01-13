use bincode;
use std::fmt;
use std::io::{Cursor, Read, Write};
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

pub struct WalManager {
    fpath: PathBuf,
    name: String,
    seq_no: u64,
    file: File,
}

impl WalManager {
    pub fn new(fpath: &PathBuf, name: &str, seq_no: u64) -> Result<Self, WalError> {
        let fpath = fpath.join("wal");

        std::fs::create_dir_all(&fpath)?;
        let file = std::fs::File::create(fpath.join(format!("{}_{:09}.wal", name, seq_no)))?;
        Ok(WalManager {
            fpath: fpath,
            name: name.to_string(),
            seq_no,
            file: file,
        })
    }

    pub fn write(&mut self, op: Operation, data: &Document) -> Result<(), WalError> {
        let record_bytes = bincode::serialize(&(op, data))?;

        self.file.write_all(&record_bytes)?;

        // Flush to at least OS level, not fsync. This could do extreme optimization
        self.file.flush()?;
        Ok(())
    }

    pub fn read(&self) -> Result<Vec<(Operation, Document)>, WalError> {
        let mut records = Vec::new();
        let mut file = File::open(
            self.fpath
                .join(format!("{}_{:09}.wal", self.name, self.seq_no)),
        )?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;
        let mut cursor = Cursor::new(buffer);

        loop {
            match bincode::deserialize_from(&mut cursor) {
                Ok(record) => records.push(record),
                Err(_) => break,
            }
        }

        Ok(records)
    }
}
