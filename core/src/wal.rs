use bincode;
use std::fmt;
use std::io::Write;
use std::path::PathBuf;
use uuid::Uuid;

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
    uuid: Uuid,
    file: File,
}

impl WalManager {
    pub fn new(fpath: &PathBuf, uuid: Uuid) -> Result<Self, WalError> {
        let fpath = fpath.join("wal");

        std::fs::create_dir_all(&fpath)?;
        let file = std::fs::File::create(fpath.join(format!("{}.wal", uuid)))?;
        Ok(WalManager {
            fpath: fpath,
            uuid: uuid,
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
}
