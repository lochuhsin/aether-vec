use std::path::PathBuf;

pub struct WalManager {
    fpath: PathBuf,
}

impl WalManager {
    pub fn new(fpath: &PathBuf) -> Self {
        // create directory if not exists and create wal current wal file
        std::fs::create_dir_all(&fpath).unwrap();
        WalManager {
            fpath: fpath.join("wal"),
        }
    }
}
