use std::fs::{File, OpenOptions};
use std::path::PathBuf;

const TEMP_FILE: &str = "init.temp";

pub fn check_write_permission(path: &PathBuf) -> Result<File, std::io::Error> {
    let mut options = OpenOptions::new();
    options.write(true).create(true);
    options
        .open(format!("{}{}", path.as_path().to_str().unwrap(), TEMP_FILE))
        .map_err(|e| e.into())
}

pub fn check_read_permission(path: &PathBuf) -> Result<File, std::io::Error> {
    let mut options = OpenOptions::new();

    options.read(true);
    options.open(path).map_err(|e| e.into())
}

pub fn remove_temp_file(path: &PathBuf) -> Result<(), std::io::Error> {
    std::fs::remove_file(format!("{}{}", path.as_path().to_str().unwrap(), TEMP_FILE))
}
