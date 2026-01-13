use crate::AetherDB;
use std::sync::Arc;

pub struct TestDb {
    pub db: Arc<AetherDB>,
    pub path: String,
}
impl TestDb {
    pub fn new(name: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let path = format!("./test_path/db/{}", name);
        std::fs::remove_dir_all(&path).ok();
        let db = AetherDB::new(&path)?;
        Ok(TestDb { db, path })
    }
}
impl Drop for TestDb {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.path).ok();
    }
}
