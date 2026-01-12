use crate::collection::{Collection, CollectionManager, IndexConfig};
use crate::constant::MAX_DIMENSION;
use crate::error::{CollectionError, DatabaseError};
use crate::wal::WalManager;
use fs2::FileExt;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock, Mutex, RwLock, Weak};
static DATABASE_REGISTRY: LazyLock<Mutex<DatabaseRegistery>> =
    LazyLock::new(|| Mutex::new(DatabaseRegistery::new()));

pub struct AetherDB {
    path: PathBuf,
    collection_manager: CollectionManager,
    wal_manager: Arc<WalManager>,
    _lock_file: File, // process lock
}

impl AetherDB {
    pub fn new(path: &str) -> Result<Arc<Self>, DatabaseError> {
        {
            let registry = DATABASE_REGISTRY.lock().unwrap();
            if let Some(weak_ref) = registry.get(path) {
                if let Some(strong_ref) = weak_ref.upgrade() {
                    return Ok(strong_ref);
                }
            }
        }

        // init db
        let pathbuf = PathBuf::from(path);

        let lock_file = validate_path(&pathbuf)?;

        lock_file.try_lock_exclusive().map_err(|_| {
            DatabaseError::InvalidPath(Some("Database is locked by another process".to_string()))
        })?;

        let db = Arc::new(AetherDB {
            collection_manager: CollectionManager::new(),
            wal_manager: Arc::new(WalManager::new(&pathbuf)),
            _lock_file: lock_file,
            path: pathbuf,
        });

        let mut registry = DATABASE_REGISTRY.lock().unwrap();
        registry.set(path, Arc::downgrade(&db));

        Ok(db)
    }

    pub fn create_collection(
        &self,
        name: &str,
        dimension: i32,
        distance: &str,
        index_config: IndexConfig,
    ) -> Result<Arc<RwLock<Collection>>, CollectionError> {
        if dimension > MAX_DIMENSION || dimension < 1 {
            return Err(CollectionError::InvalidDimension(Some(
                "Dimension must be between 1 and 65332".to_string(),
            )));
        }

        let collection = Collection::new(
            name,
            dimension,
            distance,
            index_config,
            Arc::clone(&self.wal_manager),
        )?;
        Ok(self.collection_manager.create_collection(collection))
    }

    pub fn get_collection(&self, name: &str) -> Result<Arc<RwLock<Collection>>, CollectionError> {
        self.collection_manager
            .get_collection(name)
            .ok_or_else(|| CollectionError::NotFound(Some(name.to_string())))
    }

    pub fn list_collections(&self) -> Result<Vec<String>, CollectionError> {
        panic!("Not implemented");
    }

    pub fn delete_collection(&self, name: &str) -> Result<(), CollectionError> {
        panic!("Not implemented");
    }
}

pub fn validate_path(path: &PathBuf) -> Result<File, DatabaseError> {
    if !path.exists() {
        std::fs::create_dir_all(path).map_err(|e| {
            DatabaseError::InvalidPath(Some(format!("Cannot create directory: {}", e)))
        })?;
    } else if !Path::new(path).is_dir() {
        return Err(DatabaseError::InvalidPath(Some(
            "Path is not a directory".to_string(),
        )));
    }

    let lock_path = path.join(".lock");
    let lock_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(&lock_path)
        .map_err(|e| DatabaseError::InvalidPath(Some(format!("Cannot open lock file: {}", e))))?;

    Ok(lock_file)
}

struct DatabaseRegistery {
    registry: HashMap<String, Weak<AetherDB>>,
}

impl DatabaseRegistery {
    pub fn new() -> Self {
        DatabaseRegistery {
            registry: HashMap::new(),
        }
    }

    pub fn get(&self, key: &str) -> Option<&Weak<AetherDB>> {
        self.registry.get(key)
    }

    pub fn set(&mut self, key: &str, value: Weak<AetherDB>) {
        self.registry.insert(key.to_string(), value);
    }
}

#[cfg(test)]
mod tests {
    use crate::AetherDB;
    use std::sync::Arc;

    #[test]
    fn test_new_with_valid_path() {
        let test_path = "./test_db_valid";
        std::fs::remove_dir_all("./test_db_valid").ok();

        let result = AetherDB::new(test_path);

        assert!(result.is_ok(), "Should successfully create database");

        let db = result.unwrap();
        assert!(Arc::strong_count(&db) >= 1, "DB should be wrapped in Arc");

        std::fs::remove_dir_all(test_path).ok();
    }

    #[test]
    fn test_new_creates_directory_if_not_exists() {
        let test_path = "./test_db_nested/level1/level2";

        std::fs::remove_dir_all("./test_db_nested").ok();

        let result = AetherDB::new(test_path);
        assert!(result.is_ok());

        assert!(std::path::Path::new(test_path).exists());

        std::fs::remove_dir_all("./test_db_nested").ok();
    }
}
