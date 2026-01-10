use crate::collection::Collection;
use crate::constant::MAX_DIMENSION;
use crate::error::{CollectionError, DatabaseError};
use fs2::FileExt;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock, Mutex, Weak};
static DATABASE_REGISTRY: LazyLock<Mutex<DatabaseRegistery>> =
    LazyLock::new(|| Mutex::new(DatabaseRegistery::new()));

pub struct AetherDB {
    path: PathBuf,
    _lock_file: File,
}

impl AetherDB {
    pub fn new(path: &str) -> Result<Arc<Self>, DatabaseError> {
        {
            // 獲取 Lock, 作用域結束後或自動釋放鎖, 因為 MutexGuard
            let registry = DATABASE_REGISTRY.lock().unwrap();
            if let Some(weak_ref) = registry.get(path) {
                if let Some(strong_ref) = weak_ref.upgrade() {
                    return Ok(strong_ref);
                }
            }
        }

        let pathbuf = PathBuf::from(path);
        let lock_file = is_valid_path(&pathbuf)?;
        let db = Arc::new(AetherDB {
            path: pathbuf,
            _lock_file: lock_file,
        });

        let mut registry = DATABASE_REGISTRY.lock().unwrap();
        registry.set(path, Arc::downgrade(&db));

        Ok(db)
    }

    pub fn create_collection(
        &self,
        index: &str,
        dimension: i32,
    ) -> Result<Collection, CollectionError> {
        if dimension > MAX_DIMENSION || dimension < 1 {
            return Err(CollectionError::InvalidDimension(Some(
                "Dimension must be between 1 and 65332".to_string(),
            )));
        }

        Ok(Collection::new(dimension, index.to_string()))
    }
}

pub fn is_valid_path(path: &PathBuf) -> Result<File, DatabaseError> {
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

    lock_file.try_lock_exclusive().map_err(|_| {
        DatabaseError::InvalidPath(Some("Database is locked by another process".to_string()))
    })?;

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
