use uuid::Uuid;

use crate::document::Document;
use crate::error::CollectionError;
use crate::memtable::{MemTable, get_memtable};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::{Arc, RwLock};

pub enum IndexType {
    HNSW,
    IVF,
    Flat,
}

impl FromStr for IndexType {
    type Err = CollectionError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let lower = s.to_lowercase();
        match lower.as_str() {
            "hnsw" => Ok(IndexType::HNSW),
            "ivf" => Ok(IndexType::IVF),
            "flat" => Ok(IndexType::Flat),
            _ => Err(CollectionError::InvalidIndexType(Some(
                "Invalid index type".to_string(),
            ))),
        }
    }
}

pub struct IndexConfig {
    index: IndexType,
    pub params: HashMap<String, String>,
}

impl IndexConfig {
    pub fn new(index: &str, params: HashMap<String, String>) -> Result<Self, CollectionError> {
        let index_type = IndexType::from_str(index)?;
        Ok(IndexConfig {
            index: index_type,
            params,
        })
    }

    pub fn new_with_default_config(index: &str) -> Result<Self, CollectionError> {
        let index_type = IndexType::from_str(index)?;
        match index_type {
            IndexType::HNSW => {
                let mut default_params = HashMap::new();
                default_params.insert("m".to_string(), "16".to_string());
                default_params.insert("efConstruction".to_string(), "200".to_string());
                default_params.insert("efSearch".to_string(), "50".to_string());
                return Ok(IndexConfig {
                    index: index_type,
                    params: default_params,
                });
            }
            IndexType::IVF => {
                let mut default_params = HashMap::new();
                default_params.insert("nlist".to_string(), "1024".to_string());
                default_params.insert("quantization".to_string(), "PQ16".to_string());
                default_params.insert("nprobe".to_string(), "10".to_string());
                return Ok(IndexConfig {
                    index: index_type,
                    params: default_params,
                });
            }
            IndexType::Flat => {
                return Ok(IndexConfig {
                    index: index_type,
                    params: HashMap::new(),
                });
            }
        }
    }
}

impl Default for IndexConfig {
    fn default() -> Self {
        let mut default_params = HashMap::new();
        default_params.insert("m".to_string(), "16".to_string());
        default_params.insert("efConstruction".to_string(), "200".to_string());
        default_params.insert("efSearch".to_string(), "50".to_string());
        IndexConfig {
            index: IndexType::HNSW,
            params: default_params,
        }
    }
}
enum DistanceType {
    Cosine,
    L2,
    Dot,
}

impl FromStr for DistanceType {
    type Err = CollectionError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let lower = s.to_lowercase();
        match lower.as_str() {
            "cosine" => Ok(DistanceType::Cosine),
            "l2" => Ok(DistanceType::L2),
            "dot" => Ok(DistanceType::Dot),
            _ => Err(CollectionError::InvalidDistanceType(Some(
                "Invalid distance type".to_string(),
            ))),
        }
    }
}

pub struct Collection {
    name: String,

    dimension: i32,
    distance: DistanceType,
    index_config: IndexConfig,

    memtable: RwLock<Box<dyn MemTable>>,
    // wal_manager::
}

impl Collection {
    pub fn new(
        name: &str,
        dimension: i32,
        distance: &str,
        index_config: IndexConfig,
    ) -> Result<Self, CollectionError> {
        Ok(Collection {
            name: name.to_string(),
            dimension,
            distance: distance.parse()?,
            memtable: RwLock::new(get_memtable(&index_config.index)),
            index_config,
        })
    }
    pub fn upsert(&mut self, document: Document) -> Result<(), CollectionError> {
        if document.dimension() != self.dimension {
            Err(CollectionError::InvalidDimension(Some(
                "Dimension mismatch".to_string(),
            )))
        } else {
            let mut memtable = self.memtable.write().unwrap();
            memtable.insert(document);
            Ok(())
        }
    }

    pub fn search(&self, vector: &Vec<f32>) -> Option<Arc<Document>> {
        self.memtable.read().unwrap().search(vector)
    }

    pub fn fetch(&self, uuid: &Uuid) -> Option<Arc<Document>> {
        self.memtable.read().unwrap().get(uuid)
    }

    pub fn delete(&mut self, uuid: &Uuid) {
        self.memtable.write().unwrap().delete(uuid);
    }
}

pub struct CollectionManager {
    collections: RwLock<HashMap<String, Arc<RwLock<Collection>>>>,
}

impl CollectionManager {
    pub fn new() -> Self {
        CollectionManager {
            collections: RwLock::new(HashMap::new()),
        }
    }

    pub fn create_collection(&self, collection: Collection) -> Arc<RwLock<Collection>> {
        let name = collection.name.clone();
        let arc_collection = Arc::new(RwLock::new(collection));

        self.collections
            .write()
            .unwrap()
            .insert(name, arc_collection.clone());

        arc_collection
    }

    pub fn get_collection(&self, name: &str) -> Option<Arc<RwLock<Collection>>> {
        let map = self.collections.read().unwrap();
        map.get(name).cloned()
    }

    pub fn delete_collection(&mut self, name: &str) {
        self.collections.write().unwrap().remove(name);
    }
}
