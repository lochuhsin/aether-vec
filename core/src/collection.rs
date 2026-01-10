use crate::document::Document;
use crate::error::CollectionError;
use std::collections::HashMap;
use std::str::FromStr;

enum IndexType {
    HNSM,
    IVF,
    Flat,
}

impl FromStr for IndexType {
    type Err = CollectionError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let lower = s.to_lowercase();
        match lower.as_str() {
            "hnsm" => Ok(IndexType::HNSM),
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

    pub fn new_with_default() -> Self {
        let mut default_params = HashMap::new();
        default_params.insert("m".to_string(), "16".to_string());
        default_params.insert("efConstruction".to_string(), "200".to_string());
        default_params.insert("efSearch".to_string(), "50".to_string());
        IndexConfig {
            index: IndexType::HNSM,
            params: default_params,
        }
    }

    pub fn new_with_default_config(index: &str) -> Result<Self, CollectionError> {
        let index_type = IndexType::from_str(index)?;
        match index_type {
            IndexType::HNSM => {
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
            index_config,
        })
    }
    pub fn upsert(&self, document: Document) -> Result<(), CollectionError> {
        if document.dimension() != self.dimension {
            Err(CollectionError::InvalidDimension(Some(
                "Dimension mismatch".to_string(),
            )))
        } else {
            Ok(())
        }
    }

    pub fn search(&self, vector: Vec<f32>) {}

    pub fn fetch(&self, uuid: &str) {}

    pub fn delete(&self, uuid: &str) {}
}

// 需要 Thread Safe
pub struct CollectionManager {
    collections: HashMap<String, Collection>,
}

impl CollectionManager {
    pub fn new() -> Self {
        CollectionManager {
            collections: HashMap::new(),
        }
    }

    pub fn create_collection(&mut self, collection: Collection) {
        self.collections.insert(collection.name.clone(), collection);
    }

    pub fn get_collection(&self, name: &str) -> Option<&Collection> {
        self.collections.get(name)
    }

    pub fn delete_collection(&mut self, name: &str) {
        self.collections.remove(name);
    }
}
