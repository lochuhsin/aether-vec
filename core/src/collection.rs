use uuid::Uuid;

use crate::compact::{BackgroundContext, CompactTask};
use crate::document::Document;
use crate::error::CollectionError;
use crate::memtable::{MemTable, get_memtable};
use crate::wal::Operation;
use crate::wal::WalManager;
use std::collections::{HashMap, VecDeque};
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
    memtable_size: usize,
    dimension: i32,
    distance: DistanceType,
    index_config: IndexConfig,
    memtable: RwLock<Box<dyn MemTable>>,
    wal_manager: WalManager,
    frozen_memtable_list: VecDeque<Arc<dyn MemTable>>, // Note: this might grow infinity. Need a bound on it, and force write both write to disk and stop accepting new write
    background_context: BackgroundContext,
}

impl Collection {
    pub fn new(
        name: &str,
        dimension: i32,
        distance: &str,
        index_config: IndexConfig,
        wal_manager: WalManager,
        memtable_size: usize,
        background_context: BackgroundContext,
    ) -> Result<Self, CollectionError> {
        Ok(Collection {
            name: name.to_string(),
            dimension: dimension,
            distance: distance.parse()?,
            memtable: RwLock::new(get_memtable(&index_config.index)),
            index_config: index_config,
            wal_manager: wal_manager,
            memtable_size: memtable_size,
            frozen_memtable_list: VecDeque::with_capacity(10),
            background_context: background_context,
        })
    }
    pub fn upsert(&mut self, document: Document) -> Result<(), CollectionError> {
        if document.dimension() != self.dimension {
            Err(CollectionError::InvalidDimension(Some(
                "Dimension mismatch".to_string(),
            )))
        } else {
            let mut memtable = self.memtable.write()?;

            self.wal_manager.write(Operation::Insert, &document)?;

            memtable.upsert(document);
            if memtable.size() >= self.memtable_size {
                let old_memtable =
                    std::mem::replace(&mut *memtable, get_memtable(&self.index_config.index));

                let arc_memtable: Arc<dyn MemTable> = Arc::from(old_memtable);

                self.frozen_memtable_list.push_back(arc_memtable.clone());
                self.wal_manager.rotate()?;

                self.background_context
                    .compact_task_sender
                    .send(CompactTask::new_default_layer(
                        self.name.clone(),
                        self.wal_manager.get_seq_no(),
                        arc_memtable.clone(),
                    ))
                    .map_err(|e| CollectionError::InternalError(Some(e.to_string())))?;
            }

            Ok(())
        }
    }

    pub fn search(&self, vector: &Vec<f32>, top_k: i32) -> Vec<Arc<Document>> {
        self.memtable.read().unwrap().search(vector, top_k)
        // Ideally we have a search manager to identify where and how to search.
        // Since the process is quite complex
        // TODO: search frozen memtables
        // TODO: search SSTs
    }

    pub fn fetch(&self, id: &u128) -> Option<Arc<Document>> {
        self.memtable.read().unwrap().get(id)
    }

    pub fn delete(&mut self, id: &u128) {
        self.memtable.write().unwrap().delete(id);
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
