/**
 * This should be thread safe, as there are multiple threads
 * that will be updating the index.
 */
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::RwLock;

pub struct SSTEvent {
    pub metadata: SSTMetadata,
    // NOTE: we can add status later if needed, which handles any write fail
}

#[derive(Debug, Clone)]
pub struct SSTMetadata {
    pub collection_name: String,
    pub seq_no: u64,
    pub layer: u64,
    pub min_id: u128,
    pub max_id: u128,
    pub path: PathBuf,
    pub entry_count: u64,
}

// Optimize this lock, it is too wide
pub struct SSTIndex {
    layer_bucket: RwLock<Vec<Vec<Arc<SSTMetadata>>>>, // NOTE: We believe that the compactor will always send the seq_no in ascending order, so we can just append to the end of the vector
}

pub struct IndexManager {
    sst_index: SSTIndex,
}

impl IndexManager {
    pub fn new() -> Self {
        IndexManager {
            sst_index: SSTIndex {
                layer_bucket: RwLock::new(vec![Vec::with_capacity(10); 10]),
            },
        }
    }

    pub fn add_sst_metadata(&self, sst_metadata: SSTMetadata) {
        let layer = sst_metadata.layer;

        let mut bucket = self.sst_index.layer_bucket.write().unwrap();

        if bucket.len() <= (layer + 1) as usize {
            let bucket_len = bucket.len();
            bucket.resize(bucket_len * 2, Vec::new());
        }

        bucket[layer as usize].push(Arc::new(sst_metadata));
    }

    // NOTE: This is the downside of LSM Tree design
    // but we can optimize this by using a BTreeMap
    pub fn get_sst_metadata(&self, id: u128) -> Option<Arc<SSTMetadata>> {
        for bucket in self.sst_index.layer_bucket.read().unwrap().iter() {
            for sst_metadata in bucket.iter() {
                if sst_metadata.min_id <= id && sst_metadata.max_id >= id {
                    return Some(sst_metadata.clone());
                }
            }
        }
        None
    }
}

impl Default for IndexManager {
    fn default() -> Self {
        IndexManager::new()
    }
}
