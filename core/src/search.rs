use crate::DistanceType;
use crate::IndexManager;
use crate::document::Document;
use std::sync::Arc;

pub struct SearchManager {
    index_manager: Arc<IndexManager>,
    distance: DistanceType,
}

impl SearchManager {
    pub fn new(index_manager: Arc<IndexManager>, distance: DistanceType) -> Self {
        SearchManager {
            index_manager,
            distance,
        }
    }

    pub fn search(&self, query: &Vec<f32>, top_k: i32) -> Vec<Arc<Document>> {
        panic!("Not implemented");
    }

    pub fn fetch(&self, id: &u128) -> Option<Arc<Document>> {
        // 1. Search index
        // 2. If not found, search SSTs
        panic!("Not implemented");
    }
}
