use std::sync::Arc;
use uuid::Uuid;

use crate::collection::IndexType;
use crate::document::Document;

pub trait MemTable: Send + Sync {
    fn insert(&mut self, doc: Document) {
        panic!("Not implemented");
    }

    fn delete(&mut self, id: &Uuid) {
        panic!("Not implemented");
    }

    fn get(&self, id: &Uuid) -> Option<Arc<Document>> {
        panic!("Not implemented");
    }

    fn search(&self, vector: &Vec<f32>) -> Option<Arc<Document>> {
        panic!("Not implemented");
    }
}

struct FlatMemTable {}

impl MemTable for FlatMemTable {}

struct HNSWMemTable {}

impl MemTable for HNSWMemTable {}

struct IVFMemTable {}

impl MemTable for IVFMemTable {}

pub fn get_memtable(i_type: &IndexType) -> Box<dyn MemTable> {
    match i_type {
        IndexType::Flat => Box::new(FlatMemTable {}),
        IndexType::HNSW => Box::new(HNSWMemTable {}),
        IndexType::IVF => Box::new(IVFMemTable {}),
    }
}
