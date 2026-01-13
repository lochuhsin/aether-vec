use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

use crate::collection::IndexType;
use crate::document::Document;

pub trait MemTable: Send + Sync {
    fn upsert(&mut self, _doc: Document) {
        panic!("Not implemented");
    }

    fn get(&self, _id: &Uuid) -> Option<Arc<Document>> {
        panic!("Not implemented");
    }

    fn search(&self, _vector: &Vec<f32>, _top_k: i32) -> Vec<Arc<Document>> {
        panic!("Not implemented");
    }

    fn delete(&mut self, _id: &Uuid) {
        panic!("Not implemented");
    }
}

struct FlatMemTable {
    table: HashMap<Uuid, Arc<Document>>,
}

impl Default for FlatMemTable {
    fn default() -> Self {
        FlatMemTable {
            table: HashMap::with_capacity(500),
        }
    }
}

impl MemTable for FlatMemTable {
    fn upsert(&mut self, doc: Document) {
        self.table.insert(doc.id, Arc::new(doc));
    }

    fn delete(&mut self, id: &Uuid) {
        self.table.remove(id);
    }

    fn get(&self, id: &Uuid) -> Option<Arc<Document>> {
        self.table.get(id).cloned()
    }

    fn search(&self, vector: &Vec<f32>, top_k: i32) -> Vec<Arc<Document>> {
        panic!("Not implemented");
    }
}

struct HNSWMemTable {}

impl MemTable for HNSWMemTable {}

impl Default for HNSWMemTable {
    fn default() -> Self {
        HNSWMemTable {}
    }
}

struct IVFMemTable {}

impl MemTable for IVFMemTable {}

impl Default for IVFMemTable {
    fn default() -> Self {
        IVFMemTable {}
    }
}

pub fn get_memtable(i_type: &IndexType) -> Box<dyn MemTable> {
    match i_type {
        IndexType::Flat => Box::new(FlatMemTable::default()),
        IndexType::HNSW => Box::new(HNSWMemTable::default()),
        IndexType::IVF => Box::new(IVFMemTable::default()),
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn test_flat_memtable() {}
}
