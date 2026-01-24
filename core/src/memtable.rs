use std::collections::HashMap;
use std::sync::Arc;

use crate::collection::IndexType;
use crate::document::Document;

pub trait MemTable: Send + Sync {
    fn upsert(&mut self, _doc: Document) {
        panic!("Not implemented");
    }

    fn get(&self, _id: &u128) -> Option<Arc<Document>> {
        panic!("Not implemented");
    }

    fn search(&self, _vector: &Vec<f32>, _top_k: i32) -> Vec<Arc<Document>> {
        panic!("Not implemented");
    }

    fn delete(&mut self, _id: &u128) {
        panic!("Not implemented");
    }

    fn size(&self) -> usize {
        panic!("Not implemented");
    }

    fn sorted_iter(&self) -> Box<dyn Iterator<Item = Arc<Document>> + '_> {
        panic!("Not implemented")
    }
}

struct FlatMemTable {
    table: HashMap<u128, Arc<Document>>,
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

    fn delete(&mut self, id: &u128) {
        self.table.remove(id);
    }

    fn get(&self, id: &u128) -> Option<Arc<Document>> {
        self.table.get(id).cloned()
    }

    fn search(&self, vector: &Vec<f32>, top_k: i32) -> Vec<Arc<Document>> {
        panic!("Not implemented");
    }

    fn size(&self) -> usize {
        self.table.len()
    }

    fn sorted_iter(&self) -> Box<dyn Iterator<Item = Arc<Document>> + '_> {
        // 可能return 要做 Option 避免 unwrap
        let map = &self.table;
        let mut keys: Vec<u128> = map.keys().copied().collect();
        keys.sort_unstable();
        Box::new(keys.into_iter().map(|id| map.get(&id).unwrap().clone()))
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
