use crate::document::Document;
use crate::error::CollectionError;
pub struct Collection {
    dimension: i32,
    index: String,
}

impl Collection {
    pub fn new(dimension: i32, index: String) -> Self {
        Collection { dimension, index }
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
