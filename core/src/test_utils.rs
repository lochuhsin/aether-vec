use crate::Document;
use rand::Rng;
use uuid::Uuid;

pub fn random_document(dim: usize) -> Document {
    let mut rng = rand::rng();
    Document {
        id: Uuid::new_v4(),
        vector: (0..dim).map(|_| rng.random_range(-1.0..1.0)).collect(),
        content: "test".to_string(),
    }
}

pub fn bulk_random_documents(dim: usize, count: usize) -> Vec<Document> {
    (0..count).map(|_| random_document(dim)).collect()
}
