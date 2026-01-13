use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Serialize, Deserialize)]
pub struct Document {
    pub id: Uuid,
    pub vector: Vec<f32>,
    pub content: String,
}

impl Document {
    pub fn new(vector: Vec<f32>, content: String) -> Self {
        Document {
            id: Uuid::new_v4(),
            vector,
            content,
        }
    }

    pub fn dimension(&self) -> i32 {
        self.vector.len() as i32
    }
}
