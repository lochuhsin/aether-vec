use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Serialize, Deserialize)]
pub struct Document {
    pub id: u128,
    pub vector: Vec<f32>,
    pub content: String,
}

impl Document {
    pub fn new(vector: Vec<f32>, content: String) -> Self {
        Document {
            id: Uuid::new_v4().as_u128(),
            vector,
            content,
        }
    }

    pub fn dimension(&self) -> i32 {
        self.vector.len() as i32
    }

    pub fn serialize(&self) -> Result<Vec<u8>, bincode::Error> {
        bincode::serialize(self)
    }

    pub fn deserialize(data: Vec<u8>) -> Result<Self, bincode::Error> {
        bincode::deserialize(&data)
    }
}
