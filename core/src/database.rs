use crate::collection::Collection;
use crate::constant::MAX_DIMENSION;
use crate::error::CollectionError;

pub struct AetherDB {
    path: String,
}

impl AetherDB {
    pub fn new(path: &str) -> Self {
        AetherDB {
            path: path.to_string(),
        }
    }

    pub fn create_collection(
        &self,
        index: &str,
        dimension: i32,
    ) -> Result<Collection, CollectionError> {
        if dimension > MAX_DIMENSION || dimension < 1 {
            return Err(CollectionError::InvalidDimension(Some(
                "Dimension must be between 1 and 65332".to_string(),
            )));
        }

        Ok(Collection::new(dimension, index.to_string()))
    }
}
