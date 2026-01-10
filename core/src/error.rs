use std::fmt;

#[derive(Debug)]
pub enum CollectionError {
    InvalidDimension(Option<String>),
    InvalidIndexType(Option<String>),
    InvalidDistanceType(Option<String>),
}

impl fmt::Display for CollectionError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            CollectionError::InvalidDimension(Some(msg)) => {
                write!(f, "Invalid dimension: {}", msg)
            }
            CollectionError::InvalidDimension(None) => {
                write!(f, "Invalid dimension")
            }
            CollectionError::InvalidIndexType(Some(msg)) => {
                write!(f, "Invalid index type: {}", msg)
            }
            CollectionError::InvalidIndexType(None) => {
                write!(f, "Invalid index type")
            }
            CollectionError::InvalidDistanceType(Some(msg)) => {
                write!(f, "Invalid distance type: {}", msg)
            }
            CollectionError::InvalidDistanceType(None) => {
                write!(f, "Invalid distance type")
            }
        }
    }
}

#[derive(Debug)]
pub enum DatabaseError {
    InvalidPath(Option<String>),
}

impl fmt::Display for DatabaseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            DatabaseError::InvalidPath(Some(msg)) => {
                write!(f, "Invalid path: {}", msg)
            }
            DatabaseError::InvalidPath(None) => {
                write!(f, "Invalid path")
            }
        }
    }
}
