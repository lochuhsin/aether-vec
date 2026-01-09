use std::fmt;

#[derive(Debug)]
pub enum CollectionError {
    InvalidDimension(Option<String>),
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
        }
    }
}
