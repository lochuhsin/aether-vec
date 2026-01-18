use std::{fmt, sync::PoisonError};

#[derive(Debug)]
pub enum CollectionError {
    InvalidDimension(Option<String>),
    InvalidIndexType(Option<String>),
    InvalidDistanceType(Option<String>),
    PoisonError(Option<String>),
    WalError(Option<String>),
    NotFound(Option<String>),
    InternalError(Option<String>),
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
            CollectionError::WalError(Some(msg)) => {
                write!(f, "Collection error from wal: {}", msg)
            }
            CollectionError::WalError(None) => {
                write!(f, "Collection error from wal")
            }
            CollectionError::NotFound(Some(msg)) => {
                write!(f, "Collection not found: {}", msg)
            }
            CollectionError::NotFound(None) => {
                write!(f, "Collection not found")
            }
            CollectionError::PoisonError(Some(msg)) => {
                write!(f, "Poison error: {}", msg)
            }
            CollectionError::PoisonError(None) => {
                write!(f, "Poison error")
            }
            CollectionError::InternalError(Some(msg)) => {
                write!(f, "Internal error: {}", msg)
            }
            CollectionError::InternalError(None) => {
                write!(f, "Internal error")
            }
        }
    }
}

impl std::error::Error for CollectionError {}

impl<T> From<PoisonError<T>> for CollectionError {
    fn from(err: PoisonError<T>) -> Self {
        CollectionError::PoisonError(Some(err.to_string()))
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

impl std::error::Error for DatabaseError {}

impl From<WalError> for CollectionError {
    fn from(err: WalError) -> Self {
        CollectionError::WalError(Some(err.to_string()))
    }
}

#[derive(Debug)]
pub enum WalError {
    InvalidOperation(Option<String>),
    WriteError(Option<String>),
}

impl fmt::Display for WalError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            WalError::InvalidOperation(Some(msg)) => {
                write!(f, "Invalid operation: {}", msg)
            }
            WalError::InvalidOperation(None) => {
                write!(f, "Invalid operation")
            }
            WalError::WriteError(Some(msg)) => {
                write!(f, "Write error: {}", msg)
            }
            WalError::WriteError(None) => {
                write!(f, "Write error")
            }
        }
    }
}

impl std::error::Error for WalError {}

impl From<std::io::Error> for WalError {
    fn from(err: std::io::Error) -> Self {
        WalError::WriteError(Some(err.to_string()))
    }
}
impl From<Box<bincode::ErrorKind>> for WalError {
    fn from(err: Box<bincode::ErrorKind>) -> Self {
        WalError::WriteError(Some(err.to_string()))
    }
}
