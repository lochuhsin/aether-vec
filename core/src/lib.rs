mod collection;
mod compact;
mod constant;
mod database;
mod document;
mod error;
mod memtable;
mod utils;
mod wal;

pub use collection::{Collection, IndexConfig};
pub use compact::CompactionManager;
pub use database::AetherDB;
pub use document::Document;
pub use error::{CollectionError, WalError};
pub use utils::*;
pub use wal::Operation;

#[cfg(test)]
mod test_utils;

#[cfg(test)]
mod tests;
