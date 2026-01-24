mod background_context;
mod collection;
mod compact;
mod constant;
mod database;
mod document;
mod error;
mod index;
mod memtable;
mod search;
mod sst;
mod utils;
mod wal;

pub use collection::{Collection, DistanceType, IndexConfig};
pub use compact::CompactionManager;
pub use database::AetherDB;
pub use document::Document;
pub use error::{CollectionError, WalError};
pub use index::{IndexManager, SSTEvent, SSTMetadata};
pub use search::SearchManager;
pub use sst::{Footer, IndexEntry, SSTManager};
pub use utils::*;
pub use wal::Operation;

#[cfg(test)]
mod test_utils;

#[cfg(test)]
mod tests;
