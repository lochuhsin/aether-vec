use uuid::Uuid;

use crate::AetherDB;
use crate::IndexConfig;
use crate::test_utils::random_document;
use crate::tests::utils::TestDb;

#[test]
fn test_add_documents() -> Result<(), Box<dyn std::error::Error>> {
    let test_dimension = 12345;
    let test_document = random_document(test_dimension as usize);
    let test_path = "./test_path/db/test_add";

    let test_db = TestDb::new("test_add").unwrap();

    let collection = test_db.db.create_collection(
        "abcde",
        test_dimension,
        "l2",
        IndexConfig::new_with_default_config("flat")?,
    )?;

    // 需要獲取寫鎖才能調用 upsert
    collection.write().unwrap().upsert(test_document)?;

    Ok(())
}

#[test]
fn test_fetch_documents() -> Result<(), Box<dyn std::error::Error>> {
    let test_dimension = 12345;
    let test_document = random_document(test_dimension as usize);
    let test_id = test_document.id;

    let test_db = TestDb::new("test_fetch").unwrap();

    let collection = test_db.db.create_collection(
        "abcde",
        test_dimension,
        "l2",
        IndexConfig::new_with_default_config("flat")?,
    )?;

    collection.write().unwrap().upsert(test_document)?;
    if collection.read().unwrap().fetch(&test_id).is_none() {
        return Err("Document not found".into());
    }

    if collection.write().unwrap().fetch(&Uuid::new_v4()).is_some() {
        return Err("Document should not be found".into());
    }

    Ok(())
}
