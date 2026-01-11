use crate::AetherDB;
use crate::collection::IndexConfig;

#[test]
fn test_create_collection() {
    let test_path = "./test_collection";
    let test_collection = "test_collection";

    std::fs::remove_dir_all(test_path).ok();

    let db = AetherDB::new(test_path).unwrap();
    db.create_collection(test_collection, 128, "cosine", IndexConfig::default())
        .unwrap();

    db.get_collection(test_collection)
        .expect("Collection should exist");
    std::fs::remove_dir_all(test_path).ok();
}
