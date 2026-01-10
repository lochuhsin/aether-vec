use core::AetherDB;
use core::IndexConfig;

fn main() {
    let db = AetherDB::new("./test").expect("Failed to create database");
    db.create_collection("abcde", 12345, "l2", IndexConfig::new_with_default())
        .expect("Failed to create collection");
    println!("Hello, world!");
}
