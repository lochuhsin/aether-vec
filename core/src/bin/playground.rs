use core::AetherDB;
use core::IndexConfig;

fn main() {
    let db = AetherDB::new("./test_path/db/test").expect("Failed to create database");
    db.create_collection(
        "abcde",
        12345,
        "l2",
        IndexConfig::new_with_default_config("flat").unwrap(),
    )
    .expect("Failed to create collection");

    let x = 5;
    let y = x;
    println!("x: {}", x);
    println!("y: {}", y);
    println!("Hello, world!");
    db.get_collection("abcde")
        .expect("Failed to get collection");
}
