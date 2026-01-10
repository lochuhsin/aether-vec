use core::AetherDB;

fn main() {
    let db = AetherDB::new("./test").expect("Failed to create database");
    db.create_collection("abcde", 12345)
        .expect("Failed to create collection");
    println!("Hello, world!");
}
