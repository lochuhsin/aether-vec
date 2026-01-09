use core::AetherDB;

fn main() {
    let db = AetherDB::new();
    db.create_collection("abcde", 12345);
    println!("Hello, world!");
}
