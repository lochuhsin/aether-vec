pub struct WalManager {
    fpath: String,
}

impl WalManager {
    pub fn new(fpath: &str) -> Self {
        WalManager {
            fpath: fpath.to_string(),
        }
    }
}
