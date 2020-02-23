use crate::Result;

pub use self::{kvs::KvStore, sled::SledKvsEngine};

pub trait KvsEngine {
    /// Set the value of a string key to a string
    fn set(&mut self, key: String, value: String) -> Result<()>;
    /// Get the string value of a given string key
    fn get(&mut self, key: String) -> Result<Option<String>>;
    /// Remove a given key
    fn remove(&mut self, key: String) -> Result<()>;
}

mod kvs;
mod sled;
