//! This module provides pluggable storage engine trait and instances.

use crate::Result;

pub use self::kvs::KvStore;
pub use self::sled::SledKvsEngine;

/// Trait for a shared K-V store engine.
pub trait KvsEngine: Clone + Send + 'static {
    /// Sets the value of a string key to a string.
    fn set(&self, key: String, value: String) -> Result<()>;

    /// Gets the string value of a given string key.
    ///
    /// Returns `Ok(None)` if the key is not found.
    fn get(&self, key: String) -> Result<Option<String>>;

    /// Removes a given key.
    ///
    /// Returns error if the key is not found.
    fn remove(&self, key: String) -> Result<()>;
}

mod kvs;
mod sled;
