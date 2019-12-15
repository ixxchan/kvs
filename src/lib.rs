#![deny(missing_docs)]

//! A key-value store

use std::collections::HashMap;

/// The key-value database
///
/// # Examples
///
/// ```rust
/// # use kvs::KvStore;
/// let mut store = KvStore::new();
/// store.set("key1".to_owned(), "value1".to_owned());
/// assert_eq!(store.get("key1".to_owned()), Some("value1".to_owned()));
///
/// store.set("key1".to_owned(), "value2".to_owned());
/// assert_eq!(store.get("key1".to_owned()), Some("value2".to_owned()));
///
/// assert_eq!(store.get("key2".to_owned()), None);
///
/// store.remove("key1".to_owned());
/// assert_eq!(store.get("key1".to_owned()), None);
/// ```
#[derive(Default)]
pub struct KvStore {
    map: HashMap<String, String>,
}

impl KvStore {
    /// Creates an empty instance of the database
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use kvs::KvStore;
    /// let mut store = KvStore::new();
    /// ```
    pub fn new() -> KvStore {
        KvStore {
            map: HashMap::new(),
        }
    }

    /// Set the value of a string key to a string
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use kvs::KvStore;
    /// let mut store = KvStore::new();
    /// store.set("key".to_owned(), "value".to_owned());
    pub fn set(&mut self, key: String, value: String) {
        self.map.insert(key, value);
    }

    /// Get the string value of a given string key
    /// # Examples
    ///
    /// ```rust
    /// # use kvs::KvStore;
    /// let mut store = KvStore::new();
    /// store.set("key".to_owned(), "value".to_owned());
    /// assert_eq!(store.get("key".to_owned()), Some("value".to_owned()));
    /// assert_eq!(store.get("another_key".to_owned()), None)
    /// ```
    pub fn get(&self, key: String) -> Option<String> {
        self.map.get(&key).map(|s| s.to_string())
    }

    /// Remove a given key
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use kvs::KvStore;
    /// let mut store = KvStore::new();
    /// store.set("key".to_owned(), "value".to_owned());
    /// store.remove("key".to_owned());
    /// assert_eq!(store.get("key".to_owned()), None);
    /// ```
    pub fn remove(&mut self, key: String) {
        self.map.remove(&key);
    }
}
