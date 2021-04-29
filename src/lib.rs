#![deny(missing_docs)]

//! A simple key-value store that supports get, set and remove operations

use std::collections::HashMap;

/// Implementation of key-value store
pub struct KvStore {
    hm: HashMap<String, String>,
}

impl KvStore {
    /// Returns a new empty `KvStore`
    pub fn new() -> KvStore {
        return KvStore { hm: HashMap::new() };
    }
}

impl KvStore {
    /// Set the value of a key, overrides the original value if the key is already present.
    /// ```rust
    /// use kvs::KvStore;
    /// let mut s = KvStore::new();
    /// s.set("a".to_owned(), "x".to_owned());
    /// let v = s.get("a".to_owned()).unwrap();
    /// assert_eq!(v, "x");
    /// ```
    pub fn set(&mut self, k: String, v: String) {
        self.hm.insert(k, v);
    }

    /// Get the value of a key if present
    /// ```rust
    /// use kvs::KvStore;
    /// let mut s = KvStore::new();
    /// s.set("a".to_owned(), "x".to_owned());
    /// let v = s.get("a".to_owned()).unwrap();
    /// assert_eq!(v, "x");
    /// ```
    pub fn get(&self, k: String) -> Option<String> {
        let v = self.hm.get(&k)?;
        Some(v.to_owned())
    }

    /// Remove a key if present
    /// ```rust
    /// use kvs::KvStore;
    /// let mut s = KvStore::new();
    /// s.set("a".to_owned(), "x".to_owned());
    /// let v = s.get("a".to_owned()).unwrap();
    /// assert_eq!(v, "x");
    /// s.remove("a".to_owned());
    /// assert_eq!(s.get("a".to_owned()), None);
    /// ```
    pub fn remove(&mut self, k: String) {
        self.hm.remove(&k);
    }
}
