#![deny(missing_docs)]

//! A simple key-value store that supports get, set and remove operations

#[macro_use]
extern crate failure_derive;

use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fs::OpenOptions, io::BufReader, path};
use std::{
    fs::File,
    io::{BufWriter, Seek, SeekFrom},
};

mod err;
pub use err::Result;

#[derive(Debug, Serialize, Deserialize)]
enum Command {
    Set { key: String, value: String },
    Remove { key: String },
}

/// Implementation of key-value store
pub struct KvStore {
    file_path: path::PathBuf,
    w: BufWriter<File>,
}

impl KvStore {
    /// Open a new KvStore
    pub fn open(path: &path::Path) -> Result<KvStore> {
        let mut f = OpenOptions::new().append(true).open(path)?;
        Ok(KvStore {
            file_path: path.to_path_buf(),
            w: BufWriter::new(f),
        })
    }

    /// Init index for Read and Remove command
    pub fn init_index(&mut self) -> Result<()> {
        let mut f = File::open(&self.file_path)?;
        let mut br = BufReader::new(&mut f);
        loop {
            let read_result = bson::Document::from_reader(&mut br);
            let d = match read_result {
                Ok(d) => d,
                Err(bson::de::Error::EndOfStream) => break,
                Err(e) => return Err(e.into()),
            };
            let c: Command = bson::from_document(d)?;
            self.update_index(&c);
        }
        Ok(())
    }

    fn update_index(&mut self, c: &Command) {

    }

    fn write_command(&mut self, c: &Command) -> Result<()> {
        let b = bson::to_document(c)?;
        b.to_writer(&mut self.w)?;
        Ok(())
    }

    /// Set the value of a key, overrides the original value if the key is already present.
    /// ```rust
    /// use kvs::KvStore;
    /// let mut s = KvStore::new();
    /// s.set("a".to_owned(), "x".to_owned());
    /// let v = s.get("a".to_owned()).unwrap();
    /// assert_eq!(v, "x");
    /// ```
    pub fn set(&mut self, k: String, v: String) -> Result<()> {
        Ok(())
    }

    /// Get the value of a key if present
    /// ```rust
    /// use kvs::KvStore;
    /// let mut s = KvStore::new();
    /// s.set("a".to_owned(), "x".to_owned());
    /// let v = s.get("a".to_owned()).unwrap();
    /// assert_eq!(v, "x");
    /// ```
    pub fn get(&self, k: String) -> Result<Option<String>> {
        panic!("not implemented")
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
    pub fn remove(&mut self, k: String) -> Result<()> {
        Ok(())
    }
}
