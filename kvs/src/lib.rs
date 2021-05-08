#![deny(missing_docs)]

//! A simple key-value store that supports get, set and remove operations

#[macro_use]
extern crate failure_derive;

use err::KvsError;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fs::OpenOptions, io::BufReader, path};
use std::{
    fs::File,
    io::{BufWriter, Seek, SeekFrom},
};

mod err;
pub use err::Result;

const LOG_FILE: &str = "kvs_log";

#[derive(Debug, Serialize, Deserialize)]
enum Command {
    Set { k: String, v: String },
    Remove { k: String },
}

/// Implementation of key-value store
pub struct KvStore {
    log_path: path::PathBuf,
    w: BufWriter<File>,
    index: HashMap<String, String>,
}

impl KvStore {
    /// Open a new KvStore
    pub fn open(path: &path::Path) -> Result<KvStore> {
        let log_path = path.join(LOG_FILE);
        let f = OpenOptions::new().create(true).append(true).open(&log_path)?;
        let mut kvs = KvStore {
            log_path: log_path,
            w: BufWriter::new(f),
            index: HashMap::new(),
        };
        kvs.init_index()?;
        Ok(kvs)
    }

    /// Init index for Read and Remove command
    pub fn init_index(&mut self) -> Result<()> {
        let mut f = File::open(&self.log_path)?;
        let mut br = BufReader::new(&mut f);
        loop {
            let read_result = bson::Document::from_reader(&mut br);
            let d = match read_result {
                Ok(d) => d,
                // Err(bson::de::Error::EndOfStream) => break,
                Err(bson::de::Error::IoError(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            };
            let c: Command = bson::from_document(d)?;
            self.update_index(&c);
        }
        Ok(())
    }

    fn update_index(&mut self, c: &Command) {
        match *c {
            Command::Set { k: ref key, v: ref value } => self.index.insert(key.to_owned(), value.to_owned()),
            Command::Remove { k: ref key } => self.index.remove(key),
        };
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
        let c = Command::Set{ k, v };
        self.write_command(&c)?;
        self.update_index(&c);
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
        Ok(self.index.get(&k).map(|s| s.to_owned()))
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
        if let None = self.index.get(&k) {
            return Err(KvsError::KeyNotExist)
        }
        let c = Command::Remove{ k: k };
        self.write_command(&c)?;
        self.update_index(&c);
        Ok(())
    }
}
