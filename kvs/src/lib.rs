#![deny(missing_docs)]

//! A simple key-value store that supports get, set and remove operations

#[macro_use]
extern crate failure_derive;

use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fs::OpenOptions,
    io::{BufReader, Seek, SeekFrom, Write},
    path,
};
use std::{fs::File, io::BufWriter};

/// Errors for KvStore
mod err;
pub use err::Result;
pub use err::KvsError;

const LOG_FILE: &str = "kvs_log";

#[derive(Debug, Serialize, Deserialize)]
enum Command {
    Set { k: String, v: String },
    Remove { k: String },
}

/// Use bson format for log for the following reasons:
/// 1. bson library consumes stream lazily, in contrast to json library, which eases
/// development work.
/// 2. bson format maintains forward and backward compatibility automatically and is
/// self-describable, which makes it easy to debug.
/// 3. bson is a widely accepeted format and may encourage future development of tools
/// around this database. (probably not)
fn write_command<W: std::io::Write>(c: &Command, w: &mut W) -> Result<()> {
    let b = bson::to_document(c)?;
    b.to_writer(w)?;
    Ok(())
}

fn read_command<R: std::io::Read>(r: &mut R) -> Result<Option<Command>> {
    let read_result = bson::Document::from_reader(r);
    let d = match read_result {
        Ok(d) => d,
        // Err(bson::de::Error::EndOfStream) => break,
        Err(bson::de::Error::IoError(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
            return Ok(None)
        }
        Err(e) => return Err(e.into()),
    };
    let c: Command = bson::from_document(d)?;
    Ok(Some(c))
}

fn get_offset<T: Seek>(br: &mut T) -> Result<u64> {
    // May not be efficient, consider using custom reader and writer
    Seek::seek(br, SeekFrom::Current(0)).map_err(|e| e.into())
}

/// Implementation of key-value store
pub struct KvStore {
    log_path: path::PathBuf,
    w: BufWriter<File>,
    r: BufReader<File>,
    index: HashMap<String, u64>,
}

impl KvStore {
    /// Open a new KvStore
    pub fn open(path: &path::Path) -> Result<KvStore> {
        let log_path = path.join(LOG_FILE);
        let mut wf = OpenOptions::new()
            .create(true)
            .write(true)
            .open(&log_path)?;
        Seek::seek(&mut wf, SeekFrom::End(0))?;
        let rf = std::fs::File::open(&log_path)?;
        let mut kvs = KvStore {
            log_path: log_path,
            w: BufWriter::new(wf),
            r: BufReader::new(rf),
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
            let offset = get_offset(&mut br)?;
            let c = match read_command(&mut br) {
                Ok(Some(c)) => c,
                Ok(None) => break,
                Err(e) => return Err(e),
            };
            self.update_index(&c, offset);
        }
        Ok(())
    }

    fn update_index(&mut self, c: &Command, offset: u64) {
        match *c {
            Command::Set { ref k, .. } => self.index.insert(k.to_owned(), offset),
            Command::Remove { k: ref key } => self.index.remove(key),
        };
    }

    fn write_command(&mut self, c: &Command) -> Result<u64> {
        let offset = get_offset(&mut self.w)?;
        write_command(c, &mut self.w)?;
        self.w.flush()?; // flush to disk to ensure content can be read later
        Ok(offset)
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
        let c = Command::Set { k, v };
        let offset = self.write_command(&c)?;
        self.update_index(&c, offset);
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
    pub fn get(&mut self, k: String) -> Result<Option<String>> {
        match self.index.get(&k) {
            Some(offset) => {
                let offset = *offset;
                self.get_val(offset).map(|v| Some(v))
            }
            None => Ok(None),
        }
    }

    fn get_val(&mut self, offset: u64) -> Result<String> {
        Seek::seek(&mut self.r, SeekFrom::Start(offset))?;
        if let Some(Command::Set { v, .. }) = read_command(&mut self.r)? {
            Ok(v)
        } else {
            Err(KvsError::DataCurruption)
        }
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
            return Err(KvsError::KeyNotExist);
        }
        let c = Command::Remove { k: k };
        let offset = self.write_command(&c)?;
        self.update_index(&c, offset);
        Ok(())
    }
}
