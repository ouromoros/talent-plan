#![deny(missing_docs)]

//! A simple key-value store that supports get, set and remove operations

#[macro_use]
extern crate failure_derive;

use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fs::OpenOptions, io::{BufReader, Seek, SeekFrom, Write}, path, usize};
use std::{fs::File, io::BufWriter};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

/// Errors for KvStore
mod err;
pub use err::Result;
pub use err::KvsError;

const LOG_FILE: &str = "kvs_log";
const COMPACT_LOG_FILE: &str = "kvs_log_compact";

#[derive(Debug, Serialize, Deserialize)]
enum Command {
    Set { k: String, v: String },
    Remove { k: String },
}

/// UPDATE: Use framed json because it's easier to debug and work with.
/// The first 8 bytes indicate size of packet.
/// ~Use bson format for log for the following reasons:~
/// 1. bson library consumes stream lazily, in contrast to json library, which eases
/// development work.
/// 2. bson format maintains forward and backward compatibility automatically and is
/// self-describable, which makes it easy to debug.
/// 3. bson is a widely accepeted format and may encourage future development of tools
/// around this database. (probably not)
fn write_command<W: std::io::Write>(c: &Command, w: &mut W) -> Result<()> {
    let data = serde_json::to_vec(c)?;
    let size = data.len();
    write_u64(size as u64, w)?;
    w.write_all(data.as_ref())?;
    Ok(())
}

fn read_command<R: std::io::Read>(r: &mut R) -> Result<Option<Command>> {
    let header_result = read_u64(r);
    let size = match header_result {
        Ok(n) => n,
        // Err(bson::de::Error::EndOfStream) => break,
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
            return Ok(None)
        }
        Err(e) => return Err(e.into()),
    };
    let mut body = vec![0 as u8; size as usize];
    r.read_exact(&mut body)?;
    let c: Command = serde_json::from_slice(body.as_ref())?;
    Ok(Some(c))
}

fn read_u64<R: std::io::Read>(r: &mut R) -> std::io::Result<u64> {
    r.read_u64::<BigEndian>()
}

fn write_u64<W: std::io::Write>(n: u64, w: &mut W) -> std::io::Result<()> {
    w.write_u64::<BigEndian>(n)
}

fn get_offset<T: Seek>(br: &mut T) -> Result<u64> {
    // May not be efficient, consider using custom reader and writer
    Seek::seek(br, SeekFrom::Current(0)).map_err(|e| e.into())
}

/// Implementation of key-value store
pub struct KvStore {
    base_path: path::PathBuf,
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
            base_path: path.to_owned(),
            log_path: log_path,
            w: BufWriter::new(wf),
            r: BufReader::new(rf),
            index: HashMap::new(),
        };
        kvs.init_index()?;
        Ok(kvs)
    }

    fn reload(&mut self) -> Result<()> {
        let mut wf = OpenOptions::new()
            .create(true)
            .write(true)
            .open(&self.log_path)?;
        Seek::seek(&mut wf, SeekFrom::End(0))?;
        let rf = std::fs::File::open(&self.log_path)?;
        self.w =  BufWriter::new(wf);
        self.r =  BufReader::new(rf);
        self.index =  HashMap::new();
        self.init_index()
    }

    /// Init index for Read and Remove command
    fn init_index(&mut self) -> Result<()> {
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
        self.maybe_do_compaction(offset)?;
        write_command(c, &mut self.w)?;
        self.w.flush()?; // flush to disk to ensure content can be read later
        Ok(offset)
    }

    /// Set the value of a key, overrides the original value if the key is already present.
    /// ```rust
    /// use kvs::KvStore;
    /// let temp_dir = tempfile::TempDir::new().unwrap();
    /// let mut s = KvStore::open(temp_dir.path()).unwrap();
    /// s.set("a".to_owned(), "x".to_owned());
    /// let v = s.get("a".to_owned()).unwrap().unwrap();
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
    /// let temp_dir = tempfile::TempDir::new().unwrap();
    /// let mut s = KvStore::open(temp_dir.path()).unwrap();
    /// s.set("a".to_owned(), "x".to_owned());
    /// let v = s.get("a".to_owned()).unwrap().unwrap();
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
    /// let temp_dir = tempfile::TempDir::new().unwrap();
    /// let mut s = KvStore::open(temp_dir.path()).unwrap();
    /// s.set("a".to_owned(), "x".to_owned());
    /// let v = s.get("a".to_owned()).unwrap().unwrap();
    /// assert_eq!(v, "x");
    /// s.remove("a".to_owned());
    /// assert_eq!(s.get("a".to_owned()).unwrap(), None);
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

    // A naive STW log compaction implementation that rewrites the whole KvStore to a
    /// new compacted log file to replace the original one.
    fn maybe_do_compaction(&mut self, offset: u64) -> Result<()> {
        if offset < 1024 * 1024 {
            return Ok(())
        }
        let compact_path = self.base_path.join(COMPACT_LOG_FILE);
        let br = BufReader::new(std::fs::File::open(&self.log_path)?);
        let bw = BufWriter::new(std::fs::File::create(&compact_path)?);
        self.compact(br, bw)?;

        std::fs::remove_file(&self.log_path)?;
        std::fs::rename(&compact_path, &self.log_path)?;

        self.reload()?;
        Ok(())
    }

    fn compact(&mut self, mut old_log_reader: BufReader<File>, mut new_log_writer: BufWriter<File>) -> Result<()> {
        loop {
            let current_offset = get_offset(&mut old_log_reader)?;
            let result = read_command(&mut old_log_reader)?;
            let c = match result {
                Some(c) => c,
                None => break,
            };
            match c {
                Command::Set{ ref k, .. } => {
                    if let Some(offset) = self.index.get(k) {
                        if *offset == current_offset {
                            eprintln!("{:?} {:?} {:?}", c, offset, current_offset);
                            write_command(&c, &mut new_log_writer)?;
                        }
                    }
                }
                Command::Remove { .. } => (),
            }
        }
        new_log_writer.flush()?;
        Ok(())
    }
}

/// Engine
pub trait KvsEngine {
    /// Set the value of a string key to a string.
    fn set(&mut self, key: String, value: String) -> Result<()>;
    /// Get the string value of a string key. If the key does not exist, return None.
    fn get(&mut self, key: String) -> Result<Option<String>>;
    /// Remove a given string key.
    fn remove(&mut self, key: String) -> Result<()>;
}
