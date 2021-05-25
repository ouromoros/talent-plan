#![deny(missing_docs)]

//! A simple key-value store that supports get, set and remove operations

use serde::{Deserialize, Serialize};
use std::{fs::OpenOptions, io::{BufReader, Seek, SeekFrom, Write}, path, usize};
use std::{fs::File, io::BufWriter};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

/// Errors for KvStore
mod err;
pub use err::Result;
pub use err::Error;

/// Kvs client-server protocol
pub mod protocol;

mod sled_engine;
pub mod thread_pool;
pub mod client;
pub mod server;

pub use sled_engine::SLED_DB_FILE;
pub use sled_engine::SledStore;
use std::sync::{RwLock, Arc, Mutex};

/// KvStore log file name
pub const KVS_LOG_FILE: &str = "kvs_log";
const KVS_COMPACT_LOG_FILE: &str = "kvs_log_compact";

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

type Index = chashmap::CHashMap<String, u64>;
type Writer = BufWriter<File>;

/// Implementation of key-value store
pub struct KvStore {
    base_path: path::PathBuf,
    w: Arc<Mutex<Writer>>,
    index: Arc<Index>,
}

impl KvStore {
    /// Open a new KvStore
    pub fn open(path: &path::Path) -> Result<KvStore> {
        let log_path = Self::log_path(path);
        let mut wf = OpenOptions::new()
            .create(true)
            .write(true)
            .open(&log_path)?;
        Seek::seek(&mut wf, SeekFrom::End(0))?;
        let mut kvs = KvStore {
            base_path: path.to_owned(),
            w: Arc::new(Mutex::new(BufWriter::new(wf))),
            index: Arc::new(chashmap::CHashMap::new()),
        };
        {
            kvs.init_index(&kvs.index)?;
        }
        Ok(kvs)
    }

    fn log_path(base_path: &path::Path) -> path::PathBuf {
        base_path.join(KVS_LOG_FILE)
    }

    fn get_log_reader(&self) -> Result<BufReader<File>> {
        let log_path = Self::log_path(&self.base_path);
        Ok(BufReader::new(File::open(log_path)?))
    }

    fn reload(&self, index: &mut Index, w: &mut Writer) -> Result<()> {
        let log_path = Self::log_path(&self.base_path);
        let mut wf = OpenOptions::new()
            .create(true)
            .write(true)
            .open(&log_path)?;
        Seek::seek(&mut wf, SeekFrom::End(0))?;
        let rf = std::fs::File::open(&log_path)?;
        *w =  BufWriter::new(wf);
        *index =  chashmap::CHashMap::new();
        self.init_index(index)
    }

    /// Init index for Read and Remove command
    fn init_index(&self, index: &Index) -> Result<()> {
        let mut f = File::open(Self::log_path(&self.base_path))?;
        let mut br = BufReader::new(&mut f);
        loop {
            let offset = get_offset(&mut br)?;
            let c = match read_command(&mut br) {
                Ok(Some(c)) => c,
                Ok(None) => break,
                Err(e) => return Err(e),
            };
            Self::update_index(&c, offset, index);
        }
        Ok(())
    }

    fn update_index(c: &Command, offset: u64, index: &Index) {
        match *c {
            Command::Set { ref k, .. } => index.insert(k.to_owned(), offset),
            Command::Remove { k: ref key } => index.remove(key),
        };
    }

    fn write_command(&self, w: &mut Writer, c: &Command) -> Result<u64> {
        let offset = get_offset(w)?;
        // self.maybe_do_compaction(data, offset)?;
        write_command(c, w)?;
        w.flush()?; // flush to disk to ensure content can be read later
        Ok(offset)
    }

    fn get_val(&self, offset: u64) -> Result<String> {
        let mut r = self.get_log_reader()?;
        Seek::seek(&mut r, SeekFrom::Start(offset))?;
        if let Some(Command::Set { v, .. }) = read_command(&mut r)? {
            Ok(v)
        } else {
            Err(Error::DataCorruption)
        }
    }

    /// A naive STW log compaction implementation that rewrites the whole KvStore to a
    /// new compacted log file to replace the original one.
    // fn maybe_do_compaction(&self, data: &mut KvStoreInternal, offset: u64) -> Result<()> {
    //     if offset < 1024 * 1024 {
    //         return Ok(())
    //     }
    //     let compact_path = self.base_path.join(KVS_COMPACT_LOG_FILE);
    //     let br = BufReader::new(std::fs::File::open(&self.log_path)?);
    //     let bw = BufWriter::new(std::fs::File::create(&compact_path)?);
    //     self.compact(&mut data.index, br, bw)?;
    //
    //     std::fs::remove_file(&self.log_path)?;
    //     std::fs::rename(&compact_path, &self.log_path)?;
    //
    //     self.reload(data)?;
    //     Ok(())
    // }

    fn compact(&self, index: &mut Index, mut old_log_reader: BufReader<File>, mut new_log_writer: BufWriter<File>) -> Result<()> {
        loop {
            let current_offset = get_offset(&mut old_log_reader)?;
            let result = read_command(&mut old_log_reader)?;
            let c = match result {
                Some(c) => c,
                None => break,
            };
            match c {
                Command::Set{ ref k, .. } => {
                    if let Some(offset) = index.get(k) {
                        if *offset == current_offset {
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

impl Clone for KvStore {
    fn clone(&self) -> Self {
        KvStore {
            base_path: self.base_path.clone(),
            w: self.w.clone(),
            index: self.index.clone(),
        }
    }
}

impl KvsEngine for KvStore {
    /// Set the value of a key, overrides the original value if the key is already present.
    /// ```rust
    /// use kvs::{KvsEngine, KvStore};
    /// let temp_dir = tempfile::TempDir::new().unwrap();
    /// let mut s = KvStore::open(temp_dir.path()).unwrap();
    /// s.set("a".to_owned(), "x".to_owned());
    /// let v = s.get("a".to_owned()).unwrap().unwrap();
    /// assert_eq!(v, "x");
    /// ```
    fn set(&self, k: String, v: String) -> Result<()> {
        let c = Command::Set { k, v };
        let mut w= self.w.lock().unwrap();
        let index = self.index.as_ref();
        let offset = self.write_command(&mut *w, &c)?;
        Self::update_index(&c, offset, index);
        Ok(())
    }

    /// Get the value of a key if present
    /// ```rust
    /// use kvs::{KvsEngine, KvStore};
    /// let temp_dir = tempfile::TempDir::new().unwrap();
    /// let mut s = KvStore::open(temp_dir.path()).unwrap();
    /// s.set("a".to_owned(), "x".to_owned());
    /// let v = s.get("a".to_owned()).unwrap().unwrap();
    /// assert_eq!(v, "x");
    /// ```
    fn get(&self, k: String) -> Result<Option<String>> {
        let index = self.index.as_ref();
        match index.get(&k) {
            Some(offset) => {
                let offset = *offset;
                self.get_val(offset).map(|v| Some(v))
            }
            None => Ok(None),
        }
    }

    /// Remove a key if present
    /// ```rust
    /// use kvs::{KvsEngine, KvStore};
    /// let temp_dir = tempfile::TempDir::new().unwrap();
    /// let mut s = KvStore::open(temp_dir.path()).unwrap();
    /// s.set("a".to_owned(), "x".to_owned());
    /// let v = s.get("a".to_owned()).unwrap().unwrap();
    /// assert_eq!(v, "x");
    /// s.remove("a".to_owned());
    /// assert_eq!(s.get("a".to_owned()).unwrap(), None);
    /// ```
    fn remove(&self, k: String) -> Result<()> {
        let mut w= self.w.lock().unwrap();
        let index = self.index.as_ref();
        if let None = index.get(&k) {
            return Err(Error::KeyNotExist);
        }
        let c = Command::Remove { k };
        let offset = self.write_command(&mut *w, &c)?;
        Self::update_index(&c, offset, index);
        Ok(())
    }
}

/// Engine
pub trait KvsEngine: Clone + Send + 'static {
    /// Set the value of a string key to a string.
    fn set(&self, key: String, value: String) -> Result<()>;
    /// Get the string value of a string key. If the key does not exist, return None.
    fn get(&self, key: String) -> Result<Option<String>>;
    /// Remove a given string key.
    fn remove(&self, key: String) -> Result<()>;
}
