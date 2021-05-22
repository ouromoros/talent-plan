use std::path;
use crate::err::Result;
use crate::{KvsEngine, Error};

/// SledStore DB file name
pub const SLED_DB_FILE: &str = "sled_db";

/// KvsEngine implemented with `sled`
pub struct SledStore {
    db: sled::Db
}

impl SledStore {
    /// Open a new SledStore
    pub fn open(path: &path::Path) -> Result<SledStore> {
        let db_path = path.join(SLED_DB_FILE);
        let db = sled::open(db_path)?;
        Ok(SledStore{ db })
    }
}

impl KvsEngine for SledStore {
    fn set(&self, key: String, value: String) -> Result<()> {
        self.db.insert(key, value.as_bytes())?;
        self.db.flush()?;
        Ok(())
    }

    fn get(&self, key: String) -> Result<Option<String>> {
        let result = self.db.get(key)?;
        if let Some(v) = result {
            match String::from_utf8(v.to_vec()) {
                Ok(v) => Ok(Some(v)),
                Err(_) => Err(Error::ParseError("from utf8 error".to_string())),
            }
        } else {
            Ok(None)
        }
    }

    fn remove(&self, key: String) -> Result<()> {
        let result = self.db.remove(key)?;
        self.db.flush()?;
        match result {
            Some(_) => Ok(()),
            None => Err(Error::KeyNotExist),
        }
    }
}
