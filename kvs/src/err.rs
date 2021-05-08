/// Errors
#[derive(Fail, Debug)]
pub enum KvsError {
    /// Key not exsit error
    #[fail(display = "KeyNotExist Error")]
    KeyNotExist,
    /// File data is corrupted
    #[fail(display = "DataCurruption Error")]
    DataCurruption,
    /// Io error
    #[fail(display = "{}", _0)]
    Io(#[cause] std::io::Error),
    /// Bson serialization error
    #[fail(display = "{}", _0)]
    Bsons(#[cause] bson::ser::Error),
    /// Bson deserialization error
    #[fail(display = "{}", _0)]
    Bsond(#[cause] bson::de::Error),
}

impl From<std::io::Error> for KvsError {
    fn from(e: std::io::Error) -> Self {
        KvsError::Io(e)
    }
}

impl From<bson::ser::Error> for KvsError {
    fn from(e: bson::ser::Error) -> Self {
        KvsError::Bsons(e)
    }
}

impl From<bson::de::Error> for KvsError {
    fn from(e: bson::de::Error) -> Self {
        KvsError::Bsond(e)
    }
}

/// Kvs Result
pub type Result<T> = std::result::Result<T, KvsError>;
