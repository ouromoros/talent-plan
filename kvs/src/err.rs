/// Errors
#[derive(Fail, Debug)]
pub enum KvsError {
    /// Key not exsit error
    #[fail(display = "KeyNotExist Error")]
    KeyNotExist,
    /// File data is corrupted
    #[fail(display = "DataCurruption Error")]
    DataCurruption,
    /// Parsing client-server command error
    #[fail(display = "Parse arguments error: {}", _0)]
    ParseError(String),
    /// Io error
    #[fail(display = "{}", _0)]
    Io(#[cause] std::io::Error),
    /// Json error
    #[fail(display = "{}", _0)]
    Json(#[cause] serde_json::Error),
}

impl From<std::io::Error> for KvsError {
    fn from(e: std::io::Error) -> Self {
        KvsError::Io(e)
    }
}

impl From<serde_json::Error> for KvsError {
    fn from(e: serde_json::Error) -> Self {
        KvsError::Json(e)
    }
}

/// Kvs Result
pub type Result<T> = std::result::Result<T, KvsError>;
