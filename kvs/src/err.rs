/// Errors
#[derive(Fail, Debug)]
pub enum Error {
    /// Key not exsit error
    #[fail(display = "KeyNotExist Error")]
    KeyNotExist,
    /// File data is corrupted
    #[fail(display = "DataCorruption Error")]
    DataCorruption,
    /// Parsing client-server command error
    #[fail(display = "Parse arguments error: {}", _0)]
    ParseError(String),
    /// Io error
    #[fail(display = "{}", _0)]
    Io(#[cause] std::io::Error),
    /// Json error
    #[fail(display = "{}", _0)]
    Json(#[cause] serde_json::Error),
    /// Json error
    #[fail(display = "{}", _0)]
    Sled(#[cause] sled::Error),
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e)
    }
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Error::Json(e)
    }
}

impl From<sled::Error> for Error {
    fn from(e: sled::Error) -> Self {
        Error::Sled(e)
    }
}

/// Kvs Result
pub type Result<T> = std::result::Result<T, Error>;
