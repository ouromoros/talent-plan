/// Errors
#[derive(Fail, Debug)]
pub enum KvsError {
    /// Dummy error
    #[fail(display = "Kvs Error")]
    Dummy,
    #[fail(display = "{}", _0)]
    Io(#[cause] std::io::Error),
    #[fail(display = "{}", _0)]
    Bsons(#[cause] bson::ser::Error),
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
