//! Client implementation

use std::net::{TcpStream, ToSocketAddrs};
use crate::err::Result;
use crate::protocol::{Request, Response};
use std::io::{Write, BufReader};
use crate::Error;

/// KvsClient
pub struct Client {
    conn: TcpStream,
}

impl Client {
    /// Creates new instance
    pub fn new<A: ToSocketAddrs>(addr: A) -> Result<Self> {
        let conn = TcpStream::connect(addr)?;
        Ok(Client{ conn })
    }

    /// Get
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        let req = Request::Get(key);
        self.conn.write_all(req.to_str()?.as_bytes())?;
        let rsp = Response::from_reader(&mut BufReader::new(&mut self.conn))?;
        match rsp {
            Response::Value(v) => Ok(v),
            Response::Err(e) => match e.as_str() {
                "KeyNotExist" => Err(Error::KeyNotExist),
                e => Err(Error::ServerError(e.to_string())),
            }
        }
    }

    /// Set
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let req = Request::Set(key, value);
        self.conn.write_all(req.to_str()?.as_bytes())?;
        let rsp = Response::from_reader(&mut BufReader::new(&mut self.conn))?;
        match rsp {
            Response::Err(e) => match e.as_str() {
                "OK" => Ok(()),
                e => Err(Error::ServerError(e.to_string())),
            },
            _ => Err(Error::ParseError("Unexpected Response Type".to_string())),
        }
    }

    /// Remove
    pub fn remove(&mut self, key: String) -> Result<()> {
        let req = Request::Remove(key);
        self.conn.write_all(req.to_str()?.as_bytes())?;
        let rsp = Response::from_reader(&mut BufReader::new(&mut self.conn))?;
        match rsp {
            Response::Err(e) => match e.as_str() {
                "OK" => Ok(()),
                "KeyNotExist" => Err(Error::KeyNotExist),
                e => Err(Error::ServerError(e.to_string())),
            },
            _ => Err(Error::ParseError("Unexpected Response Type".to_string())),
        }
    }

    /// Shutdown
    pub fn shutdown(&mut self) -> Result<()> {
        let req = Request::Shutdown;
        self.conn.write_all(req.to_str()?.as_bytes())?;
        let rsp = Response::from_reader(&mut BufReader::new(&mut self.conn))?;
        match rsp {
            Response::Err(e) => match e.as_str() {
                "OK" => Ok(()),
                e => Err(Error::ServerError(e.to_string())),
            },
            _ => Err(Error::ParseError("Unexpected Response Type".to_string())),
        }
    }
}
