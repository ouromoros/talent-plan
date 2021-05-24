//! Server implementation

use std::net::TcpListener;
use crate::thread_pool::ThreadPool;
use crate::{KvsEngine, Result, Error};
use log::{debug, error};
use crate::protocol::{Request, Response};
use std::io::Write;

/// Server struct
pub struct Server<P, E> where P: ThreadPool, E: KvsEngine {
    pool: P,
    engine: E,
    listener: TcpListener,
}

impl<P, E> Server<P, E> where P: ThreadPool, E: KvsEngine {
    /// new instance
    pub fn new(pool: P, engine: E, listener: TcpListener) -> Self {
        Server { pool, engine, listener }
    }

    /// run
    pub fn run(&mut self) -> Result<()> {
        loop {
            debug!("start listening...");
            let connection = self.listener.accept()?;
            let engine = self.engine.clone();
            self.pool.spawn(|| {
                let (stream, sock_addr) = connection;
                let mut engine = engine;
                debug!("connection from {:?}", sock_addr.ip());
                match Self::serve(&mut engine, stream) {
                    Ok(()) => {},
                    Err(Error::Io(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {},
                    Err(e) => error!("Unhandled error: {:?}", e),
                }
            });
        }
    }

    fn serve(engine: &mut E, s: std::net::TcpStream) -> Result<()> {
        let mut br = std::io::BufReader::new(&s);
        let mut bw = std::io::BufWriter::new(&s);
        loop {
            let req = Request::from_reader(&mut br)?;
            let mut shutdown = false;
            debug!("request: {:?}", req);
            let rsp = match req {
                Request::Get(k) => {
                    let v = engine.get(k)?;
                    Response::Value(v)
                },
                Request::Set(k, v) => {
                    let result = engine.set(k, v);
                    match result {
                        Ok(()) => Response::Err("OK".to_string()),
                        Err(e) => Response::Err(format!("{:?}", e)),
                    }
                }
                Request::Remove(k) => {
                    let result = engine.remove(k);
                    match result {
                        Ok(()) => Response::Err("OK".to_string()),
                        Err(e) => Response::Err(format!("{:?}", e)),
                    }
                }
                Request::Shutdown => {
                    shutdown = true;
                    Response::Err("OK".to_string())
                },
            };
            debug!("response: {:?}", rsp);
            rsp.to_writer(&mut bw)?;
            bw.flush()?;
            if shutdown {
                break
            }
        };
        Ok(())
    }
}
