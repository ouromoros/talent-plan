#[macro_use]
extern crate clap;

use clap::App;

use kvs::{Result, KvsEngine};
use kvs::protocol::{Request, Response};
use log::{debug, error};

fn exit(code: i32, msg: &str) -> ! {
    eprintln!("{}", msg);
    std::process::exit(code)
}

fn main() -> Result<()> {
    env_logger::init();
    let yaml = load_yaml!("server.yml");
    let matches = App::from_yaml(yaml)
        .version(env!("CARGO_PKG_VERSION"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .get_matches();

    if matches.is_present("version") {
        println!("{}", env!("CARGO_PKG_VERSION"));
        return Ok(());
    }

    let addr = matches.value_of("addr").unwrap();
    let engine = matches.value_of("engine").unwrap();

    error!("VERSION={} Addr={} Engine={}", env!("CARGO_PKG_VERSION"), addr, engine);
    let mut store = kvs::KvStore::open(&std::env::current_dir()?)?;
    let bind = if let Ok(bind) = std::net::TcpListener::bind(addr) {
        bind
    } else {
        exit(1, "bind addr error!")
    };
    loop {
        debug!("start listening...");
        let connection = bind.accept()?;
        let (stream, sock_addr) = connection;
        debug!("connection from {:?}", sock_addr.ip());
        serve(&mut store, stream)?;
    }
}

fn serve<E: KvsEngine>(store: &mut E, s: std::net::TcpStream) -> Result<()> {
    let mut br = std::io::BufReader::new(&s);
    let mut bw = std::io::BufWriter::new(&s);
    let req = Request::from_reader(&mut br)?;
    debug!("request: {:?}", req);
    let rsp = match req {
        Request::Get(k) => {
            let v = store.get(k)?;
            Response::Value(v)
        },
        Request::Set(k, v) => {
            let result = store.set(k, v);
            match result {
                Ok(()) => Response::Err("OK".to_string()),
                Err(e) => Response::Err(format!("{:?}", e)),
            }
        }
        Request::Remove(k) => {
            let result = store.remove(k);
            match result {
                Ok(()) => Response::Err("OK".to_string()),
                Err(e) => Response::Err(format!("{:?}", e)),
            }
        }
    };
    debug!("response: {:?}", rsp);
    rsp.to_writer(&mut bw)?;
    Ok(())
}
