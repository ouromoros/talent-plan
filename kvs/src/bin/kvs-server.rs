#[macro_use]
extern crate clap;

use clap::App;

use kvs::{Result};
use log::{debug, error};
use kvs::thread_pool::{NaiveThreadPool, ThreadPool, SharedQueueThreadPool};
use kvs::server::Server;

fn exit(code: i32, msg: &str) -> ! {
    eprintln!("{}", msg);
    std::process::exit(code)
}

fn detect_engine(path: &std::path::Path) -> Option<String> {
    let kvs_log_file = path.join(kvs::KVS_LOG_FILE);
    let sled_db_file = path.join(kvs::SLED_DB_FILE);
    if kvs_log_file.exists() {
        Some("kvs".to_string())
    } else if sled_db_file.exists() {
        Some("sled".to_string())
    } else {
        None
    }
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
    let detected_engine = detect_engine(std::env::current_dir()?.as_path());
    debug!("specified engine: {}, detected engine: {:?}", engine, detected_engine);
    if let Some(de) = detected_engine {
        if de != engine {
            exit(5, format!("Engine specified: {}, actual: {}", engine, de).as_str());
        }
    }

    error!("VERSION={} Addr={} Engine={}", env!("CARGO_PKG_VERSION"), addr, engine);
    let listener = if let Ok(bind) = std::net::TcpListener::bind(addr) {
        bind
    } else {
        exit(1, "bind addr error!")
    };
    let current_dir = std::env::current_dir()?;
    let thread_num = num_cpus::get();
    let pool = SharedQueueThreadPool::new(thread_num as u32)?;
    if engine == "kvs" {
        let engine = kvs::KvStore::open(&current_dir)?;
        let mut server = Server::new(pool, engine, listener);
        server.run().unwrap();
    } else {
        let engine = kvs::SledStore::open(&current_dir)?;
        let mut server = Server::new(pool, engine, listener);
        server.run().unwrap();
    }

    Ok(())
}
