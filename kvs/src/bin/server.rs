#[macro_use]
extern crate clap;
use clap::App;

use kvs::Result;
use log::{info};

fn exit(code: i32, msg: &str) -> Result<()> {
    eprintln!("{}", msg);
    std::process::exit(code);
}

fn main() -> Result<()> {
    simple_logger::SimpleLogger::new().init().unwrap();
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

    let addr =  if let Some(addr) = matches.value_of("addr") {
        addr
    } else {
        "127.0.0.1:4000"
    };
    let engine = if let Some(engine) = matches.value_of("engine") {
        engine
    } else {
        "kvs"
    };

    info!("Addr={} Engine={}", addr, engine);

    Ok(())
}
