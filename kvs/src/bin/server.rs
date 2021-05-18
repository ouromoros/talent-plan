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
    let engine = if let Some(engine) = matches.value_of("engine") {
        engine
    } else {
        "kvs"
    };

    info!("Addr={} Engine={}", addr, engine);
    loop {
        let bind = std::net::TcpListener::bind(addr)?;
        let connection = bind.accept()?;
        let (stream, _) = connection;
        serve(stream)?;
    }
    Ok(())
}

fn serve(s: std::net::TcpStream) -> Result<()> {
    Ok(())
}
