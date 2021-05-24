#[macro_use]
extern crate clap;
use clap::App;

use kvs::{Result, Error};
use kvs::protocol::{Request, Response};
use std::net::TcpStream;
use std::io::Write;
use kvs::client::Client;

fn exit(code: i32, msg: &str) -> ! {
    eprintln!("{}", msg);
    std::process::exit(code)
}

fn main() -> Result<()> {
    let yaml = load_yaml!("client.yml");
    let matches = App::from_yaml(yaml)
        .version(env!("CARGO_PKG_VERSION"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .get_matches();

    if matches.is_present("version") {
        println!("{}", env!("CARGO_PKG_VERSION"));
        return Ok(());
    }

    match matches.subcommand_name() {
        Some("get") => {
            let matches = matches.subcommand_matches("get").unwrap();
            let key = matches.value_of("KEY").unwrap();
            let addr = matches.value_of("addr").unwrap();
            let mut client = Client::new(addr)?;
            match client.get(key.to_string()) {
                Ok(Some(v)) => println!("{}", v),
                Ok(None) => println!("Key not found"),
                Err(e) => exit(100, format!("Unknown Error: {}", e).as_str()),
            }
        }
        Some("set") => {
            let matches = matches.subcommand_matches("set").unwrap();
            let key = matches.value_of("KEY").unwrap();
            let value = matches.value_of("VALUE").unwrap();
            let addr = matches.value_of("addr").unwrap();
            let mut client = Client::new(addr)?;
            match client.set(key.to_string(), value.to_string()) {
                Ok(()) => {},
                Err(e) => exit(100, format!("Unknown error: {:?}", e).as_str()),
            }
        },
        Some("rm") => {
            let matches = matches.subcommand_matches("rm").unwrap();
            let key = matches.value_of("KEY").unwrap();
            let addr = matches.value_of("addr").unwrap();
            let mut client = Client::new(addr)?;
            match client.remove(key.to_string()) {
                Ok(()) => {},
                Err(Error::KeyNotExist) => exit(2, "Key not found"),
                Err(e) => exit(100, format!("Unknown error: {:?}", e).as_str()),
            }        },
        None => exit(2, "subcommand not provided"),
        _ => exit(3, "unsupported command"),
    };

    Ok(())
}
