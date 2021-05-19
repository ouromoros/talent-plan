#[macro_use]
extern crate clap;
use clap::App;

use kvs::Result;
use kvs::protocol::{Request, Response};
use std::net::TcpStream;
use std::io::Write;

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

    let (req, addr) = match matches.subcommand_name() {
        Some("get") => {
            let matches = matches.subcommand_matches("get").unwrap();
            let key = matches.value_of("KEY").unwrap();
            let addr = matches.value_of("addr").unwrap();
            (Request::Get(key.to_string()), addr)
        }
        Some("set") => {
            let matches = matches.subcommand_matches("set").unwrap();
            let key = matches.value_of("KEY").unwrap();
            let value = matches.value_of("VALUE").unwrap();
            let addr = matches.value_of("addr").unwrap();
            (Request::Set(key.to_string(), value.to_string()), addr)
        },
        Some("rm") => {
            let matches = matches.subcommand_matches("rm").unwrap();
            let key = matches.value_of("KEY").unwrap();
            let addr = matches.value_of("addr").unwrap();
            (Request::Remove(key.to_string()), addr)
        },
        None => exit(2, "subcommand not provided"),
        _ => exit(3, "unsupported command"),
    };
    let mut conn = TcpStream::connect(addr)?;
    conn.write_all(req.to_str()?.as_bytes())?;
    let rsp = Response::from_reader(&mut conn)?;
    match rsp {
        Response::Value(Some(v)) => println!("{}", v),
        Response::Value(None) => println!("Key not found"),
        Response::Err(e) => match e.as_str() {
            "OK" => {},
            "KeyNotExist" => exit(10, "Key not found"),
            e => exit(100, format!("Unknown error: {}", e).as_str()),
        }
    }
    Ok(())
}
