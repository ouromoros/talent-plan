#[macro_use]
extern crate clap;
use clap::App;

use kvs::Result;

fn exit(code: i32, msg: &str) -> Result<()> {
    eprintln!("{}", msg);
    std::process::exit(code);
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
            get(key.to_owned())
        }
        Some("set") => {
            let matches = matches.subcommand_matches("set").unwrap();
            let key = matches.value_of("KEY").unwrap();
            let value = matches.value_of("VALUE").unwrap();
            set(key.to_owned(), value.to_owned())
        },
        Some("rm") => {
            let matches = matches.subcommand_matches("rm").unwrap();
            let key = matches.value_of("KEY").unwrap();
            rm(key.to_owned())
        },
        None => exit(2, "subcommand not provided"),
        _ => exit(3, "unsupported command"),
    }
}

fn get(k: String) -> Result<()> {
    let mut store = kvs::KvStore::open(&std::env::current_dir()?)?;
    let result = store.get(k)?;
    match result {
        Some(v) => println!("{}", v),
        None => println!("Key not found"),
    }
    Ok(())
}

fn set(k: String, v: String) -> Result<()> {
    let mut store = kvs::KvStore::open(&std::env::current_dir()?)?;
    store.set(k, v)
}

fn rm(k: String) -> Result<()> {
    let mut store = kvs::KvStore::open(&std::env::current_dir()?)?;
    let result = store.remove(k);
    match result {
        Ok(_) => Ok(()),
        Err(kvs::KvsError::KeyNotExist) => {
            println!("Key not found");
            Err(kvs::KvsError::KeyNotExist)
        }
        Err(e) => Err(e)
    }
}
