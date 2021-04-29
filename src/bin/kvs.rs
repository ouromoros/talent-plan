#[macro_use]
extern crate clap;
use clap::App;

fn exit(code: i32, msg: &str) {
    eprintln!("{}", msg);
    std::process::exit(code);
}

fn main() {
    let yaml = load_yaml!("cli.yml");
    let matches = App::from_yaml(yaml)
        .version(env!("CARGO_PKG_VERSION"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .name(env!("CARGO_PKG_NAME"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .get_matches();

    if matches.is_present("version") {
        println!("{}", env!("CARGO_PKG_VERSION"));
        return;
    }

    match matches.subcommand_name() {
        Some("get") => exit(1, "unimplemented"),
        Some("set") => exit(1, "unimplemented"),
        Some("rm") => exit(1, "unimplemented"),
        None => exit(2, "subcommand not provided"),
        _ => exit(3, "unsupported command"),
    }
}
