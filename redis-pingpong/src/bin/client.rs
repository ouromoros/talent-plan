use std::{net::TcpStream};

const ADDR: &str = "127.0.0.1:7878";

fn main() {
    let mut stream = TcpStream::connect(ADDR).unwrap();
    let req = rpp::Request {
        command: "PING".to_owned(),
        args: vec!["HI".to_owned()],
    };
    rpp::req_to_writer(&mut stream, &req);
    std::io::copy(&mut stream, &mut std::io::stdout()).unwrap();
}
