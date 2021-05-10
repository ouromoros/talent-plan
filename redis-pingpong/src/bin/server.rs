use std::{net::TcpListener};

const ADDR: &str = "127.0.0.1:7878";

fn main() {
    let listener = TcpListener::bind(ADDR).unwrap();

    for stream in listener.incoming() {
        let mut stream = stream.unwrap();

        loop {
            // std::io::copy(&mut stream, &mut std::io::stdout());
            let mut req = rpp::req_from_reader(&mut stream);
            println!("{:?}", req);
            assert_eq!(req.command, "PING".to_owned());
            let s = if req.args.len() == 0 {
                "PONG".to_owned()
            } else {
                req.args.drain(0..1).next().unwrap()
            };
            let rsp = rpp::RedisData::SimpleString(s);
            rpp::rsp_to_writer(&mut stream, &rsp);
        }
    }
}
