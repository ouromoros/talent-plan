pub struct Request {
    command: String,
    args: Vec<String> 
}

fn read_number<R: std::io::Read + std::io::Seek>(r: &mut R) -> u32 {
    let mut result: u32 = 0;
    let mut buf1: [u8; 1] = [0; 1];
    loop {
        r.read_exact(&mut buf1).unwrap();
        let b = buf1[0];
        if b < b'0' || b > b'9' {
            std::io::Seek::seek(r, std::io::SeekFrom::Current(-1)).unwrap();
            break
        }
        let n = b - b'0';
        result = result * 10 + u32::from(n);
    }
    result
}

pub fn req_from_reader<R: std::io::Read + std::io::Seek>(mut r: R) -> Request {
    let mut buf1: [u8; 1] = [0; 1];
    r.read(&mut buf1).expect("read error");
    let len = read_number(&mut r);
    if len == 1 {
        return Request { command: "PING".to_owned(), args: Vec::new() }
    }

    let mut buf12: [u8; 12] = [0; 12];
    r.read(&mut buf12).expect("io error");
    r.read(&mut buf1).expect("read error");
    let len = read_number(&mut r);
    r.read(&mut buf1).expect("read error");
    r.read(&mut buf1).expect("read error");
    let mut buf3: Vec<u8> = vec![0; len as usize];
    r.read_exact(&mut buf3).unwrap();
    Request {
        command: "PING".to_owned(),
        args: vec![String::from_utf8(buf3).unwrap()]
    }
}

pub fn req_to_writer<W: std::io::Write>(w: &mut W, req: &Request) {
    let b = req_to_bytes(req);
    w.write_all(&b).expect("write error");
}

fn to_bulk_str(s: &str) -> String {
    format!("${}\r\n{}\r\n", s.len(), s)
}

fn req_to_bytes(r: &Request) -> Vec<u8> {
    let mut s = String::new();
    s.push_str(&format!("*{}\r\n", 1 + r.args.len()));
    s.push_str(&to_bulk_str(&r.command));
    for a in r.args.iter() {
        s.push_str(&to_bulk_str(a));
    }
    s.into_bytes()
}
