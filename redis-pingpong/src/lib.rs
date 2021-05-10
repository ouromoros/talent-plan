use std::io::BufReader;
use std::io::BufRead;
use std::io::Read;

#[derive(Debug, Eq, PartialEq)]
pub struct Request {
    pub command: String,
    pub args: Vec<String> 
}

#[derive(Debug, Eq, PartialEq)]
pub enum RedisData {
    BulkString(String),
    SimpleString(String),
    Error
}

fn skip<R: std::io::Read>(r: &mut R) {
}

fn read_number<R: std::io::Read>(r: &mut BufReader<R>) -> u32 {
    let mut result: u32 = 0;
    let mut buf: Vec<u8> = Vec::new();
    r.read_until(b'\r', &mut buf).unwrap();
    for b in buf[..buf.len()-1].iter() {
        let n = b - b'0';
        result = result * 10 + u32::from(n);
    }
    result
}

pub fn req_from_reader<R: std::io::Read>(r: &mut R) -> Request {
    let mut r = BufReader::new(r);
    let mut buf1: [u8; 1] = [0; 1];
    r.read(&mut buf1).expect("read error");
    let len = read_number(&mut r);
    if len == 1 {
        return Request { command: "PING".to_owned(), args: Vec::new() }
    }

    let mut buf12: [u8; 11] = [0; 11];
    r.read(&mut buf12).expect("io error");
    r.read(&mut buf1).expect("read error");
    let len = read_number(&mut r);
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

fn to_simple_str(s: &str) -> String {
    format!("+{}\r\n", s)
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

pub fn rsp_to_writer<W: std::io::Write>(w: &mut W, rsp: &RedisData) {
    if let RedisData::SimpleString(s) = rsp {
        w.write_all(to_simple_str(s).as_bytes()).unwrap();
    } else {
        panic!("not implemented")
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn read_empty_ping() {
        let empty_ping_data = b"*1\r\n$4\r\nPING\r\n";
        let mut r = std::io::Cursor::new(empty_ping_data.to_vec());
        let req = super::req_from_reader(&mut r);
        assert_eq!(req, super::Request{ command: "PING".to_owned(), args: Vec::new() })
    }

    #[test]
    fn read_ping_with_arg() {
        let test_cases  = [
            (b"*2\r\n$4\r\nPING\r\n$5\r\nHELLO\r\n".to_vec(), super::Request{command: "PING".to_owned(), args: vec!["HELLO".to_owned()]}),
            (b"*2\r\n$4\r\nPING\r\n$11\r\nHELLO WORLD\r\n".to_vec(), super::Request{command: "PING".to_owned(), args: vec!["HELLO WORLD".to_owned()]})
        ];
        for (data, expected) in test_cases.iter() {
            let mut r = std::io::Cursor::new(data);
            let req = super::req_from_reader(&mut r);
            assert_eq!(req, *expected)
        }
    }

    #[test]
    fn write_empty_ping() {
        let r = super::Request{ command: "PING".to_owned(), args: Vec::new() };
        let empty_ping_data = b"*1\r\n$4\r\nPING\r\n".to_vec();
        let parsed = super::req_to_bytes(&r);
        assert_eq!(parsed, empty_ping_data)
    }

    #[test]
    fn write_ping_with_arg() {
        let test_cases  = [
            (b"*2\r\n$4\r\nPING\r\n$5\r\nHELLO\r\n".to_vec(), super::Request{command: "PING".to_owned(), args: vec!["HELLO".to_owned()]}),
            (b"*2\r\n$4\r\nPING\r\n$11\r\nHELLO WORLD\r\n".to_vec(), super::Request{command: "PING".to_owned(), args: vec!["HELLO WORLD".to_owned()]})
        ];
        for (expected, r) in test_cases.iter() {
            let parsed = super::req_to_bytes(r);
            assert_eq!(parsed, *expected)
        }
    }
}
