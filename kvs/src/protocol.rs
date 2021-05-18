use crate::err::Result;
use crate::err::KvsError::{ParseError};

/// Kvs protocol request
#[derive(Debug, Eq, PartialEq)]
pub enum Request {
  /// Get command
  Get(String),
  /// Set command
  Set(String, String),
  /// Remove command
  Remove(String),
}

/// Kvs protocol request
#[derive(Debug, Eq, PartialEq)]
pub enum Response {
  /// Value response
  Value(Option<String>),
  /// Err response
  Err(String),
}

fn write_crlf<W: std::io::Write>(w: &mut W) -> Result<()> {
  w.write(&[b'\r', b'\n'])?;
  Ok(())
}

fn read_number<R: std::io::Read>(r: &mut R) -> Result<i64> {
  let first = read_byte(r)?;
  let mut result: i64 = if first == b'-' {
    (read_byte(r)? - b'0').into()
  } else {
    (first - b'0').into()
  };
  let buf = read_until(r, b'\r')?;
  for b in buf.iter() {
    let n = b - b'0';
    result = result * 10 + i64::from(n);
  }
  skip(r, 1)?;
  Ok(result)
}

fn write_number<W: std::io::Write>(w: &mut W, n: i64) -> Result<()> {
  w.write(n.to_string().as_ref())?;
  write_crlf(w)
}

fn read_str<R: std::io::Read>(r: &mut R) -> Result<Vec<u8>> {
  let result = read_until(r, b'\r');
  skip(r, 1)?;
  result
}

fn write_str<W: std::io::Write>(w: &mut W, s: &str) -> Result<()> {
  w.write(s.as_bytes())?;
  write_crlf(w)
}

fn read_str_array<R: std::io::Read>(r: &mut R) -> Result<Vec<String>> {
  let n = read_number(r)?;
  let mut result = Vec::new();
  for _ in 0..n {
    let s = read_str(r)?;
    result.push(String::from_utf8(s).unwrap());
  }
  skip(r, 2)?;
  Ok(result)
}

fn write_str_array<W: std::io::Write>(w: &mut W, ss: &[&str]) -> Result<()> {
  let n = ss.len();
  write_number(w, n as i64)?;
  for s in ss {
    write_str(w, *s)?;
  }
  write_crlf(w)?;
  Ok(())
}

impl Request {
  /// Decodes request from reader
  pub fn from_reader<R: std::io::Read>(r: &mut R) -> Result<Request> {
    let ss = read_str_array(r)?;
    let arg_len = ss.len();
    let req = match ss[0].as_ref() {
      "GET" => {
        if arg_len < 2 {
          return Err(ParseError("args length too short".to_string()))
        }
        Request::Get(ss[1].to_string())
      },
      "SET" => {
        if arg_len < 3 {
          return Err(ParseError("args length too short".to_string()))
        }
        Request::Set(ss[1].to_string(), ss[2].to_string())
      },
      "RM" => {
        if arg_len < 2 {
          return Err(ParseError("args length too short".to_string()))
        }
        Request::Remove(ss[1].to_string())
      },
      _ => return Err(ParseError("unknown command".to_string())),
    };
    Ok(req)
  }

  /// Encodes request to writer
  pub fn to_writer<W: std::io::Write>(&self, w: &mut W) -> Result<()> {
    match self {
      Request::Get(k) => write_str_array(w, &["GET", k.as_str()]),
      Request::Set(k, v) => write_str_array(w, &["SET", k.as_str(), v.as_str()]),
      Request::Remove(k) => write_str_array(w, &["RM", k.as_str()]),
    }
  }
}

impl Response {
  /// Decodes response from reader
  pub fn from_reader<R: std::io::Read>(r: &mut R) -> Result<Response> {
    let ss = read_str_array(r)?;
    let arg_len = ss.len();
    let rsp = match ss[0].as_ref() {
      "VALUE" => {
        if arg_len < 2 {
          return Err(ParseError("args length too short".to_string()))
        }
        Response::Value(Some(ss[1].to_string()))
      },
      "ERR" => {
        if arg_len < 2 {
          return Err(ParseError("args length too short".to_string()))
        }
        Response::Err(ss[1].to_string())
      },
      "NULL" => Response::Value(None),
      _ => return Err(ParseError("unknown response type".to_string())),
    };
    Ok(rsp)
  }

  /// Encodes request to writer
  pub fn to_writer<W: std::io::Write>(&self, w: &mut W) -> Result<()> {
    match self {
      Response::Value(Option::Some(s)) => write_str_array(w, &["VALUE", s]),
      Response::Value(Option::None) => write_str_array(w, &["NULL"]),
      Response::Err(e) => write_str_array(w, &["ERR", e]),
    }
  }
}

fn skip<R: std::io::Read>(r: &mut R, limit: u64) -> Result<()> {
  r.read_exact(vec![0 as u8; limit as usize].as_mut())?;
  Ok(())
}

fn read_byte<R: std::io::Read>(r: &mut R) -> Result<u8> {
  let mut buf = [0 as u8; 1];
  r.read_exact(&mut buf)?;
  Ok(buf[0])
}

fn write_byte<W: std::io::Write>(w: &mut W, b: u8) -> Result<()> {
  w.write(&[b])?;
  Ok(())
}

fn read_until<R: std::io::Read>(r: &mut R, end: u8) -> Result<Vec<u8>> {
  let mut result: Vec<u8> = Vec::new();
  loop {
    let b = read_byte(r)?;
    if b == end {
      break;
    }
    result.push(b);
  }
  Ok(result)
}

#[cfg(test)]
mod tests {
  #[test]
  fn request() {
    let get_req = "2\r\n\
                        GET\r\n\
                        hello\r\n\r\n";
    let mut c = std::io::Cursor::new(get_req.as_bytes());
    let req = super::Request::from_reader(&mut c).unwrap();
    assert_eq!(req, super::Request::Get("hello".to_string()));
    let mut c = std::io::Cursor::new(Vec::<u8>::new());
    req.to_writer(&mut c).unwrap();
    assert_eq!(String::from_utf8(c.into_inner()).unwrap(), get_req);
  }
  #[test]
  fn response() {
    let get_rsp = "2\r\n\
                        VALUE\r\n\
                        world\r\n\r\n";
    let mut c = std::io::Cursor::new(get_rsp.as_bytes());
    let rsp = super::Response::from_reader(&mut c).unwrap();
    assert_eq!(rsp, super::Response::Value(Some("world".to_string())));
    let mut c = std::io::Cursor::new(Vec::<u8>::new());
    rsp.to_writer(&mut c).unwrap();
    assert_eq!(String::from_utf8(c.into_inner()).unwrap(), get_rsp);
  }
}
