//! The service's event stream, `GET /api/event`, read over a direct HTTP
//! connection to the private loopback service.
//!
//! `opencode api` prints an answer only once its body has ended, and this
//! body never ends, so the stream is read here: HTTP/1.1 with the service's
//! password as basic auth, the body chunked or delimited by the connection's
//! close, server-sent events whose `data` is one JSON event each. OpenCode
//! sends `server.connected` first; events from before a connection, or during
//! a gap between two, are not replayed.

use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use serde_json::Value;

/// How long connecting to the loopback service may take.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Why a stream ended.
#[derive(Debug)]
pub(crate) enum Ended {
    /// The connection failed or closed: a new one may succeed.
    Lost,
    /// The service answered with a status other than 200: no new connection
    /// to the same service changes that.
    Refused(u16),
}

/// Read `GET /api/event` from `endpoint` (`http://host:port`) and hand each
/// event to `on_event`, until the stream ends.
pub(crate) fn subscribe(endpoint: &str, password: &str, mut on_event: impl FnMut(Value)) -> Ended {
    read(endpoint, password, &mut on_event).unwrap_or(Ended::Lost)
}

fn read(endpoint: &str, password: &str, on_event: &mut impl FnMut(Value)) -> io::Result<Ended> {
    let authority = endpoint
        .strip_prefix("http://")
        .map(|rest| rest.trim_end_matches('/'))
        .filter(|authority| !authority.is_empty() && !authority.contains('/'))
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "not an http:// endpoint"))?;
    let address = std::net::ToSocketAddrs::to_socket_addrs(authority)?
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "no address"))?;
    let mut stream = TcpStream::connect_timeout(&address, CONNECT_TIMEOUT)?;
    let credential = base64(format!("opencode:{password}").as_bytes());
    write!(
        stream,
        "GET /api/event HTTP/1.1\r\nHost: {authority}\r\nAuthorization: Basic {credential}\r\nAccept: text/event-stream\r\nCache-Control: no-cache\r\n\r\n"
    )?;
    stream.flush()?;
    let mut head = BufReader::new(stream);
    let mut line = String::new();
    head.read_line(&mut line)?;
    let status = line
        .strip_prefix("HTTP/1.")
        .and_then(|rest| rest.split_whitespace().nth(1))
        .and_then(|status| status.parse::<u16>().ok())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "no HTTP status line"))?;
    let mut chunked = false;
    loop {
        line.clear();
        if head.read_line(&mut line)? == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        let header = line.trim_end();
        if header.is_empty() {
            break;
        }
        if let Some((name, value)) = header.split_once(':')
            && name.trim().eq_ignore_ascii_case("transfer-encoding")
            && value
                .split(',')
                .any(|coding| coding.trim().eq_ignore_ascii_case("chunked"))
        {
            chunked = true;
        }
    }
    if status != 200 {
        return Ok(Ended::Refused(status));
    }
    let body: Box<dyn Read> = if chunked {
        Box::new(Chunked {
            inner: head,
            left: 0,
            trailing_crlf: false,
        })
    } else {
        Box::new(head)
    };
    let mut body = BufReader::new(body);
    let mut data = String::new();
    loop {
        line.clear();
        if body.read_line(&mut line)? == 0 {
            return Ok(Ended::Lost);
        }
        let field = line.trim_end_matches(['\r', '\n']);
        if field.is_empty() {
            // One event ends at a blank line; one that is not JSON is not
            // OpenCode's and carries nothing to read.
            if !data.is_empty()
                && let Ok(event) = serde_json::from_str::<Value>(&data)
            {
                on_event(event);
            }
            data.clear();
        } else if let Some(value) = field.strip_prefix("data:") {
            if !data.is_empty() {
                data.push('\n');
            }
            data.push_str(value.strip_prefix(' ').unwrap_or(value));
        }
    }
}

/// A chunked HTTP/1.1 body.
struct Chunked<R> {
    inner: R,
    /// Bytes left in the current chunk.
    left: usize,
    /// Whether the previous chunk's trailing CRLF still needs to be consumed.
    trailing_crlf: bool,
}

impl<R: BufRead> Read for Chunked<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.trailing_crlf {
            let mut end = String::new();
            if self.inner.read_line(&mut end)? == 0 {
                return Err(io::ErrorKind::UnexpectedEof.into());
            }
            self.trailing_crlf = false;
        }
        if self.left == 0 {
            let mut size = String::new();
            if self.inner.read_line(&mut size)? == 0 {
                return Err(io::ErrorKind::UnexpectedEof.into());
            }
            let size = size.trim_end();
            let size = size.split(';').next().unwrap_or_default().trim();
            self.left = usize::from_str_radix(size, 16)
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "bad chunk size"))?;
            if self.left == 0 {
                return Ok(0);
            }
        }
        let want = buf.len().min(self.left);
        let read = self.inner.read(&mut buf[..want])?;
        if read == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        self.left -= read;
        if self.left == 0 {
            self.trailing_crlf = true;
        }
        Ok(read)
    }
}

/// Standard base64, padded.
fn base64(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for group in bytes.chunks(3) {
        let n = group
            .iter()
            .enumerate()
            .fold(0u32, |n, (i, byte)| n | (u32::from(*byte) << (16 - 8 * i)));
        for i in 0..4 {
            if i <= group.len() {
                out.push(ALPHABET[(n >> (18 - 6 * i)) as usize & 63] as char);
            } else {
                out.push('=');
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base64_matches_the_standard() {
        assert_eq!(base64(b""), "");
        assert_eq!(base64(b"f"), "Zg==");
        assert_eq!(base64(b"fo"), "Zm8=");
        assert_eq!(base64(b"foo"), "Zm9v");
        assert_eq!(base64(b"opencode:pw"), "b3BlbmNvZGU6cHc=");
    }

    #[test]
    fn a_chunked_body_reads_whole() {
        let wire = b"5\r\nhello\r\n7;ext=1\r\n, world\r\n0\r\n\r\n";
        let mut body = String::new();
        Chunked {
            inner: &wire[..],
            left: 0,
            trailing_crlf: false,
        }
        .read_to_string(&mut body)
        .unwrap();
        assert_eq!(body, "hello, world");
    }

    #[test]
    fn a_chunk_is_handed_up_before_the_next_socket_read() {
        let wire = b"b\r\ndata: {}\n\n\n";
        let mut reader = BufReader::new(Chunked {
            inner: &wire[..],
            left: 0,
            trailing_crlf: false,
        });
        let mut line = String::new();
        assert!(reader.read_line(&mut line).unwrap() > 0);
        assert_eq!(line, "data: {}\n");
        line.clear();
        assert!(reader.read_line(&mut line).unwrap() > 0);
        assert_eq!(line, "\n");
    }
}
