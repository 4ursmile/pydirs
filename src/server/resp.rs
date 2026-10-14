//! RESP2 request parsing and reply encoding.

use std::io::{self, BufRead, Read, Write};

/// Largest accepted bulk string (512 MiB, same as Redis).
pub(crate) const MAX_BULK_LEN: usize = 512 * 1024 * 1024;
/// Largest accepted number of arguments in one multibulk request.
pub(crate) const MAX_ARGS: usize = 1_048_576;
/// Longest accepted header or inline line (excluding CRLF), in bytes.
pub(crate) const MAX_LINE_LEN: usize = 64 * 1024;

/// A reply sent back to the client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Reply {
    /// `+...` status line.
    Simple(String),
    /// `-...` error line; must include its own prefix such as `ERR`.
    Error(String),
    /// `:n` integer.
    Int(i64),
    /// `$len` bulk string.
    Bulk(Vec<u8>),
    /// `$-1` null bulk string.
    Nil,
    /// `*n` array.
    Array(Vec<Reply>),
}

/// Failure while reading a request.
#[derive(Debug)]
pub(crate) enum ProtoError {
    /// Socket error or unexpected end of stream inside a request.
    Io,
    /// Malformed or over-limit request; the connection should be closed.
    Protocol(String),
}

impl From<io::Error> for ProtoError {
    fn from(_: io::Error) -> Self {
        ProtoError::Io
    }
}

fn proto(msg: impl Into<String>) -> ProtoError {
    ProtoError::Protocol(msg.into())
}

fn eof() -> ProtoError {
    ProtoError::Io
}

/// Reads one request. Returns `Ok(None)` on clean EOF between requests. An
/// empty `Vec` means an empty request (`*0`, `*-1` or a blank inline line) and
/// should be ignored.
pub(crate) fn read_command<R: BufRead>(r: &mut R) -> Result<Option<Vec<Vec<u8>>>, ProtoError> {
    let first = match r.fill_buf()? {
        [] => return Ok(None),
        buf => buf[0],
    };
    if first != b'*' {
        return match read_line(r)? {
            None => Ok(None),
            Some(line) => split_inline(&line)
                .map(Some)
                .ok_or_else(|| proto("unbalanced quotes in request")),
        };
    }

    // Once the '*' has been seen, EOF means the request was cut short.
    let header = read_line(r)?.ok_or_else(eof)?;
    let count = parse_int(&header[1..]).ok_or_else(|| proto("invalid multibulk length"))?;
    if count <= 0 {
        return Ok(Some(Vec::new()));
    }
    if count > MAX_ARGS as i64 {
        return Err(proto("invalid multibulk length"));
    }

    let count = count as usize;
    let mut args = Vec::with_capacity(count.min(1024));
    for _ in 0..count {
        let line = read_line(r)?.ok_or_else(eof)?;
        if line.first().copied() != Some(b'$') {
            return Err(proto("expected '$' bulk string header"));
        }
        let len = match parse_int(&line[1..]) {
            Some(n) if (0..=MAX_BULK_LEN as i64).contains(&n) => n as usize,
            _ => return Err(proto("invalid bulk length")),
        };
        args.push(read_bulk(r, len)?);
    }
    Ok(Some(args))
}

/// Reads one line terminated by `\n` (an optional preceding `\r` is stripped).
/// Content is capped at `MAX_LINE_LEN` bytes so hostile input cannot grow memory.
/// Returns `Ok(None)` if the stream ends before a newline arrives.
fn read_line<R: BufRead>(r: &mut R) -> Result<Option<Vec<u8>>, ProtoError> {
    // Content plus CRLF.
    let limit = MAX_LINE_LEN as u64 + 2;
    let mut line = Vec::new();
    let n = r.by_ref().take(limit).read_until(b'\n', &mut line)?;
    if n == 0 {
        return Ok(None);
    }
    if line.last() != Some(&b'\n') {
        if line.len() as u64 >= limit {
            return Err(proto("too big line in request"));
        }
        return Ok(None);
    }
    line.pop();
    if line.last() == Some(&b'\r') {
        line.pop();
    }
    Ok(Some(line))
}

/// Reads `len` bytes followed by CRLF. The buffer grows as data actually arrives
/// instead of being allocated up front from the declared length.
fn read_bulk<R: BufRead>(r: &mut R, len: usize) -> Result<Vec<u8>, ProtoError> {
    let mut data = Vec::with_capacity(len.min(64 * 1024));
    let got = r.by_ref().take(len as u64).read_to_end(&mut data)?;
    if got != len {
        return Err(eof());
    }
    let mut crlf = [0u8; 2];
    r.read_exact(&mut crlf)?;
    if &crlf != b"\r\n" {
        return Err(proto("bulk string not terminated by CRLF"));
    }
    Ok(data)
}

fn parse_int(b: &[u8]) -> Option<i64> {
    std::str::from_utf8(b).ok()?.parse().ok()
}

/// Splits an inline command line on whitespace. Supports `"..."` (with `\n`,
/// `\r`, `\t`, `\\`, `\"` escapes) and `'...'` (with a `\'` escape). Returns
/// `None` on an unbalanced quote.
pub(crate) fn split_inline(line: &[u8]) -> Option<Vec<Vec<u8>>> {
    let n = line.len();
    let mut out = Vec::new();
    let mut i = 0;
    loop {
        while i < n && line[i].is_ascii_whitespace() {
            i += 1;
        }
        if i >= n {
            break;
        }
        let mut cur = Vec::new();
        match line[i] {
            b'"' => {
                i += 1;
                loop {
                    if i >= n {
                        return None;
                    }
                    match line[i] {
                        b'"' => {
                            i += 1;
                            break;
                        }
                        b'\\' if i + 1 < n => {
                            cur.push(match line[i + 1] {
                                b'n' => b'\n',
                                b'r' => b'\r',
                                b't' => b'\t',
                                c => c,
                            });
                            i += 2;
                        }
                        c => {
                            cur.push(c);
                            i += 1;
                        }
                    }
                }
            }
            b'\'' => {
                i += 1;
                loop {
                    if i >= n {
                        return None;
                    }
                    match line[i] {
                        b'\'' => {
                            i += 1;
                            break;
                        }
                        b'\\' if i + 1 < n && line[i + 1] == b'\'' => {
                            cur.push(b'\'');
                            i += 2;
                        }
                        c => {
                            cur.push(c);
                            i += 1;
                        }
                    }
                }
            }
            _ => {
                while i < n && !line[i].is_ascii_whitespace() {
                    cur.push(line[i]);
                    i += 1;
                }
            }
        }
        out.push(cur);
    }
    Some(out)
}

/// Encodes a reply in RESP2.
pub(crate) fn write_reply<W: Write>(w: &mut W, reply: &Reply) -> io::Result<()> {
    match reply {
        Reply::Simple(s) => {
            w.write_all(b"+")?;
            w.write_all(s.replace(['\r', '\n'], " ").as_bytes())?;
            w.write_all(b"\r\n")
        }
        Reply::Error(s) => {
            w.write_all(b"-")?;
            w.write_all(s.replace(['\r', '\n'], " ").as_bytes())?;
            w.write_all(b"\r\n")
        }
        Reply::Int(n) => write!(w, ":{n}\r\n"),
        Reply::Bulk(b) => {
            write!(w, "${}\r\n", b.len())?;
            w.write_all(b)?;
            w.write_all(b"\r\n")
        }
        Reply::Nil => w.write_all(b"$-1\r\n"),
        Reply::Array(items) => {
            write!(w, "*{}\r\n", items.len())?;
            for item in items {
                write_reply(w, item)?;
            }
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn parse(input: &[u8]) -> Result<Option<Vec<Vec<u8>>>, String> {
        let mut cur = Cursor::new(input);
        read_command(&mut cur).map_err(|e| match e {
            ProtoError::Io => "io".to_string(),
            ProtoError::Protocol(m) => m,
        })
    }

    fn strs(v: &[&str]) -> Vec<Vec<u8>> {
        v.iter().map(|s| s.as_bytes().to_vec()).collect()
    }

    #[test]
    fn parses_multibulk() {
        let got = parse(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$5\r\nhello\r\n").unwrap();
        assert_eq!(got, Some(strs(&["SET", "k", "hello"])));
    }

    #[test]
    fn bulk_may_contain_crlf() {
        let got = parse(b"*1\r\n$4\r\na\r\nb\r\n").unwrap();
        assert_eq!(got, Some(vec![b"a\r\nb".to_vec()]));
    }

    #[test]
    fn empty_bulk_is_allowed() {
        let got = parse(b"*2\r\n$3\r\nGET\r\n$0\r\n\r\n").unwrap();
        assert_eq!(got, Some(vec![b"GET".to_vec(), vec![]]));
    }

    #[test]
    fn empty_multibulk_is_empty_request() {
        assert_eq!(parse(b"*0\r\n").unwrap(), Some(vec![]));
        assert_eq!(parse(b"*-1\r\n").unwrap(), Some(vec![]));
    }

    #[test]
    fn parses_inline_with_quotes() {
        let got = parse(b"SET a \"hello world\" 'it''s'\r\n").unwrap();
        assert_eq!(got, Some(strs(&["SET", "a", "hello world", "it", "s"])));
        let got = parse(b"ECHO \"a\\tb\\\"c\"\n").unwrap();
        assert_eq!(got, Some(vec![b"ECHO".to_vec(), b"a\tb\"c".to_vec()]));
        let got = parse(b"ECHO 'x\\'y'\n").unwrap();
        assert_eq!(got, Some(vec![b"ECHO".to_vec(), b"x'y".to_vec()]));
    }

    #[test]
    fn blank_inline_line_is_empty_request() {
        assert_eq!(parse(b"\r\n").unwrap(), Some(vec![]));
    }

    #[test]
    fn unbalanced_quotes_are_protocol_error() {
        assert!(parse(b"SET a \"oops\r\n")
            .unwrap_err()
            .contains("unbalanced"));
    }

    #[test]
    fn eof_returns_none() {
        assert_eq!(parse(b"").unwrap(), None);
        assert_eq!(parse(b"PING").unwrap(), None);
    }

    #[test]
    fn eof_inside_multibulk_is_io_error() {
        assert!(parse(b"*2\r\n$3\r\nGET\r\n").unwrap_err() == "io");
        assert!(parse(b"*2\r\n$3\r\nGET").unwrap_err() == "io");
        assert_eq!(parse(b"*2\r\n$3\r\nGET\r\n$5\r\nab").unwrap_err(), "io");
    }

    #[test]
    fn pipelined_commands_parse_in_sequence() {
        let mut cur = Cursor::new(&b"PING\r\n*1\r\n$4\r\nQUIT\r\n"[..]);
        assert_eq!(read_command(&mut cur).unwrap(), Some(strs(&["PING"])));
        assert_eq!(read_command(&mut cur).unwrap(), Some(strs(&["QUIT"])));
        assert_eq!(read_command(&mut cur).unwrap(), None);
    }

    #[test]
    fn rejects_oversized_bulk_header() {
        let input = format!("*1\r\n${}\r\n", MAX_BULK_LEN + 1);
        assert!(parse(input.as_bytes())
            .unwrap_err()
            .contains("invalid bulk length"));
        assert!(parse(b"*1\r\n$-1\r\n")
            .unwrap_err()
            .contains("invalid bulk length"));
    }

    #[test]
    fn bulk_at_limit_header_waits_for_body() {
        // The header is accepted; without a body the read fails as I/O.
        let input = format!("*1\r\n${}\r\n", MAX_BULK_LEN);
        assert!(parse(input.as_bytes()).unwrap_err() == "io");
    }

    #[test]
    fn rejects_too_many_args() {
        let input = format!("*{}\r\n", MAX_ARGS + 1);
        assert!(parse(input.as_bytes())
            .unwrap_err()
            .contains("multibulk length"));
    }

    #[test]
    fn rejects_overlong_line() {
        let mut input = vec![b'a'; MAX_LINE_LEN + 10];
        input.extend_from_slice(b"\r\n");
        assert!(parse(&input).unwrap_err().contains("too big line"));
    }

    #[test]
    fn accepts_line_at_limit() {
        let mut input = vec![b'a'; MAX_LINE_LEN];
        input.extend_from_slice(b"\r\n");
        assert_eq!(parse(&input).unwrap(), Some(vec![vec![b'a'; MAX_LINE_LEN]]));
    }

    #[test]
    fn missing_dollar_is_protocol_error() {
        assert!(parse(b"*1\r\nPING\r\n")
            .unwrap_err()
            .contains("expected '$'"));
    }

    #[test]
    fn bulk_without_crlf_is_protocol_error() {
        assert!(parse(b"*1\r\n$2\r\nabXY").unwrap_err().contains("CRLF"));
    }

    #[test]
    fn non_numeric_counts_are_protocol_errors() {
        assert!(parse(b"*x\r\n").unwrap_err().contains("multibulk length"));
        assert!(parse(b"*1\r\n$x\r\n").unwrap_err().contains("bulk length"));
    }

    #[test]
    fn encodes_all_reply_types() {
        let mut out = Vec::new();
        write_reply(&mut out, &Reply::Simple("OK".into())).unwrap();
        write_reply(&mut out, &Reply::Error("ERR bad\r\nx".into())).unwrap();
        write_reply(&mut out, &Reply::Int(-7)).unwrap();
        write_reply(&mut out, &Reply::Bulk(b"hi".to_vec())).unwrap();
        write_reply(&mut out, &Reply::Nil).unwrap();
        write_reply(
            &mut out,
            &Reply::Array(vec![Reply::Int(1), Reply::Nil, Reply::Bulk(vec![])]),
        )
        .unwrap();
        assert_eq!(
            out,
            b"+OK\r\n-ERR bad  x\r\n:-7\r\n$2\r\nhi\r\n$-1\r\n*3\r\n:1\r\n$-1\r\n$0\r\n\r\n"
        );
    }
}
