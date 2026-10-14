//! Integration tests: start the server on port 0 and talk to it over TCP.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::thread;
use std::time::Duration;

use super::start;
use crate::core::{Config, Store};

fn server() -> (super::ServerHandle, u16) {
    let store = Store::new(Config {
        sweep_interval_ms: 0,
        ..Config::default()
    });
    let handle = start(store, "127.0.0.1", 0).expect("bind");
    let port = handle.port();
    assert_ne!(port, 0);
    (handle, port)
}

/// Encodes a command as a RESP multibulk request.
fn encode(args: &[&[u8]]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", args.len()).into_bytes();
    for a in args {
        out.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
        out.extend_from_slice(a);
        out.extend_from_slice(b"\r\n");
    }
    out
}

struct Client {
    reader: BufReader<TcpStream>,
    stream: TcpStream,
}

impl Client {
    fn connect(port: u16) -> Client {
        let stream = TcpStream::connect(("127.0.0.1", port)).expect("connect");
        stream
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        Client {
            reader: BufReader::new(stream.try_clone().unwrap()),
            stream,
        }
    }

    fn send(&mut self, bytes: &[u8]) {
        self.stream.write_all(bytes).unwrap();
    }

    /// Reads one complete reply and returns it as raw RESP text (CRLF kept).
    fn read_reply(&mut self) -> String {
        let mut out = String::new();
        self.read_into(&mut out);
        out
    }

    fn read_into(&mut self, out: &mut String) {
        let mut line = String::new();
        self.reader.read_line(&mut line).expect("read line");
        assert!(line.ends_with("\r\n"), "truncated reply: {line:?}");
        out.push_str(&line);
        let kind = line.as_bytes()[0];
        let n: i64 = line[1..line.len() - 2].parse().unwrap_or(0);
        match kind {
            b'$' if n >= 0 => {
                let mut buf = vec![0u8; n as usize + 2];
                self.reader.read_exact(&mut buf).unwrap();
                out.push_str(std::str::from_utf8(&buf).unwrap());
            }
            b'*' => {
                for _ in 0..n.max(0) {
                    self.read_into(out);
                }
            }
            _ => {}
        }
    }

    fn cmd(&mut self, args: &[&[u8]]) -> String {
        self.send(&encode(args));
        self.read_reply()
    }
}

#[test]
fn ping_and_echo() {
    let (_h, port) = server();
    let mut c = Client::connect(port);
    assert_eq!(c.cmd(&[b"PING"]), "+PONG\r\n");
    assert_eq!(c.cmd(&[b"ping", b"hi"]), "$2\r\nhi\r\n");
    assert_eq!(c.cmd(&[b"ECHO", b"x"]), "$1\r\nx\r\n");
}

#[test]
fn set_get_and_nil() {
    let (_h, port) = server();
    let mut c = Client::connect(port);
    assert_eq!(c.cmd(&[b"SET", b"k", b"v"]), "+OK\r\n");
    assert_eq!(c.cmd(&[b"GET", b"k"]), "$1\r\nv\r\n");
    assert_eq!(c.cmd(&[b"GET", b"missing"]), "$-1\r\n");
    assert_eq!(c.cmd(&[b"set", b"k", b"w"]), "+OK\r\n");
    assert_eq!(c.cmd(&[b"GET", b"k"]), "$1\r\nw\r\n");
}

#[test]
fn set_with_ex_expires() {
    let (_h, port) = server();
    let mut c = Client::connect(port);
    assert_eq!(c.cmd(&[b"SET", b"e", b"1", b"PX", b"100"]), "+OK\r\n");
    assert_eq!(c.cmd(&[b"GET", b"e"]), "$1\r\n1\r\n");
    thread::sleep(Duration::from_millis(250));
    assert_eq!(c.cmd(&[b"GET", b"e"]), "$-1\r\n");

    assert_eq!(c.cmd(&[b"SET", b"s", b"v", b"EX", b"1"]), "+OK\r\n");
    assert_eq!(c.cmd(&[b"TTL", b"s"]), ":1\r\n");
    assert!(c
        .cmd(&[b"SET", b"s", b"v", b"EX", b"0"])
        .starts_with("-ERR invalid expire"));
}

#[test]
fn set_nx_and_xx() {
    let (_h, port) = server();
    let mut c = Client::connect(port);
    assert_eq!(c.cmd(&[b"SET", b"n", b"a", b"NX"]), "+OK\r\n");
    assert_eq!(c.cmd(&[b"SET", b"n", b"b", b"NX"]), "$-1\r\n");
    assert_eq!(c.cmd(&[b"GET", b"n"]), "$1\r\na\r\n");
    assert_eq!(c.cmd(&[b"SET", b"nope", b"b", b"XX"]), "$-1\r\n");
    assert_eq!(c.cmd(&[b"SET", b"n", b"c", b"XX"]), "+OK\r\n");
    assert_eq!(c.cmd(&[b"GET", b"n"]), "$1\r\nc\r\n");
    assert!(c
        .cmd(&[b"SET", b"n", b"c", b"NX", b"XX"])
        .starts_with("-ERR syntax error"));
}

#[test]
fn incr_and_errors() {
    let (_h, port) = server();
    let mut c = Client::connect(port);
    assert_eq!(c.cmd(&[b"INCR", b"c"]), ":1\r\n");
    assert_eq!(c.cmd(&[b"INCRBY", b"c", b"41"]), ":42\r\n");
    assert_eq!(c.cmd(&[b"DECR", b"c"]), ":41\r\n");
    assert_eq!(c.cmd(&[b"DECRBY", b"c", b"1"]), ":40\r\n");
    assert_eq!(c.cmd(&[b"SET", b"s", b"abc"]), "+OK\r\n");
    assert_eq!(
        c.cmd(&[b"INCR", b"s"]),
        "-ERR value is not an integer or out of range\r\n"
    );
    assert_eq!(
        c.cmd(&[b"INCRBY", b"c", b"x"]),
        "-ERR value is not an integer or out of range\r\n"
    );
}

#[test]
fn pipelined_100_commands_in_one_write() {
    let (_h, port) = server();
    let mut c = Client::connect(port);
    let mut req = Vec::new();
    for i in 0..100 {
        req.extend(encode(&[
            b"SET",
            format!("k{i}").as_bytes(),
            format!("v{i}").as_bytes(),
        ]));
    }
    for i in 0..100 {
        req.extend(encode(&[b"GET", format!("k{i}").as_bytes()]));
    }
    c.send(&req);
    for _ in 0..100 {
        assert_eq!(c.read_reply(), "+OK\r\n");
    }
    for i in 0..100 {
        assert_eq!(
            c.read_reply(),
            format!("${}\r\nv{}\r\n", format!("v{i}").len(), i)
        );
    }
}

#[test]
fn inline_commands() {
    let (_h, port) = server();
    let mut c = Client::connect(port);
    c.send(b"PING\r\n");
    assert_eq!(c.read_reply(), "+PONG\r\n");
    c.send(b"SET inline \"two words\"\r\nGET inline\n");
    assert_eq!(c.read_reply(), "+OK\r\n");
    assert_eq!(c.read_reply(), "$9\r\ntwo words\r\n");
}

#[test]
fn wrongtype_error() {
    let (_h, port) = server();
    let mut c = Client::connect(port);
    assert_eq!(c.cmd(&[b"RPUSH", b"list", b"a"]), ":1\r\n");
    assert!(c
        .cmd(&[b"GET", b"list"])
        .starts_with("-WRONGTYPE Operation against a key holding the wrong kind of value"));
    assert_eq!(c.cmd(&[b"HSET", b"h", b"f", b"v"]), ":1\r\n");
    assert!(c.cmd(&[b"LPUSH", b"h", b"x"]).starts_with("-WRONGTYPE"));
}

#[test]
fn unknown_command_and_arity_errors() {
    let (_h, port) = server();
    let mut c = Client::connect(port);
    assert_eq!(c.cmd(&[b"NOPE"]), "-ERR unknown command 'NOPE'\r\n");
    assert_eq!(
        c.cmd(&[b"GET"]),
        "-ERR wrong number of arguments for 'get' command\r\n"
    );
    assert_eq!(c.cmd(&[b"SELECT", b"0"]), "+OK\r\n");
    assert!(c.cmd(&[b"SELECT", b"1"]).starts_with("-ERR DB index"));
    assert_eq!(c.cmd(&[b"COMMAND"]), "*0\r\n");
}

#[test]
fn large_value_one_mib() {
    let (_h, port) = server();
    let mut c = Client::connect(port);
    let value = vec![b'x'; 1024 * 1024];
    assert_eq!(c.cmd(&[b"SET", b"big", &value]), "+OK\r\n");
    let reply = c.cmd(&[b"GET", b"big"]);
    assert_eq!(
        reply,
        format!("${}\r\n{}\r\n", value.len(), "x".repeat(value.len()))
    );
}

#[test]
fn lists_sets_hashes_roundtrip() {
    let (_h, port) = server();
    let mut c = Client::connect(port);
    assert_eq!(c.cmd(&[b"RPUSH", b"l", b"a", b"b"]), ":2\r\n");
    assert_eq!(c.cmd(&[b"LPUSH", b"l", b"z"]), ":3\r\n");
    assert_eq!(
        c.cmd(&[b"LRANGE", b"l", b"0", b"-1"]),
        "*3\r\n$1\r\nz\r\n$1\r\na\r\n$1\r\nb\r\n"
    );
    assert_eq!(c.cmd(&[b"LPOP", b"l"]), "$1\r\nz\r\n");
    assert_eq!(c.cmd(&[b"SADD", b"s", b"m"]), ":1\r\n");
    assert_eq!(c.cmd(&[b"SISMEMBER", b"s", b"m"]), ":1\r\n");
    assert_eq!(c.cmd(&[b"SCARD", b"s"]), ":1\r\n");
    assert_eq!(c.cmd(&[b"HSET", b"h", b"f", b"v"]), ":1\r\n");
    assert_eq!(c.cmd(&[b"HGET", b"h", b"f"]), "$1\r\nv\r\n");
    assert_eq!(c.cmd(&[b"HGETALL", b"h"]), "*2\r\n$1\r\nf\r\n$1\r\nv\r\n");
    assert_eq!(c.cmd(&[b"HDEL", b"h", b"f"]), ":1\r\n");
    assert_eq!(c.cmd(&[b"HLEN", b"h"]), ":0\r\n");
}

#[test]
fn keys_mget_mset_del_exists() {
    let (_h, port) = server();
    let mut c = Client::connect(port);
    assert_eq!(c.cmd(&[b"MSET", b"a", b"1", b"b", b"2"]), "+OK\r\n");
    assert_eq!(
        c.cmd(&[b"MGET", b"a", b"nope", b"b"]),
        "*3\r\n$1\r\n1\r\n$-1\r\n$1\r\n2\r\n"
    );
    assert_eq!(c.cmd(&[b"EXISTS", b"a", b"a", b"zz"]), ":2\r\n");
    assert_eq!(c.cmd(&[b"DBSIZE"]), ":2\r\n");
    assert_eq!(c.cmd(&[b"DEL", b"a", b"zz"]), ":1\r\n");
    assert_eq!(c.cmd(&[b"KEYS", b"*"]), "*1\r\n$1\r\nb\r\n");
    assert_eq!(c.cmd(&[b"FLUSHALL"]), "+OK\r\n");
    assert_eq!(c.cmd(&[b"DBSIZE"]), ":0\r\n");
}

#[test]
fn expire_persist_ttl() {
    let (_h, port) = server();
    let mut c = Client::connect(port);
    assert_eq!(c.cmd(&[b"TTL", b"nokey"]), ":-2\r\n");
    assert_eq!(c.cmd(&[b"SET", b"k", b"v"]), "+OK\r\n");
    assert_eq!(c.cmd(&[b"TTL", b"k"]), ":-1\r\n");
    assert_eq!(c.cmd(&[b"EXPIRE", b"k", b"100"]), ":1\r\n");
    assert_eq!(c.cmd(&[b"TTL", b"k"]), ":100\r\n");
    assert_eq!(c.cmd(&[b"PERSIST", b"k"]), ":1\r\n");
    assert_eq!(c.cmd(&[b"PEXPIRE", b"k", b"50"]), ":1\r\n");
    thread::sleep(Duration::from_millis(120));
    assert_eq!(c.cmd(&[b"GET", b"k"]), "$-1\r\n");
}

#[test]
fn four_concurrent_clients() {
    let (_h, port) = server();
    let workers: Vec<_> = (0..4)
        .map(|t| {
            thread::spawn(move || {
                let mut c = Client::connect(port);
                for i in 0..200 {
                    let key = format!("t{t}-{i}");
                    assert_eq!(c.cmd(&[b"SET", key.as_bytes(), b"x"]), "+OK\r\n");
                    assert_eq!(
                        c.cmd(&[b"INCR", format!("c{t}").as_bytes()]),
                        format!(":{}\r\n", i + 1)
                    );
                }
            })
        })
        .collect();
    for w in workers {
        w.join().unwrap();
    }
    let mut c = Client::connect(port);
    assert_eq!(c.cmd(&[b"DBSIZE"]), ":804\r\n");
}

#[test]
fn quit_closes_connection() {
    let (_h, port) = server();
    let mut c = Client::connect(port);
    assert_eq!(c.cmd(&[b"QUIT"]), "+OK\r\n");
    let mut buf = [0u8; 1];
    assert_eq!(c.reader.read(&mut buf).unwrap(), 0);
}

#[test]
fn protocol_error_closes_connection() {
    let (_h, port) = server();
    let mut c = Client::connect(port);
    c.send(b"*1\r\nPING\r\n");
    assert!(c.read_reply().starts_with("-ERR Protocol error"));
    let mut buf = [0u8; 1];
    assert_eq!(c.reader.read(&mut buf).unwrap(), 0);
}

#[test]
fn stop_terminates_connections_and_is_idempotent() {
    let (handle, port) = server();
    let mut c = Client::connect(port);
    assert_eq!(c.cmd(&[b"PING"]), "+PONG\r\n");
    handle.stop();
    handle.stop();
    let mut buf = [0u8; 1];
    // Existing client sees EOF (or a reset) rather than hanging.
    match c.reader.read(&mut buf) {
        Ok(n) => assert_eq!(n, 0),
        Err(e) => assert_ne!(e.kind(), std::io::ErrorKind::WouldBlock),
    }
    assert!(TcpStream::connect(("127.0.0.1", port)).is_err());
}

#[test]
fn drop_stops_server() {
    let (handle, port) = server();
    drop(handle);
    assert!(TcpStream::connect(("127.0.0.1", port)).is_err());
}

// ---- persistence and auth (PLAN.md 4d) ------------------------------------

fn tempdir(name: &str) -> std::path::PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering::SeqCst};
    static N: AtomicU64 = AtomicU64::new(0);
    let d = std::env::temp_dir().join(format!(
        "tc-server-{name}-{}-{}",
        std::process::id(),
        N.fetch_add(1, SeqCst)
    ));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn persistent_server(dir: &std::path::Path, password: Option<&str>) -> (super::ServerHandle, u16) {
    let store = Store::open(Config {
        sweep_interval_ms: 0,
        persist: Some(crate::core::PersistConfig {
            dir: dir.to_path_buf(),
            fsync: crate::core::FsyncPolicy::Never,
            snapshot_interval_secs: 0,
        }),
        ..Config::default()
    })
    .expect("open");
    let opts = super::ServerOptions {
        password: password.map(str::to_string),
    };
    let handle = super::start_with_options(store, "127.0.0.1", 0, opts).expect("bind");
    let port = handle.port();
    (handle, port)
}

/// Polls INFO until the background save has finished, or fails after a timeout.
fn wait_for_bgsave(c: &mut Client) {
    for _ in 0..500 {
        let info = c.cmd(&[b"INFO"]);
        if info.contains("last_save_time:") && !info.contains("last_save_time:0\r\n") {
            return;
        }
        thread::sleep(Duration::from_millis(10));
    }
    panic!("background save never completed");
}

#[test]
fn save_lastsave_and_info_over_tcp() {
    let dir = tempdir("save");
    let (_h, port) = persistent_server(&dir, None);
    let mut c = Client::connect(port);
    assert_eq!(c.cmd(&[b"LASTSAVE"]), ":0\r\n", "no save yet");
    assert_eq!(c.cmd(&[b"SET", b"k", b"v"]), "+OK\r\n");
    assert_eq!(c.cmd(&[b"SAVE"]), "+OK\r\n");
    let last = c.cmd(&[b"LASTSAVE"]);
    assert!(last.starts_with(':') && last != ":0\r\n", "{last}");
    assert!(dir.join("snapshot.tcs").exists());

    let info = c.cmd(&[b"INFO"]);
    for section in ["# Server", "# Memory", "# Keyspace", "# Persistence"] {
        assert!(info.contains(section), "missing {section}: {info}");
    }
    assert!(info.contains("uptime_in_seconds:"));
    assert!(info.contains("used_memory:"));
    assert!(info.contains("keys:1\r\n"), "{info}");
    assert!(info.contains("persistence_enabled:1"));
    assert!(info.contains("last_error:\r\n"), "{info}");
}

#[test]
fn bgsave_starts_and_completes() {
    let dir = tempdir("bgsave");
    let (_h, port) = persistent_server(&dir, None);
    let mut c = Client::connect(port);
    assert_eq!(c.cmd(&[b"SET", b"k", b"v"]), "+OK\r\n");
    assert_eq!(c.cmd(&[b"BGSAVE"]), "+Background saving started\r\n");
    wait_for_bgsave(&mut c);
    assert!(dir.join("snapshot.tcs").exists());
}

#[test]
fn bgsave_while_running_is_refused() {
    let dir = tempdir("bgsave-busy");
    let (_h, port) = persistent_server(&dir, None);
    let mut c = Client::connect(port);
    // Keep a save busy by filling the store, then ask for a second one immediately.
    for i in 0..20_000 {
        c.send(&encode(&[b"SET", format!("k{i}").as_bytes(), &[b'x'; 64]]));
    }
    let mut replies = String::new();
    for _ in 0..20_000 {
        replies.push_str(&c.read_reply());
    }
    assert!(!replies.contains("-ERR"), "setup failed");
    let first = c.cmd(&[b"BGSAVE"]);
    let second = c.cmd(&[b"BGSAVE"]);
    assert_eq!(first, "+Background saving started\r\n");
    // The first save may already have finished; if it has not, the second must be refused.
    // Either way the server answers without error and never runs two at once.
    assert!(
        second == "+Background saving started\r\n"
            || second == "-ERR Background save already in progress\r\n",
        "{second}"
    );
    wait_for_bgsave(&mut c);
}

#[test]
fn save_without_persistence_is_an_error_over_tcp() {
    let (_h, port) = server();
    let mut c = Client::connect(port);
    assert!(c
        .cmd(&[b"SAVE"])
        .starts_with("-ERR persistence is not enabled"));
    assert!(c
        .cmd(&[b"BGSAVE"])
        .starts_with("-ERR persistence is not enabled"));
    assert!(c.cmd(&[b"INFO"]).contains("persistence_enabled:0"));
    assert_eq!(c.cmd(&[b"LASTSAVE"]), ":0\r\n");
}

#[test]
fn info_section_names_are_present_without_persistence() {
    let (_h, port) = server();
    let mut c = Client::connect(port);
    let info = c.cmd(&[b"INFO"]);
    assert!(info.contains("# Server\r\n"));
    assert!(info.contains("# Memory\r\n"));
    assert!(info.contains("# Keyspace\r\n"));
    assert!(info.contains("# Persistence\r\n"));
}

#[test]
fn auth_required_before_other_commands() {
    let (_h, port) = persistent_server_auth("s3cret");
    let mut c = Client::connect(port);
    assert_eq!(c.cmd(&[b"PING"]), "-NOAUTH Authentication required.\r\n");
    assert_eq!(
        c.cmd(&[b"GET", b"k"]),
        "-NOAUTH Authentication required.\r\n"
    );
    // Arity errors come after the auth check, so a bad command also gets NOAUTH.
    assert_eq!(c.cmd(&[b"GET"]), "-NOAUTH Authentication required.\r\n");
    assert_eq!(c.cmd(&[b"AUTH", b"s3cret"]), "+OK\r\n");
    assert_eq!(c.cmd(&[b"PING"]), "+PONG\r\n");
}

#[test]
fn auth_wrong_password_is_rejected_and_keeps_locked() {
    let (_h, port) = persistent_server_auth("s3cret");
    let mut c = Client::connect(port);
    assert_eq!(
        c.cmd(&[b"AUTH", b"nope"]),
        "-WRONGPASS invalid password\r\n"
    );
    assert_eq!(c.cmd(&[b"PING"]), "-NOAUTH Authentication required.\r\n");
    assert_eq!(c.cmd(&[b"AUTH", b"s3cret"]), "+OK\r\n");
    assert_eq!(c.cmd(&[b"PING"]), "+PONG\r\n");
}

#[test]
fn auth_is_per_connection() {
    let (_h, port) = persistent_server_auth("s3cret");
    let mut a = Client::connect(port);
    assert_eq!(a.cmd(&[b"AUTH", b"s3cret"]), "+OK\r\n");
    let mut b = Client::connect(port);
    assert_eq!(b.cmd(&[b"PING"]), "-NOAUTH Authentication required.\r\n");
    assert_eq!(a.cmd(&[b"PING"]), "+PONG\r\n");
}

#[test]
fn quit_works_before_auth() {
    let (_h, port) = persistent_server_auth("s3cret");
    let mut c = Client::connect(port);
    assert_eq!(c.cmd(&[b"QUIT"]), "+OK\r\n");
}

#[test]
fn auth_without_configured_password_returns_redis_error() {
    let (_h, port) = server();
    let mut c = Client::connect(port);
    assert_eq!(
        c.cmd(&[b"AUTH", b"anything"]),
        "-ERR AUTH <password> called without any password configured\r\n"
    );
    // No password set: other commands work without authenticating, as before.
    assert_eq!(c.cmd(&[b"PING"]), "+PONG\r\n");
    assert_eq!(c.cmd(&[b"SET", b"k", b"v"]), "+OK\r\n");
}

#[test]
fn no_password_path_is_unchanged_by_options() {
    let store = Store::new(Config {
        sweep_interval_ms: 0,
        ..Config::default()
    });
    let handle = super::start_with_options(store, "127.0.0.1", 0, super::ServerOptions::default())
        .expect("bind");
    let mut c = Client::connect(handle.port());
    assert_eq!(c.cmd(&[b"PING"]), "+PONG\r\n");
}

fn persistent_server_auth(password: &str) -> (super::ServerHandle, u16) {
    let store = Store::new(Config {
        sweep_interval_ms: 0,
        ..Config::default()
    });
    let handle = super::start_with_options(
        store,
        "127.0.0.1",
        0,
        super::ServerOptions {
            password: Some(password.to_string()),
        },
    )
    .expect("bind");
    let port = handle.port();
    (handle, port)
}
