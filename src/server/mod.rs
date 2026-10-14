//! RESP2 TCP server over `std::net` that serves a [`Store`] to Redis clients.
//!
//! One accept thread plus one thread per connection. Requests may be pipelined:
//! replies are buffered and flushed once the read buffer holds no complete
//! request, so a burst of commands costs a single write.

mod commands;
mod resp;
#[cfg(test)]
mod tests;

use std::io::{self, BufReader, BufWriter, Cursor, Write};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, Shutdown, SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering::SeqCst};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::core::Store;
use resp::{ProtoError, Reply};

/// Options for [`start_with_options`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ServerOptions {
    /// When set, every connection must send `AUTH <password>` before any other command
    /// except `QUIT`. With no password, `AUTH` is refused as in Redis.
    pub password: Option<String>,
}

/// Server-wide state shared by every connection.
pub(crate) struct Ctx {
    pub(crate) store: Store,
    pub(crate) password: Option<String>,
    pub(crate) started: std::time::Instant,
    /// True while a BGSAVE thread is running. Set before the thread starts, cleared by it.
    pub(crate) bgsave_running: Arc<AtomicBool>,
}

/// Per-connection state.
#[derive(Default)]
pub(crate) struct Session {
    pub(crate) authed: bool,
}

/// Handle to a running server. Dropping it stops the server.
pub struct ServerHandle {
    port: u16,
    wake_addr: SocketAddr,
    shared: Arc<Shared>,
    accept: Mutex<Option<JoinHandle<()>>>,
}

struct Shared {
    stopping: AtomicBool,
    conns: Mutex<Vec<Conn>>,
}

struct Conn {
    /// Second handle to the socket, used to force-close it on `stop`.
    stream: TcpStream,
    handle: JoinHandle<()>,
}

impl ServerHandle {
    /// Actual bound port (useful when the server was started with port 0).
    pub fn port(&self) -> u16 {
        self.port
    }

    /// Stops accepting, closes every client socket and joins all threads.
    /// Safe to call more than once.
    pub fn stop(&self) {
        self.shared.stopping.store(true, SeqCst);
        if let Some(handle) = lock(&self.accept).take() {
            // Wake the blocked accept() with a throwaway connection. The accept loop sees
            // the flag, exits and drops the listener. A single attempt can be lost (a full
            // backlog, a slow loopback), which would leave join() blocked forever, so keep
            // knocking until the thread is gone.
            while !handle.is_finished() {
                let _ = TcpStream::connect_timeout(&self.wake_addr, Duration::from_secs(1));
                thread::sleep(Duration::from_millis(20));
            }
            let _ = handle.join();
        }
        // The accept thread is gone, so no new connection can register after this drain.
        let conns = std::mem::take(&mut *lock(&self.shared.conns));
        for conn in &conns {
            let _ = conn.stream.shutdown(Shutdown::Both);
        }
        for conn in conns {
            let _ = conn.handle.join();
        }
    }
}

impl Drop for ServerHandle {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Binds `host:port` and starts serving `store` in background threads.
/// Port 0 picks a free port; read it back with [`ServerHandle::port`].
pub fn start(store: Store, host: &str, port: u16) -> io::Result<ServerHandle> {
    start_with_options(store, host, port, ServerOptions::default())
}

/// Like [`start`], with options such as a password. With the default options it behaves
/// exactly like `start`.
pub fn start_with_options(
    store: Store,
    host: &str,
    port: u16,
    opts: ServerOptions,
) -> io::Result<ServerHandle> {
    let listener = TcpListener::bind((host, port))?;
    let local = listener.local_addr()?;
    let ip = if local.ip().is_unspecified() {
        match local {
            SocketAddr::V4(_) => IpAddr::V4(Ipv4Addr::LOCALHOST),
            SocketAddr::V6(_) => IpAddr::V6(Ipv6Addr::LOCALHOST),
        }
    } else {
        local.ip()
    };
    let shared = Arc::new(Shared {
        stopping: AtomicBool::new(false),
        conns: Mutex::new(Vec::new()),
    });
    let accept_shared = Arc::clone(&shared);
    let ctx = Arc::new(Ctx {
        store,
        password: opts.password,
        started: std::time::Instant::now(),
        bgsave_running: Arc::new(AtomicBool::new(false)),
    });
    let handle = thread::Builder::new()
        .name("pydirs-accept".into())
        .spawn(move || accept_loop(&listener, &ctx, &accept_shared))?;
    Ok(ServerHandle {
        port: local.port(),
        wake_addr: SocketAddr::new(ip, local.port()),
        shared,
        accept: Mutex::new(Some(handle)),
    })
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

fn accept_loop(listener: &TcpListener, ctx: &Arc<Ctx>, shared: &Shared) {
    loop {
        let stream = match listener.accept() {
            Ok((stream, _)) => stream,
            Err(_) => {
                if shared.stopping.load(SeqCst) {
                    break;
                }
                // Transient failure such as EMFILE: back off briefly and retry.
                thread::sleep(Duration::from_millis(10));
                continue;
            }
        };
        let mut conns = lock(&shared.conns);
        if shared.stopping.load(SeqCst) {
            break;
        }
        reap_finished(&mut conns);
        let Ok(shutdown_handle) = stream.try_clone() else {
            continue;
        };
        let ctx = Arc::clone(ctx);
        let spawned = thread::Builder::new()
            .name("pydirs-conn".into())
            .spawn(move || serve(stream, &ctx));
        if let Ok(handle) = spawned {
            conns.push(Conn {
                stream: shutdown_handle,
                handle,
            });
        }
    }
}

/// Joins and drops connection threads that have already exited.
fn reap_finished(conns: &mut Vec<Conn>) {
    let mut i = 0;
    while i < conns.len() {
        if conns[i].handle.is_finished() {
            let conn = conns.swap_remove(i);
            let _ = conn.handle.join();
        } else {
            i += 1;
        }
    }
}

fn serve(stream: TcpStream, ctx: &Ctx) {
    let _ = stream.set_nodelay(true);
    let Ok(read_half) = stream.try_clone() else {
        return;
    };
    let mut reader = BufReader::new(read_half);
    let mut writer = BufWriter::new(stream);
    let mut session = Session {
        authed: ctx.password.is_none(),
    };
    loop {
        let args = match resp::read_command(&mut reader) {
            Ok(Some(args)) => args,
            Ok(None) | Err(ProtoError::Io) => break,
            Err(ProtoError::Protocol(msg)) => {
                let reply = Reply::Error(format!("ERR Protocol error: {msg}"));
                let _ = resp::write_reply(&mut writer, &reply);
                break;
            }
        };
        if !args.is_empty() {
            let (reply, close) = commands::dispatch(ctx, &mut session, &args);
            if resp::write_reply(&mut writer, &reply).is_err() || close {
                break;
            }
        }
        // Flush before blocking on more input. Staying buffered only while a
        // complete request is already waiting keeps pipelines to one write.
        if !has_complete_request(reader.buffer()) && writer.flush().is_err() {
            break;
        }
    }
    let _ = writer.flush();
    // A clone of this socket is still held by `Conn`, so dropping our handles
    // would not close the connection. Shut it down so the peer sees EOF now.
    let _ = writer.get_ref().shutdown(Shutdown::Both);
}

/// True if `buf` already holds a full request (or one that is malformed and
/// will fail at once), so parsing it would not block.
fn has_complete_request(buf: &[u8]) -> bool {
    matches!(
        resp::read_command(&mut Cursor::new(buf)),
        Ok(Some(_)) | Err(ProtoError::Protocol(_))
    )
}
