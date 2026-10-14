//! Command table and dispatch from RESP argument lists onto [`Store`].

use std::sync::atomic::Ordering::SeqCst;
use std::sync::Arc;
use std::thread;

use crate::core::{Store, StoreError};

use super::resp::Reply;
use super::{Ctx, Session};

/// Handler result. `Err` carries the error reply to send, so `?` works in handlers.
type Out = Result<Reply, Reply>;

impl From<StoreError> for Reply {
    fn from(e: StoreError) -> Reply {
        match e {
            StoreError::NotInteger => {
                Reply::Error("ERR value is not an integer or out of range".into())
            }
            StoreError::Overflow => {
                Reply::Error("ERR increment or decrement would overflow".into())
            }
            other => Reply::Error(other.to_string()),
        }
    }
}

/// Runs one command for a connection. Returns the reply and whether the connection should
/// close. Until the session is authenticated, only `AUTH` and `QUIT` are accepted.
pub(crate) fn dispatch(ctx: &Ctx, session: &mut Session, args: &[Vec<u8>]) -> (Reply, bool) {
    let Some(first) = args.first() else {
        return (Reply::Error("ERR empty command".into()), false);
    };
    let name = first.to_ascii_uppercase();
    if name == b"QUIT" {
        return (Reply::Simple("OK".into()), true);
    }
    if name == b"AUTH" {
        return (auth(ctx, session, args), false);
    }
    if !session.authed {
        return (
            Reply::Error("NOAUTH Authentication required.".into()),
            false,
        );
    }
    let Some((min, max)) = arity(&name) else {
        let shown = String::from_utf8_lossy(first);
        return (
            Reply::Error(format!("ERR unknown command '{shown}'")),
            false,
        );
    };
    if args.len() < min || args.len() > max {
        return (wrong_args(first), false);
    }
    let reply = match execute(ctx, args, &name) {
        Ok(reply) | Err(reply) => reply,
    };
    (reply, false)
}

/// `AUTH password`. Compares in constant time with respect to the password's contents.
fn auth(ctx: &Ctx, session: &mut Session, args: &[Vec<u8>]) -> Reply {
    if args.len() != 2 {
        return wrong_args(&args[0]);
    }
    let Some(expected) = ctx.password.as_deref() else {
        return Reply::Error("ERR AUTH <password> called without any password configured".into());
    };
    if ct_eq(expected.as_bytes(), &args[1]) {
        session.authed = true;
        Reply::Simple("OK".into())
    } else {
        session.authed = false;
        Reply::Error("WRONGPASS invalid password".into())
    }
}

/// Equality that always scans the longer of the two inputs, so the time taken does not reveal
/// where the first mismatch is. A length mismatch is still reported, which leaks only the length.
fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    let n = a.len().max(b.len());
    let mut diff = (a.len() ^ b.len()) as u32;
    for i in 0..n {
        let x = a.get(i).copied().unwrap_or(0);
        let y = b.get(i).copied().unwrap_or(0);
        diff |= u32::from(x ^ y);
    }
    diff == 0
}

/// `(min, max)` argument counts including the command name.
fn arity(name: &[u8]) -> Option<(usize, usize)> {
    const V: usize = usize::MAX;
    Some(match name {
        b"PING" => (1, 2),
        b"ECHO" | b"GET" | b"KEYS" | b"HGETALL" | b"HLEN" | b"LPOP" | b"RPOP" | b"LLEN"
        | b"SMEMBERS" | b"SCARD" | b"PERSIST" | b"TTL" | b"PTTL" | b"INCR" | b"DECR"
        | b"SELECT" => (2, 2),
        b"SET" => (3, V),
        b"SETEX" | b"LRANGE" => (4, 4),
        b"DEL" | b"EXISTS" | b"MGET" | b"HDEL" | b"LPUSH" | b"RPUSH" | b"SADD" | b"SREM" => (2, V),
        b"EXPIRE" | b"PEXPIRE" | b"INCRBY" | b"DECRBY" | b"APPEND" | b"HGET" | b"SISMEMBER" => {
            (3, 3)
        }
        b"MSET" => (3, V),
        b"DBSIZE" => (1, 1),
        b"FLUSHALL" | b"FLUSHDB" => (1, 2),
        b"HSET" => (4, V),
        b"COMMAND" => (1, V),
        b"SAVE" | b"BGSAVE" | b"LASTSAVE" => (1, 1),
        b"INFO" => (1, 2),
        b"QUIT" => (1, 1),
        _ => return None,
    })
}

fn execute(ctx: &Ctx, args: &[Vec<u8>], name: &[u8]) -> Out {
    let store = &ctx.store;
    match name {
        b"SAVE" => save(store),
        b"BGSAVE" => bgsave(ctx),
        b"LASTSAVE" => Ok(Reply::Int(last_save_secs(store))),
        b"INFO" => Ok(Reply::Bulk(info(ctx).into_bytes())),
        b"PING" => Ok(if args.len() == 2 {
            Reply::Bulk(args[1].clone())
        } else {
            Reply::Simple("PONG".into())
        }),
        b"ECHO" => Ok(Reply::Bulk(args[1].clone())),
        b"GET" => Ok(bulk_or_nil(store.get(&args[1])?)),
        b"SET" => set(store, args),
        b"SETEX" => setex(store, args),
        b"DEL" => Ok(Reply::Int(store.delete(&refs(&args[1..])) as i64)),
        b"EXISTS" => Ok(Reply::Int(
            args[1..]
                .iter()
                .filter(|k| store.exists(k.as_slice()))
                .count() as i64,
        )),
        b"EXPIRE" => expire(store, args, 1000, "expire"),
        b"PEXPIRE" => expire(store, args, 1, "pexpire"),
        b"PERSIST" => Ok(Reply::Int(store.persist(&args[1]) as i64)),
        b"TTL" => Ok(Reply::Int(ttl_secs(store.pttl(&args[1])))),
        b"PTTL" => Ok(Reply::Int(store.pttl(&args[1]))),
        b"INCR" => Ok(Reply::Int(store.incr_by(&args[1], 1)?)),
        b"DECR" => Ok(Reply::Int(store.incr_by(&args[1], -1)?)),
        b"INCRBY" => Ok(Reply::Int(store.incr_by(&args[1], parse_i64(&args[2])?)?)),
        b"DECRBY" => {
            let delta = parse_i64(&args[2])?
                .checked_neg()
                .ok_or(StoreError::Overflow)?;
            Ok(Reply::Int(store.incr_by(&args[1], delta)?))
        }
        b"APPEND" => Ok(Reply::Int(store.append(&args[1], &args[2])? as i64)),
        b"MGET" => Ok(Reply::Array(
            store
                .mget(&refs(&args[1..]))
                .into_iter()
                .map(bulk_or_nil)
                .collect(),
        )),
        b"MSET" => mset(store, args),
        b"KEYS" => keys(store, args),
        b"DBSIZE" => Ok(Reply::Int(store.dbsize() as i64)),
        b"FLUSHALL" | b"FLUSHDB" => flush(store, args),
        b"HSET" => hset(store, args),
        b"HGET" => Ok(bulk_or_nil(store.hget(&args[1], &args[2])?)),
        b"HDEL" => Ok(Reply::Int(store.hdel(&args[1], &refs(&args[2..]))? as i64)),
        b"HGETALL" => {
            let flat = store
                .hgetall(&args[1])?
                .into_iter()
                .flat_map(|(f, v)| [Reply::Bulk(f), Reply::Bulk(v)])
                .collect();
            Ok(Reply::Array(flat))
        }
        b"HLEN" => Ok(Reply::Int(store.hlen(&args[1])? as i64)),
        b"LPUSH" => Ok(Reply::Int(store.lpush(&args[1], args[2..].to_vec())? as i64)),
        b"RPUSH" => Ok(Reply::Int(store.rpush(&args[1], args[2..].to_vec())? as i64)),
        b"LPOP" => Ok(bulk_or_nil(store.lpop(&args[1])?)),
        b"RPOP" => Ok(bulk_or_nil(store.rpop(&args[1])?)),
        b"LRANGE" => {
            let start = parse_i64(&args[2])?;
            let stop = parse_i64(&args[3])?;
            Ok(bulk_array(store.lrange(&args[1], start, stop)?))
        }
        b"LLEN" => Ok(Reply::Int(store.llen(&args[1])? as i64)),
        b"SADD" => Ok(Reply::Int(store.sadd(&args[1], args[2..].to_vec())? as i64)),
        b"SREM" => Ok(Reply::Int(store.srem(&args[1], &refs(&args[2..]))? as i64)),
        b"SMEMBERS" => Ok(bulk_array(store.smembers(&args[1])?)),
        b"SISMEMBER" => Ok(Reply::Int(store.sismember(&args[1], &args[2])? as i64)),
        b"SCARD" => Ok(Reply::Int(store.scard(&args[1])? as i64)),
        b"SELECT" => {
            if parse_i64(&args[1])? == 0 {
                Ok(Reply::Simple("OK".into()))
            } else {
                Err(Reply::Error("ERR DB index is out of range".into()))
            }
        }
        // Clients such as redis-py send COMMAND on connect; an empty table is enough.
        b"COMMAND" => Ok(Reply::Array(Vec::new())),
        _ => Err(Reply::Error("ERR unknown command".into())),
    }
}

fn set(store: &Store, args: &[Vec<u8>]) -> Out {
    let (mut nx, mut xx, mut ttl_ms) = (false, false, None);
    let mut i = 3;
    while i < args.len() {
        let opt = args[i].to_ascii_uppercase();
        match opt.as_slice() {
            b"NX" => nx = true,
            b"XX" => xx = true,
            b"EX" | b"PX" => {
                if ttl_ms.is_some() || i + 1 >= args.len() {
                    return Err(syntax());
                }
                let n = parse_i64(&args[i + 1])?;
                if n <= 0 {
                    return Err(invalid_expire("set"));
                }
                let ms = if opt == b"EX" {
                    n.checked_mul(1000)
                } else {
                    Some(n)
                };
                ttl_ms = Some(ms.ok_or_else(|| invalid_expire("set"))? as u64);
                i += 1;
            }
            _ => return Err(syntax()),
        }
        i += 1;
    }
    if nx && xx {
        return Err(syntax());
    }
    if store.set(&args[1], args[2].clone(), ttl_ms, nx, xx)? {
        Ok(Reply::Simple("OK".into()))
    } else {
        Ok(Reply::Nil)
    }
}

fn setex(store: &Store, args: &[Vec<u8>]) -> Out {
    let secs = parse_i64(&args[2])?;
    if secs <= 0 {
        return Err(invalid_expire("setex"));
    }
    let ms = secs
        .checked_mul(1000)
        .ok_or_else(|| invalid_expire("setex"))? as u64;
    store.set(&args[1], args[3].clone(), Some(ms), false, false)?;
    Ok(Reply::Simple("OK".into()))
}

/// Shared by EXPIRE (`factor` 1000) and PEXPIRE (`factor` 1).
fn expire(store: &Store, args: &[Vec<u8>], factor: i64, cmd: &str) -> Out {
    let n = parse_i64(&args[2])?;
    if n <= 0 {
        // Redis semantics: a non-positive TTL deletes the key.
        return Ok(Reply::Int(store.delete(&[args[1].as_slice()]) as i64));
    }
    let ms = n.checked_mul(factor).ok_or_else(|| invalid_expire(cmd))?;
    Ok(Reply::Int(store.expire_ms(&args[1], ms as u64) as i64))
}

fn mset(store: &Store, args: &[Vec<u8>]) -> Out {
    if args.len().is_multiple_of(2) {
        return Err(wrong_args(&args[0]));
    }
    let pairs = args[1..]
        .chunks(2)
        .map(|c| (c[0].clone(), c[1].clone()))
        .collect();
    store.mset(pairs)?;
    Ok(Reply::Simple("OK".into()))
}

fn keys(store: &Store, args: &[Vec<u8>]) -> Out {
    let pattern =
        std::str::from_utf8(&args[1]).map_err(|_| Reply::Error("ERR invalid pattern".into()))?;
    Ok(bulk_array(
        store.keys(pattern).into_iter().collect::<Vec<_>>(),
    ))
}

fn flush(store: &Store, args: &[Vec<u8>]) -> Out {
    if let Some(opt) = args.get(1) {
        let opt = opt.to_ascii_uppercase();
        if opt != b"ASYNC" && opt != b"SYNC" {
            return Err(syntax());
        }
    }
    store.flush();
    Ok(Reply::Simple("OK".into()))
}

fn hset(store: &Store, args: &[Vec<u8>]) -> Out {
    if !args.len().is_multiple_of(2) {
        return Err(wrong_args(&args[0]));
    }
    let mut added = 0;
    for pair in args[2..].chunks(2) {
        if store.hset(&args[1], &pair[0], pair[1].clone())? {
            added += 1;
        }
    }
    Ok(Reply::Int(added))
}

fn persistence_disabled() -> Reply {
    Reply::Error("ERR persistence is not enabled for this server".into())
}

fn save(store: &Store) -> Out {
    if store.persist_stats().is_none() {
        return Err(persistence_disabled());
    }
    match store.save() {
        Ok(()) => Ok(Reply::Simple("OK".into())),
        Err(e) => Err(Reply::Error(format!("ERR save failed: {e}"))),
    }
}

/// Starts a snapshot on a background thread. Refuses if one is already running. The thread
/// clears the running flag when it finishes, whatever the outcome.
fn bgsave(ctx: &Ctx) -> Out {
    if ctx.store.persist_stats().is_none() {
        return Err(persistence_disabled());
    }
    if ctx
        .bgsave_running
        .compare_exchange(false, true, SeqCst, SeqCst)
        .is_err()
    {
        return Err(Reply::Error(
            "ERR Background save already in progress".into(),
        ));
    }
    let store = ctx.store.clone();
    let running = Arc::clone(&ctx.bgsave_running);
    let spawned = thread::Builder::new()
        .name("tinycache-bgsave".into())
        .spawn(move || {
            let _ = store.save();
            running.store(false, SeqCst);
        });
    match spawned {
        Ok(_) => Ok(Reply::Simple("Background saving started".into())),
        Err(e) => {
            ctx.bgsave_running.store(false, SeqCst);
            Err(Reply::Error(format!(
                "ERR cannot start background save: {e}"
            )))
        }
    }
}

/// Unix time of the last successful save, in seconds. Zero if none has happened.
fn last_save_secs(store: &Store) -> i64 {
    store
        .persist_stats()
        .map_or(0, |s| (s.last_save_ms / 1000) as i64)
}

/// The INFO text, one `key:value` per line under `# Section` headers.
fn info(ctx: &Ctx) -> String {
    let store = &ctx.store;
    let mut out = String::new();
    out.push_str("# Server\r\n");
    out.push_str(&format!(
        "uptime_in_seconds:{}\r\n",
        ctx.started.elapsed().as_secs()
    ));
    out.push_str("\r\n# Memory\r\n");
    out.push_str(&format!("used_memory:{}\r\n", store.used_memory()));
    out.push_str("\r\n# Keyspace\r\n");
    out.push_str(&format!("keys:{}\r\n", store.dbsize()));
    out.push_str("\r\n# Persistence\r\n");
    match store.persist_stats() {
        None => out.push_str("persistence_enabled:0\r\n"),
        Some(st) => {
            out.push_str("persistence_enabled:1\r\n");
            out.push_str(&format!("last_save_time:{}\r\n", st.last_save_ms / 1000));
            out.push_str(&format!("wal_bytes:{}\r\n", st.wal_bytes));
            out.push_str(&format!("snapshot_bytes:{}\r\n", st.snapshot_bytes));
            out.push_str(&format!(
                "last_error:{}\r\n",
                st.last_error.as_deref().unwrap_or("")
            ));
        }
    }
    out
}

fn refs(args: &[Vec<u8>]) -> Vec<&[u8]> {
    args.iter().map(Vec::as_slice).collect()
}

fn bulk_or_nil(v: Option<Vec<u8>>) -> Reply {
    v.map_or(Reply::Nil, Reply::Bulk)
}

fn bulk_array(items: Vec<Vec<u8>>) -> Reply {
    Reply::Array(items.into_iter().map(Reply::Bulk).collect())
}

/// Converts a PTTL result to Redis TTL seconds (rounded), passing -1/-2 through.
fn ttl_secs(pttl: i64) -> i64 {
    if pttl >= 0 {
        (pttl + 500) / 1000
    } else {
        pttl
    }
}

fn parse_i64(b: &[u8]) -> Result<i64, Reply> {
    std::str::from_utf8(b)
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
        .ok_or_else(|| Reply::Error("ERR value is not an integer or out of range".into()))
}

fn syntax() -> Reply {
    Reply::Error("ERR syntax error".into())
}

fn invalid_expire(cmd: &str) -> Reply {
    Reply::Error(format!("ERR invalid expire time in '{cmd}' command"))
}

fn wrong_args(name: &[u8]) -> Reply {
    let lower = String::from_utf8_lossy(&name.to_ascii_lowercase()).into_owned();
    Reply::Error(format!(
        "ERR wrong number of arguments for '{lower}' command"
    ))
}
