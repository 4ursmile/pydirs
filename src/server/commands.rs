//! Command table and dispatch from RESP argument lists onto [`Store`].

use crate::core::{Store, StoreError};

use super::resp::Reply;

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

/// Runs one command. Returns the reply and whether the connection should close.
pub(crate) fn dispatch(store: &Store, args: &[Vec<u8>]) -> (Reply, bool) {
    let Some(first) = args.first() else {
        return (Reply::Error("ERR empty command".into()), false);
    };
    let name = first.to_ascii_uppercase();
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
    if name == b"QUIT" {
        return (Reply::Simple("OK".into()), true);
    }
    let reply = match execute(store, args, &name) {
        Ok(reply) | Err(reply) => reply,
    };
    (reply, false)
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
        b"QUIT" => (1, 1),
        _ => return None,
    })
}

fn execute(store: &Store, args: &[Vec<u8>], name: &[u8]) -> Out {
    match name {
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
