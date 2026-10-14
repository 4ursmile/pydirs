//! Byte encodings shared by the WAL and the snapshot, plus CRC-32 (IEEE 802.3, reflected).
//!
//! A WAL payload is one logical operation. Keys and values are length-prefixed with a
//! LEB128 varint. Operations are logged as they are applied, so replay runs the same
//! mutation code that produced them.

// WAL operation tags.
pub(crate) const OP_SET: u8 = 1;
pub(crate) const OP_DEL: u8 = 2;
pub(crate) const OP_EXPIRE: u8 = 3;
pub(crate) const OP_HSET: u8 = 4;
pub(crate) const OP_HDEL: u8 = 5;
pub(crate) const OP_LPUSH: u8 = 6;
pub(crate) const OP_RPUSH: u8 = 7;
pub(crate) const OP_LPOP: u8 = 8;
pub(crate) const OP_RPOP: u8 = 9;
pub(crate) const OP_SADD: u8 = 10;
pub(crate) const OP_SREM: u8 = 11;
pub(crate) const OP_FLUSH: u8 = 12;

pub(crate) fn put_varint(b: &mut Vec<u8>, mut n: u64) {
    while n >= 0x80 {
        b.push(((n as u8) & 0x7f) | 0x80);
        n >>= 7;
    }
    b.push(n as u8);
}

pub(crate) fn put_u64(b: &mut Vec<u8>, n: u64) {
    b.extend_from_slice(&n.to_le_bytes());
}

pub(crate) fn put_bytes(b: &mut Vec<u8>, s: &[u8]) {
    put_varint(b, s.len() as u64);
    b.extend_from_slice(s);
}

pub(crate) fn enc_set(b: &mut Vec<u8>, k: &[u8], expires_at: u64, v: &[u8]) {
    b.push(OP_SET);
    put_u64(b, expires_at);
    put_bytes(b, k);
    b.extend_from_slice(v);
}

/// An operation on one key with no further arguments (DEL, LPOP, RPOP).
pub(crate) fn enc_key(b: &mut Vec<u8>, op: u8, k: &[u8]) {
    b.push(op);
    put_bytes(b, k);
}

pub(crate) fn enc_expire(b: &mut Vec<u8>, k: &[u8], expires_at: u64) {
    b.push(OP_EXPIRE);
    put_u64(b, expires_at);
    put_bytes(b, k);
}

pub(crate) fn enc_hset(b: &mut Vec<u8>, k: &[u8], f: &[u8], v: &[u8]) {
    b.push(OP_HSET);
    put_bytes(b, k);
    put_bytes(b, f);
    b.extend_from_slice(v);
}

/// An operation on one key with a list of members (HDEL, LPUSH, RPUSH, SADD, SREM).
pub(crate) fn enc_items<T: AsRef<[u8]>>(b: &mut Vec<u8>, op: u8, k: &[u8], items: &[T]) {
    b.push(op);
    put_bytes(b, k);
    put_varint(b, items.len() as u64);
    for item in items {
        put_bytes(b, item.as_ref());
    }
}

/// Removes every key. Logged once, under all shard locks, so it is ordered against writes.
pub(crate) fn enc_flush(b: &mut Vec<u8>) {
    b.push(OP_FLUSH);
}

/// Cursor over a decoded WAL payload. Every read returns `None` when the bytes run out.
pub(crate) struct Rd<'a>(&'a [u8]);

impl<'a> Rd<'a> {
    pub(crate) fn new(buf: &'a [u8]) -> Self {
        Rd(buf)
    }

    pub(crate) fn u8(&mut self) -> Option<u8> {
        let (&b, rest) = self.0.split_first()?;
        self.0 = rest;
        Some(b)
    }

    pub(crate) fn u64(&mut self) -> Option<u64> {
        let head: [u8; 8] = self.0.get(..8)?.try_into().ok()?;
        self.0 = &self.0[8..];
        Some(u64::from_le_bytes(head))
    }

    pub(crate) fn varint(&mut self) -> Option<u64> {
        let mut n = 0u64;
        let mut shift = 0u32;
        loop {
            let b = self.u8()?;
            if shift > 63 {
                return None;
            }
            n |= u64::from(b & 0x7f) << shift;
            if b & 0x80 == 0 {
                return Some(n);
            }
            shift += 7;
        }
    }

    pub(crate) fn bytes(&mut self) -> Option<&'a [u8]> {
        let n = usize::try_from(self.varint()?).ok()?;
        if self.0.len() < n {
            return None;
        }
        let (head, rest) = self.0.split_at(n);
        self.0 = rest;
        Some(head)
    }

    /// Everything not yet consumed; used for a trailing value.
    pub(crate) fn rest(&mut self) -> &'a [u8] {
        std::mem::take(&mut self.0)
    }

    pub(crate) fn list(&mut self) -> Option<Vec<&'a [u8]>> {
        let n = self.varint()?;
        let mut out = Vec::new();
        for _ in 0..n {
            out.push(self.bytes()?);
        }
        Some(out)
    }
}

const fn crc_table() -> [u32; 256] {
    let mut table = [0u32; 256];
    let mut i = 0;
    while i < 256 {
        let mut c = i as u32;
        let mut k = 0;
        while k < 8 {
            c = if c & 1 != 0 {
                0xEDB8_8320 ^ (c >> 1)
            } else {
                c >> 1
            };
            k += 1;
        }
        table[i] = c;
        i += 1;
    }
    table
}

const CRC_TABLE: [u32; 256] = crc_table();

/// Incremental CRC-32 (the zlib/PNG polynomial).
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct Crc32(u32);

impl Crc32 {
    pub(crate) fn update(&mut self, data: &[u8]) {
        let mut c = !self.0;
        for &b in data {
            c = CRC_TABLE[((c ^ u32::from(b)) & 0xFF) as usize] ^ (c >> 8);
        }
        self.0 = !c;
    }

    pub(crate) fn value(self) -> u32 {
        self.0
    }
}

pub(crate) fn crc32(data: &[u8]) -> u32 {
    let mut c = Crc32::default();
    c.update(data);
    c.value()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc32_matches_standard_check_value() {
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
        assert_eq!(crc32(b""), 0);
        let mut inc = Crc32::default();
        inc.update(b"1234");
        inc.update(b"56789");
        assert_eq!(inc.value(), 0xCBF4_3926);
    }

    #[test]
    fn varint_round_trip_including_edges() {
        for n in [0u64, 1, 127, 128, 300, u32::MAX as u64, u64::MAX] {
            let mut b = Vec::new();
            put_varint(&mut b, n);
            let mut r = Rd::new(&b);
            assert_eq!(r.varint(), Some(n));
            assert!(r.rest().is_empty());
        }
    }

    #[test]
    fn reader_rejects_truncated_input() {
        let mut b = Vec::new();
        put_bytes(&mut b, b"hello");
        let mut r = Rd::new(&b[..3]);
        assert_eq!(r.bytes(), None);
        assert_eq!(Rd::new(&[0x80]).varint(), None);
    }
}
