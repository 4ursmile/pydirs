//! Leak tests (PLAN.md 4f). Each test runs a workload many times and asserts that live heap
//! bytes, measured by a counting global allocator, return to a flat level after warm-up.
//! Growth proportional to the iteration count fails. Tests are kept fast (< 10 s total).
//!
//! The counting allocator lives in this test binary only, so it does not affect the library.

use std::alloc::{GlobalAlloc, Layout, System};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use _tinycache::core::{Config, Policy, Store};

struct Counting;

static LIVE: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        LIVE.fetch_add(l.size(), Relaxed);
        System.alloc(l)
    }
    unsafe fn alloc_zeroed(&self, l: Layout) -> *mut u8 {
        LIVE.fetch_add(l.size(), Relaxed);
        System.alloc_zeroed(l)
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        LIVE.fetch_sub(l.size(), Relaxed);
        System.dealloc(p, l)
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, new: usize) -> *mut u8 {
        let q = System.realloc(p, l, new);
        if !q.is_null() {
            LIVE.fetch_add(new, Relaxed);
            LIVE.fetch_sub(l.size(), Relaxed);
        }
        q
    }
}

#[global_allocator]
static ALLOC: Counting = Counting;

/// The counter is process-global and the test harness runs tests on several threads, so the
/// leak tests take this lock to keep each other's allocations out of their measurements.
static SERIAL: Mutex<()> = Mutex::new(());

fn live() -> usize {
    LIVE.load(Relaxed)
}

/// Tolerance for allocator and bookkeeping noise between two measurements.
const TOLERANCE: usize = 1 << 20;

fn quiet_store(max_memory: usize, policy: Policy) -> Store {
    Store::new(Config {
        shards: 16,
        max_memory,
        policy,
        sweep_interval_ms: 0,
        persist: None,
    })
}

#[test]
fn set_delete_rounds_do_not_grow() {
    let _g = SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    let s = quiet_store(0, Policy::NoEviction);
    let round = |s: &Store| {
        for i in 0..40_000 {
            let k = format!("k{i}");
            s.set(k.as_bytes(), vec![b'x'; 64], None, false, false)
                .unwrap();
        }
        for i in 0..40_000 {
            let k = format!("k{i}");
            s.delete(&[k.as_bytes()]);
        }
    };
    round(&s); // warm-up
    let base = live();
    for _ in 0..10 {
        round(&s);
    }
    assert_eq!(s.dbsize(), 0);
    assert!(
        live() <= base + TOLERANCE,
        "live bytes grew {} -> {}",
        base,
        live()
    );
}

#[test]
fn ttl_keys_expire_through_sweeper_without_growth() {
    let _g = SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    let s = Store::new(Config {
        shards: 16,
        max_memory: 0,
        policy: Policy::NoEviction,
        sweep_interval_ms: 1,
        persist: None,
    });
    // Short real TTLs; the sweeper removes them with no reads to trigger lazy expiry.
    let round = |s: &Store| {
        for i in 0..20_000 {
            let k = format!("t{i}");
            s.set(k.as_bytes(), vec![b'x'; 64], Some(5), false, false)
                .unwrap();
        }
        let deadline = Instant::now() + Duration::from_secs(5);
        while s.dbsize() > 0 && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(2));
        }
        assert_eq!(s.dbsize(), 0, "sweeper must expire every TTL key");
    };
    round(&s);
    let base = live();
    for _ in 0..5 {
        round(&s);
    }
    s.close();
    assert!(
        live() <= base + TOLERANCE,
        "live bytes grew {} -> {}",
        base,
        live()
    );
}

#[test]
fn flush_after_fill_releases_memory() {
    let _g = SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    let s = quiet_store(0, Policy::NoEviction);
    let before = live();
    for round in 0..5 {
        for i in 0..100_000 {
            let k = format!("f{round}:{i}");
            s.set(k.as_bytes(), vec![b'x'; 64], None, false, false)
                .unwrap();
        }
        s.hset(b"h", b"f", vec![0; 16]).unwrap();
        s.flush();
        assert_eq!(s.used_memory(), 0);
    }
    assert!(
        live() <= before + TOLERANCE,
        "flush left {} bytes live (was {before})",
        live().saturating_sub(before)
    );
}

#[test]
fn collection_create_and_empty_auto_remove_does_not_grow() {
    let _g = SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    let s = quiet_store(0, Policy::NoEviction);
    let round = |s: &Store| {
        for i in 0..20_000 {
            let k = format!("c{i}");
            s.rpush(k.as_bytes(), vec![vec![b'v'; 32]; 3]).unwrap();
            s.lpop(k.as_bytes()).unwrap();
            s.lpop(k.as_bytes()).unwrap();
            s.lpop(k.as_bytes()).unwrap();
            s.sadd(k.as_bytes(), vec![b"m".to_vec()]).unwrap();
            s.srem(k.as_bytes(), &[b"m"]).unwrap();
            s.hset(k.as_bytes(), b"f", vec![b'v'; 8]).unwrap();
            s.hdel(k.as_bytes(), &[b"f"]).unwrap();
        }
    };
    round(&s);
    assert_eq!(s.dbsize(), 0, "emptied collections are removed");
    let base = live();
    for _ in 0..5 {
        round(&s);
    }
    assert_eq!(s.dbsize(), 0);
    assert!(
        live() <= base + TOLERANCE,
        "live bytes grew {} -> {}",
        base,
        live()
    );
}

#[test]
fn eviction_churn_stays_under_budget_without_growth() {
    let _g = SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    // 256 KiB budget with 8k x 128 B inserts per round: every round forces heavy eviction.
    let max = 256 << 10;
    let s = quiet_store(max, Policy::AllKeysLru);
    let round = |s: &Store, r: usize| {
        for i in 0..8_000 {
            let k = format!("e{r}:{i}");
            s.set(k.as_bytes(), vec![b'x'; 128], None, false, false)
                .unwrap();
        }
    };
    round(&s, 0);
    let base = live();
    for r in 1..6 {
        round(&s, r);
        assert!(
            s.used_memory() <= max,
            "budget exceeded: {}",
            s.used_memory()
        );
    }
    assert!(
        live() <= base + TOLERANCE,
        "live bytes grew {} -> {} under eviction",
        base,
        live()
    );
}

#[test]
fn store_create_close_drop_does_not_leak() {
    let _g = SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    let cycle = || {
        let s = Store::new(Config {
            shards: 8,
            max_memory: 0,
            policy: Policy::NoEviction,
            sweep_interval_ms: 1,
            persist: None,
        });
        s.set(b"k", vec![b'x'; 64], Some(1_000), false, false)
            .unwrap();
        s.close();
    };
    for _ in 0..20 {
        cycle();
    }
    let base = live();
    let threads_base = thread_count();
    for _ in 0..200 {
        cycle();
    }
    // Sweeper threads exit on their next tick after the last handle drops, so give them a
    // moment to finish before measuring.
    let deadline = Instant::now() + Duration::from_secs(5);
    while thread_count() > threads_base && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(5));
    }
    assert!(
        live() <= base + TOLERANCE,
        "live bytes grew {} -> {}",
        base,
        live()
    );
    if let (Some(before), Some(after)) = (threads_base, thread_count()) {
        assert!(
            after <= before,
            "sweeper threads leaked: {before} -> {after}"
        );
    }
}

#[test]
fn server_start_stop_does_not_leak() {
    let _g = SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    let cycle = |port: u16| {
        let store = quiet_store(0, Policy::NoEviction);
        let handle = _tinycache::server::start(store, "127.0.0.1", port).expect("bind loopback");
        handle.stop();
    };
    let port = free_port();
    for _ in 0..5 {
        cycle(port);
    }
    let base = live();
    let threads_base = thread_count();
    let fds_base = fd_count();
    for _ in 0..50 {
        cycle(port);
    }
    assert!(
        live() <= base + TOLERANCE,
        "live bytes grew {} -> {} over server start/stop",
        base,
        live()
    );
    settle_threads(threads_base);
    if let (Some(b), Some(a)) = (threads_base, thread_count()) {
        assert!(a <= b, "server threads leaked: {b} -> {a}");
    }
    if let (Some(b), Some(a)) = (fds_base, fd_count()) {
        assert!(a <= b, "server fds leaked: {b} -> {a}");
    }
}

#[test]
fn server_connect_disconnect_does_not_leak() {
    let _g = SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    let store = quiet_store(0, Policy::NoEviction);
    let port = free_port();
    let handle = _tinycache::server::start(store, "127.0.0.1", port).expect("bind loopback");
    let connect_once = || {
        let mut c = TcpStream::connect(("127.0.0.1", port)).expect("connect");
        use std::io::{Read, Write};
        c.write_all(b"PING\r\n").unwrap();
        let mut buf = [0u8; 16];
        let _ = c.read(&mut buf);
        drop(c);
    };
    for _ in 0..20 {
        connect_once();
    }
    settle_threads(None);
    let base = live();
    let threads_base = thread_count();
    let fds_base = fd_count();
    for _ in 0..200 {
        connect_once();
    }
    settle_threads(threads_base);
    assert!(
        live() <= base + TOLERANCE,
        "live bytes grew {} -> {} over 200 connections",
        base,
        live()
    );
    if let (Some(b), Some(a)) = (threads_base, thread_count()) {
        assert!(a <= b, "connection threads leaked: {b} -> {a}");
    }
    if let (Some(b), Some(a)) = (fds_base, fd_count()) {
        assert!(a <= b, "fds leaked: {b} -> {a}");
    }
    handle.stop();
}

/// Waits for the process thread count to fall back to `target` (or stop changing if `None`).
fn settle_threads(target: Option<usize>) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        match (target, thread_count()) {
            (Some(t), Some(now)) if now <= t => return,
            (None, _) => {
                std::thread::sleep(Duration::from_millis(50));
                return;
            }
            _ => std::thread::sleep(Duration::from_millis(5)),
        }
    }
}

/// Number of OS threads in this process, where it can be read cheaply (Linux only).
fn thread_count() -> Option<usize> {
    std::fs::read_dir("/proc/self/task").ok().map(|d| d.count())
}

/// Number of open file descriptors, where it can be read cheaply (Linux only).
fn fd_count() -> Option<usize> {
    std::fs::read_dir("/proc/self/fd").ok().map(|d| d.count())
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind ephemeral")
        .local_addr()
        .expect("local addr")
        .port()
}

#[test]
fn persistent_open_close_reopen_does_not_leak() {
    use _tinycache::core::{FsyncPolicy, PersistConfig};

    let _g = SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    let dir = std::env::temp_dir().join(format!("tc-leak-persist-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let cfg = || Config {
        shards: 16,
        max_memory: 0,
        policy: Policy::NoEviction,
        sweep_interval_ms: 1,
        persist: Some(PersistConfig {
            dir: dir.clone(),
            fsync: FsyncPolicy::Never,
            snapshot_interval_secs: 0,
        }),
    };
    // Alternate between close() (final snapshot, threads joined, lock released) and a plain
    // drop. Every other cycle pays the snapshot's four fsyncs, which keeps the test under
    // the leak suite's time budget while still exercising both teardown paths.
    let cycle = |round: usize| {
        let s = Store::open(cfg()).expect("open");
        for i in 0..20 {
            s.set(
                format!("r{round}k{i}").as_bytes(),
                vec![b'x'; 64],
                None,
                false,
                false,
            )
            .unwrap();
        }
        if round.is_multiple_of(2) {
            s.close();
        }
    };
    // Warm up: the first cycles allocate the directory, buffers and thread stacks.
    for round in 0..5 {
        cycle(round);
    }
    let base = live();
    let threads_base = thread_count();
    let fds_base = fd_count();
    for round in 5..55 {
        cycle(round);
    }
    settle_threads(threads_base);
    // Each cycle's dataset is dropped with its store, so live heap must stay flat. The
    // directory itself grows by design; only memory, threads and fds are checked.
    assert!(
        live() <= base + TOLERANCE * 4,
        "live bytes grew {base} -> {} over 50 persistent cycles",
        live()
    );
    if let (Some(b), Some(a)) = (threads_base, thread_count()) {
        assert!(a <= b, "persistence threads leaked: {b} -> {a}");
    }
    if let (Some(b), Some(a)) = (fds_base, fd_count()) {
        assert!(a <= b, "persistence fds leaked: {b} -> {a}");
    }
    let _ = std::fs::remove_dir_all(&dir);
}
