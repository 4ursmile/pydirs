//! Persistence integration tests (PLAN.md 4d). Each test uses its own directory under
//! `std::env::temp_dir`, so tests can run in parallel. Time is real but every TTL is
//! measured against the wall clock with generous margins.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering::SeqCst};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use _tinycache::core::{Config, FsyncPolicy, PersistConfig, SaveStep, Store};

// ---- helpers --------------------------------------------------------------

static COUNTER: AtomicU64 = AtomicU64::new(0);

/// A fresh, empty directory unique to this test run.
fn tempdir(name: &str) -> PathBuf {
    let n = COUNTER.fetch_add(1, SeqCst);
    let d = std::env::temp_dir().join(format!("tc-persist-{name}-{}-{n}", std::process::id()));
    let _ = fs::remove_dir_all(&d);
    fs::create_dir_all(&d).unwrap();
    d
}

fn cfg_in(dir: &Path, fsync: FsyncPolicy) -> Config {
    Config {
        sweep_interval_ms: 0,
        persist: Some(PersistConfig {
            dir: dir.to_path_buf(),
            fsync,
            snapshot_interval_secs: 0,
        }),
        ..Config::default()
    }
}

fn open(dir: &Path) -> Store {
    Store::open(cfg_in(dir, FsyncPolicy::Never)).expect("open")
}

/// The error from opening `dir`, for tests that expect a refusal.
fn open_err(dir: &Path) -> io::Error {
    match Store::open(cfg_in(dir, FsyncPolicy::Never)) {
        Ok(_) => panic!("open unexpectedly succeeded"),
        Err(e) => e,
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

/// Every live key and its complete logical content, for comparing two stores.
#[derive(Debug, PartialEq, Eq)]
enum Val {
    Str(Vec<u8>),
    List(Vec<Vec<u8>>),
    Hash(Vec<(Vec<u8>, Vec<u8>)>),
    Set(Vec<Vec<u8>>),
}

/// Snapshot of a store's observable state: each live key with its value and whether it has
/// a TTL. The exact deadline is not compared, because reopening measures time again.
fn dump(s: &Store) -> Vec<(Vec<u8>, Val, bool)> {
    let mut keys = s.keys("*");
    keys.sort();
    keys.into_iter()
        .map(|k| {
            let ttl = s.pttl(&k) != -1;
            let val = if let Ok(Some(v)) = s.get(&k) {
                Val::Str(v)
            } else if let Ok(l) = s.lrange(&k, 0, -1) {
                Val::List(l)
            } else if let Ok(mut h) = s.hgetall(&k) {
                h.sort();
                Val::Hash(h)
            } else {
                let mut m = s.smembers(&k).expect("a set");
                m.sort();
                Val::Set(m)
            };
            (k, val, ttl)
        })
        .collect()
}

/// Every key/value pair in a deterministic order. Used as the oracle in fault tests.
fn assert_same(a: &Store, b: &Store, what: &str) {
    assert_eq!(dump(a), dump(b), "{what}");
}

fn b(s: &str) -> Vec<u8> {
    s.as_bytes().to_vec()
}

/// Writes one of each type. Used by the round-trip and crash tests.
fn populate(s: &Store) {
    s.set(b"str", b("hello"), None, false, false).unwrap();
    s.set(b"str_ttl", b("ttl"), Some(3_600_000), false, false)
        .unwrap();
    s.set(b"n", b("41"), None, false, false).unwrap();
    s.incr_by(b"n", 1).unwrap();
    s.append(b"app", b"ab").unwrap();
    s.append(b"app", b"cd").unwrap();
    s.rpush(b"list", vec![b("a"), b("b"), b("c")]).unwrap();
    s.lpush(b"list", vec![b("z")]).unwrap();
    s.hset(b"hash", b"f1", b("v1")).unwrap();
    s.hset(b"hash", b"f2", b("v2")).unwrap();
    s.hset(b"hash_ttl", b"f", b("v")).unwrap();
    s.expire_ms(b"hash_ttl", 3_600_000);
    s.sadd(b"set", vec![b("m1"), b("m2")]).unwrap();
    s.sadd(b"set", vec![b("m3")]).unwrap();
}

/// An in-memory store with no persistence, used as the reference for a persistent one.
fn oracle() -> Store {
    Store::new(Config {
        sweep_interval_ms: 0,
        ..Config::default()
    })
}

// ---- round trip -----------------------------------------------------------

#[test]
fn round_trip_every_type_and_ttl_across_reopen() {
    let dir = tempdir("roundtrip");
    let before;
    {
        let s = open(&dir);
        populate(&s);
        before = dump(&s);
        s.close();
    }
    let s = open(&dir);
    assert_eq!(dump(&s), before);
    assert!(s.pttl(b"str_ttl") > 0, "TTL restored");
    assert!(s.pttl(b"hash_ttl") > 0, "collection TTL restored");
    assert_eq!(s.pttl(b"str"), -1);
    assert_eq!(s.get(b"n").unwrap(), Some(b("42")));
    assert_eq!(s.get(b"app").unwrap(), Some(b("abcd")));
    assert_eq!(
        s.lrange(b"list", 0, -1).unwrap(),
        vec![b("z"), b("a"), b("b"), b("c")]
    );
    assert_eq!(s.hget(b"hash", b"f2").unwrap(), Some(b("v2")));
    assert_eq!(s.scard(b"set").unwrap(), 3);
}

#[test]
fn round_trip_through_wal_only_without_snapshot() {
    // Writes stay in the WAL (no save, no close), and the store is simply dropped.
    let dir = tempdir("walonly");
    let oracle = oracle();
    populate(&oracle);
    {
        let s = open(&dir);
        populate(&s);
        s.abandon();
    }
    let s = open(&dir);
    assert_same(&s, &oracle, "replayed WAL");
}

#[test]
fn deletes_and_collection_removal_replay() {
    let dir = tempdir("deletes");
    let oracle = oracle();
    let apply = |s: &Store| {
        populate(s);
        s.delete(&[b"str"]);
        s.hdel(b"hash", &[b"f1"]).unwrap();
        s.lpop(b"list").unwrap();
        s.srem(b"set", &[b"m1", b"m2", b"m3"]).unwrap();
        s.rpop(b"list").unwrap();
        s.persist(b"str_ttl");
        s.expire_ms(b"n", 3_600_000);
        s.flush();
        s.set(b"after", b("flush"), None, false, false).unwrap();
    };
    apply(&oracle);
    {
        let s = open(&dir);
        apply(&s);
        s.abandon();
    }
    let s = open(&dir);
    assert_same(&s, &oracle, "deletes and flush replay");
}

// ---- TTL ------------------------------------------------------------------

#[test]
fn expired_before_reopen_is_not_restored() {
    let dir = tempdir("expired");
    {
        let s = open(&dir);
        s.set(b"short", b("v"), Some(150), false, false).unwrap();
        s.set(b"long", b("v"), Some(3_600_000), false, false)
            .unwrap();
        s.hset(b"hshort", b"f", b("v")).unwrap();
        s.expire_ms(b"hshort", 150);
        s.save().unwrap();
        s.abandon();
    }
    thread::sleep(Duration::from_millis(250));
    let s = open(&dir);
    assert_eq!(s.get(b"short").unwrap(), None);
    assert!(!s.exists(b"hshort"));
    assert_eq!(s.get(b"long").unwrap(), Some(b("v")));
    assert_eq!(s.dbsize(), 1, "expired keys are not counted after reload");
}

#[test]
fn ttl_is_absolute_so_downtime_counts() {
    let dir = tempdir("absolute");
    {
        let s = open(&dir);
        s.set(b"k", b("v"), Some(2_000), false, false).unwrap();
        s.save().unwrap();
        s.abandon();
    }
    // Downtime of 1.2 s spends most of the 2 s TTL. A relative TTL would give back 2 s.
    thread::sleep(Duration::from_millis(1_200));
    let s = open(&dir);
    let left = s.pttl(b"k");
    assert!(
        left > 0 && left < 1_200,
        "remaining ttl {left} ms should be under 1.2 s"
    );
    thread::sleep(Duration::from_millis(1_000));
    assert_eq!(s.get(b"k").unwrap(), None);
}

#[test]
fn absolute_deadline_matches_wall_clock_at_set_time() {
    let dir = tempdir("deadline");
    let before = now_ms();
    {
        let s = open(&dir);
        s.set(b"k", b("v"), Some(60_000), false, false).unwrap();
        s.abandon();
    }
    let after = now_ms();
    let s = open(&dir);
    let left = s.pttl(b"k") as u64;
    // The deadline was fixed at set time, so the remaining TTL is at most 60 s and at least
    // 60 s minus the elapsed time between the set and the reopen.
    assert!(left <= 60_000, "{left}");
    assert!(left + (after - before) + 1_000 >= 60_000, "{left}");
}

// ---- torn WAL and corruption ----------------------------------------------

fn wal_file(dir: &Path) -> PathBuf {
    let mut names: Vec<PathBuf> = fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.file_name().unwrap().to_str().unwrap().starts_with("wal-"))
        .collect();
    names.sort();
    names.pop().expect("a WAL file")
}

#[test]
fn torn_wal_tail_is_truncated_and_earlier_records_kept() {
    let dir = tempdir("torn");
    {
        let s = open(&dir);
        for i in 0..10 {
            s.set(format!("k{i}").as_bytes(), b("v"), None, false, false)
                .unwrap();
        }
        s.abandon();
    }
    // Chop the last record in half, as a crash mid-write would.
    let wal = wal_file(&dir);
    let len = fs::metadata(&wal).unwrap().len();
    let f = fs::OpenOptions::new().write(true).open(&wal).unwrap();
    f.set_len(len - 3).unwrap();
    drop(f);

    let s = open(&dir);
    assert_eq!(
        s.dbsize(),
        9,
        "the torn record is dropped, the nine before it kept"
    );
    for i in 0..9 {
        assert_eq!(s.get(format!("k{i}").as_bytes()).unwrap(), Some(b("v")));
    }
    let stats = s.persist_stats().unwrap();
    assert!(
        stats
            .last_error
            .as_deref()
            .is_some_and(|e| e.contains("torn")),
        "{stats:?}"
    );
    // The truncated file keeps working for new writes.
    s.set(b"again", b("ok"), None, false, false).unwrap();
    s.abandon();
    let s2 = open(&dir);
    assert_eq!(s2.get(b"again").unwrap(), Some(b("ok")));
    assert_eq!(s2.dbsize(), 10);
}

#[test]
fn corrupt_wal_crc_stops_replay_at_that_record() {
    let dir = tempdir("walcrc");
    {
        let s = open(&dir);
        s.set(b"a", b("1"), None, false, false).unwrap();
        s.set(b"b", b("2"), None, false, false).unwrap();
        s.set(b"c", b("3"), None, false, false).unwrap();
        s.abandon();
    }
    // Flip a payload byte in the second frame: its CRC no longer matches.
    let wal = wal_file(&dir);
    let mut bytes = fs::read(&wal).unwrap();
    let first_len = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize + 8;
    bytes[first_len + 10] ^= 0xFF;
    fs::write(&wal, &bytes).unwrap();

    let s = open(&dir);
    assert_eq!(s.get(b"a").unwrap(), Some(b("1")));
    assert_eq!(s.get(b"b").unwrap(), None);
    assert_eq!(
        s.get(b"c").unwrap(),
        None,
        "records after a bad frame are discarded"
    );
}

#[test]
fn corrupt_snapshot_is_an_error_not_a_panic() {
    let dir = tempdir("snapcrc");
    {
        let s = open(&dir);
        populate(&s);
        s.close();
    }
    let snap = dir.join("snapshot.tcs");
    let mut bytes = fs::read(&snap).unwrap();
    let mid = bytes.len() / 2;
    bytes[mid] ^= 0xA5;
    fs::write(&snap, &bytes).unwrap();

    // Documented behaviour: open fails with InvalidData. It never panics and never serves
    // partial data from a damaged snapshot.
    let err = open_err(&dir);
    assert_eq!(err.kind(), io::ErrorKind::InvalidData, "{err}");
    // The lock is released on the failed open, so the directory is not left wedged.
    fs::remove_file(&snap).unwrap();
    let s = open(&dir);
    assert_eq!(s.dbsize(), 0);
}

#[test]
fn truncated_snapshot_is_an_error() {
    let dir = tempdir("snaptrunc");
    {
        let s = open(&dir);
        populate(&s);
        s.close();
    }
    let snap = dir.join("snapshot.tcs");
    let bytes = fs::read(&snap).unwrap();
    fs::write(&snap, &bytes[..bytes.len() - 9]).unwrap();
    let err = open_err(&dir);
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
}

// ---- crash points inside save() -------------------------------------------

/// Runs `save` stopped at `step` on a populated store, abandons it, reopens and compares
/// against the oracle. Each step leaves a different set of files on disk.
fn crash_at(step: SaveStep, name: &str) {
    let dir = tempdir(name);
    let oracle = oracle();
    {
        let s = open(&dir);
        let apply = |s: &Store| {
            populate(s);
            s.set(b"late", b("write"), Some(3_600_000), false, false)
                .unwrap();
            s.hset(b"hash", b"f3", b("v3")).unwrap();
        };
        apply(&oracle);
        apply(&s);
        s.save_stopping_after(step).unwrap();
        // More writes arrive after the interrupted save. They belong to the new generation.
        s.set(b"after_crash_point", b("x"), None, false, false)
            .unwrap();
        s.abandon();
    }
    oracle
        .set(b"after_crash_point", b("x"), None, false, false)
        .unwrap();
    let s = open(&dir);
    assert_same(&s, &oracle, &format!("reopen after crash at {step:?}"));
}

#[test]
fn crash_after_wal_rotation_replays_both_logs() {
    crash_at(SaveStep::AfterRotate, "crash-rotate");
}

#[test]
fn crash_after_tmp_written_ignores_tmp() {
    crash_at(SaveStep::AfterTmpWrite, "crash-tmp");
}

#[test]
fn crash_after_rename_before_wal_delete_is_consistent() {
    crash_at(SaveStep::AfterRename, "crash-rename");
}

#[test]
fn crash_before_directory_sync_after_wal_delete_is_consistent() {
    crash_at(SaveStep::AfterWalDelete, "crash-wal-delete");
}

#[test]
fn leftover_snapshot_tmp_is_removed_on_open() {
    let dir = tempdir("leftover-tmp");
    {
        let s = open(&dir);
        s.set(b"k", b("v"), None, false, false).unwrap();
        s.close();
    }
    fs::write(dir.join("snapshot.tmp"), b"half written garbage").unwrap();
    let s = open(&dir);
    assert_eq!(s.get(b"k").unwrap(), Some(b("v")));
    assert!(!dir.join("snapshot.tmp").exists());
}

// ---- fsync policies -------------------------------------------------------

#[test]
fn every_fsync_policy_round_trips() {
    for policy in [
        FsyncPolicy::Always,
        FsyncPolicy::EverySec,
        FsyncPolicy::Never,
    ] {
        let dir = tempdir(&format!("fsync-{policy:?}"));
        let oracle = oracle();
        populate(&oracle);
        {
            let s = Store::open(cfg_in(&dir, policy)).unwrap();
            populate(&s);
            s.abandon();
        }
        let s = open(&dir);
        assert_same(&s, &oracle, &format!("{policy:?}"));
    }
}

#[test]
fn everysec_background_thread_fsyncs_without_blocking_writes() {
    let dir = tempdir("everysec");
    let s = Store::open(cfg_in(&dir, FsyncPolicy::EverySec)).unwrap();
    for i in 0..100 {
        s.set(format!("k{i}").as_bytes(), b("v"), None, false, false)
            .unwrap();
    }
    assert_eq!(s.dbsize(), 100);
    assert!(s.persist_stats().unwrap().wal_bytes > 0);
    s.close();
}

// ---- snapshot and generations ---------------------------------------------

#[test]
fn save_deletes_superseded_wal_and_keeps_data() {
    let dir = tempdir("gc");
    let s = open(&dir);
    s.set(b"a", b("1"), None, false, false).unwrap();
    s.save().unwrap();
    s.save().unwrap();
    let wals: Vec<_> = fs::read_dir(&dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .filter(|n| n.starts_with("wal-"))
        .collect();
    assert_eq!(
        wals.len(),
        1,
        "only the active generation remains: {wals:?}"
    );
    let stats = s.persist_stats().unwrap();
    assert!(stats.snapshot_bytes > 0);
    assert!(stats.last_save_ms > 0);
    s.close();
    let s = open(&dir);
    assert_eq!(s.get(b"a").unwrap(), Some(b("1")));
}

#[test]
fn persist_stats_reports_loaded_keys_and_wal_bytes() {
    let dir = tempdir("stats");
    {
        let s = open(&dir);
        s.set(b"a", b("1"), None, false, false).unwrap();
        s.set(b"b", b("2"), None, false, false).unwrap();
        s.close();
    }
    let s = open(&dir);
    let st = s.persist_stats().unwrap();
    assert_eq!(st.loaded_keys, 2);
    // Recovery leaves the log empty until the next write, so the reported size is zero.
    assert_eq!(st.wal_bytes, 0);
    assert!(Store::new(Config::default()).persist_stats().is_none());
}

#[test]
fn save_without_persistence_is_an_error() {
    let s = Store::new(Config::default());
    assert!(s.save().is_err());
}

#[test]
fn close_writes_final_snapshot_and_is_idempotent() {
    let dir = tempdir("close");
    let s = open(&dir);
    s.set(b"a", b("1"), None, false, false).unwrap();
    s.close();
    assert!(dir.join("snapshot.tcs").exists());
    s.close();
    let s2 = open(&dir);
    assert_eq!(s2.get(b"a").unwrap(), Some(b("1")));
}

// ---- concurrency ----------------------------------------------------------

#[test]
fn writers_during_repeated_saves_reopen_to_oracle() {
    let dir = tempdir("concurrent");
    let s = Arc::new(open(&dir));
    const THREADS: usize = 8;
    const OPS: usize = 400;
    let mut handles = Vec::new();
    for t in 0..THREADS {
        let s = Arc::clone(&s);
        handles.push(thread::spawn(move || {
            for i in 0..OPS {
                let k = format!("t{t}:k{i}");
                s.set(
                    k.as_bytes(),
                    format!("v{i}").into_bytes(),
                    None,
                    false,
                    false,
                )
                .unwrap();
                if i % 7 == 0 {
                    s.incr_by(format!("t{t}:n").as_bytes(), 1).unwrap();
                }
                if i % 11 == 0 {
                    s.rpush(
                        format!("t{t}:l").as_bytes(),
                        vec![format!("{i}").into_bytes()],
                    )
                    .unwrap();
                }
                if i % 13 == 0 {
                    s.delete(&[format!("t{t}:k{}", i / 2).as_bytes()]);
                }
            }
        }));
    }
    for _ in 0..5 {
        s.save().unwrap();
        thread::sleep(Duration::from_millis(2));
    }
    for h in handles {
        h.join().unwrap();
    }
    s.save().unwrap();
    // Build the oracle from the final state read back in memory: this checks reopen, not the
    // interleaving, which is verified by the store's own invariants.
    let expected = dump(&s);
    s.abandon();
    let reopened = open(&dir);
    assert_eq!(dump(&reopened), expected);
    assert!(!expected.is_empty());
}

// ---- lock file ------------------------------------------------------------

#[test]
fn second_open_of_live_directory_is_would_block() {
    let dir = tempdir("lock");
    let first = open(&dir);
    let err = open_err(&dir);
    assert_eq!(err.kind(), io::ErrorKind::WouldBlock, "{err}");
    drop(first);
    let again = open(&dir);
    drop(again);
}

#[test]
fn stale_lock_from_dead_pid_is_taken_over() {
    let dir = tempdir("stale-lock");
    {
        let s = open(&dir);
        s.set(b"k", b("v"), None, false, false).unwrap();
        s.abandon();
    }
    // A pid that cannot belong to a live process.
    fs::write(dir.join("LOCK"), "2147483646\n").unwrap();
    let s = open(&dir);
    assert_eq!(s.get(b"k").unwrap(), Some(b("v")));
}

#[test]
fn abandoned_store_leaves_lock_but_reopens_in_process() {
    let dir = tempdir("abandon-lock");
    let s = open(&dir);
    s.abandon();
    assert!(
        dir.join("LOCK").exists(),
        "SIGKILL leaves the lock file behind"
    );
    let again = open(&dir);
    drop(again);
}

#[test]
fn persistence_config_round_trips_through_config_clone() {
    let dir = tempdir("cfg-clone");
    let cfg = cfg_in(&dir, FsyncPolicy::Always);
    assert_eq!(cfg.clone(), cfg);
}
