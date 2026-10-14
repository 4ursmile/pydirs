//! Memory probe: RSS growth (via `ps`) versus `Store::used_memory()`.
//!
//! Run with: `cargo run --release --no-default-features --example mem_probe`
//!
//! Each scenario runs in a fresh child process so that RSS growth is not polluted by
//! earlier scenarios (the allocator rarely returns memory to the OS mid-process).

use std::process::Command;

use _tinycache::core::{Config, Store};

const KEYS: usize = 1_000_000;
const HASHES: usize = 100_000;
const LISTS: usize = 100_000;

fn rss_kib() -> u64 {
    let out = Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()
        .expect("ps must be available");
    String::from_utf8_lossy(&out.stdout)
        .trim()
        .parse()
        .unwrap_or(0)
}

fn mib(bytes: u64) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
}

fn new_store() -> Store {
    Store::new(Config {
        sweep_interval_ms: 0,
        ..Config::default()
    })
}

/// Prints one line: label, RSS (MiB), used_memory (MiB).
fn report(label: &str, rss_base_kib: u64, store: &Store) {
    let rss = rss_kib() * 1024;
    let base = rss_base_kib * 1024;
    let growth = rss.saturating_sub(base);
    println!(
        "{label}\trss_growth_mib={:.1}\trss_mib={:.1}\tused_memory_mib={:.1}\tused_bytes={}",
        mib(growth),
        mib(rss),
        mib(store.used_memory() as u64),
        store.used_memory()
    );
}

fn child(scenario: &str) {
    let store = new_store();
    let base = rss_kib();
    match scenario {
        "strings" => {
            let value = vec![b'x'; 64];
            for i in 0..KEYS {
                let k = format!("k{i:07}");
                store
                    .set(k.as_bytes(), value.clone(), None, false, false)
                    .expect("unlimited");
            }
            report("a_strings_1m", base, &store);

            // (e) delete 90% of the keys. Keys are generated on the fly so the probe's own
            // bookkeeping does not add to the RSS being measured.
            for i in 0..KEYS * 9 / 10 {
                let k = format!("k{i:07}");
                store.delete(&[k.as_bytes()]);
            }
            report("e_after_delete_90pct", base, &store);

            // (d) flush everything.
            store.flush();
            report("d_after_flush", base, &store);
        }
        "hashes" => {
            for i in 0..HASHES {
                let k = format!("h{i:07}");
                for f in 0..5 {
                    store
                        .hset(k.as_bytes(), format!("f{f}").as_bytes(), vec![b'v'; 16])
                        .expect("unlimited");
                }
            }
            report("b_hashes_100k_x5", base, &store);
        }
        "lists" => {
            for i in 0..LISTS {
                let k = format!("l{i:07}");
                let items: Vec<Vec<u8>> = (0..5).map(|_| vec![b'v'; 16]).collect();
                store.rpush(k.as_bytes(), items).expect("unlimited");
            }
            report("c_lists_100k_x5", base, &store);
        }
        other => panic!("unknown scenario {other}"),
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if let Some(name) = args.get(1).filter(|a| a.as_str() != "--") {
        child(name);
        return;
    }
    let exe = std::env::current_exe().expect("current exe");
    for scenario in ["strings", "hashes", "lists"] {
        let status = Command::new(&exe)
            .arg(scenario)
            .status()
            .expect("spawn child");
        assert!(status.success(), "scenario {scenario} failed");
    }
}
