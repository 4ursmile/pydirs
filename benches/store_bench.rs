use std::hint::black_box;
use std::thread;

use criterion::{criterion_group, criterion_main, Criterion};

use _pydirs::core::{Config, Store};

const KEYS: usize = 10_000;

fn key(i: usize) -> Vec<u8> {
    format!("key:{i}").into_bytes()
}

fn populated() -> (Store, Vec<Vec<u8>>) {
    let store = Store::new(Config {
        sweep_interval_ms: 0,
        ..Config::default()
    });
    let keys: Vec<Vec<u8>> = (0..KEYS).map(key).collect();
    for k in &keys {
        store
            .set(k, vec![b'x'; 64], None, false, false)
            .expect("unlimited store");
    }
    (store, keys)
}

fn bench_get_hit(c: &mut Criterion) {
    let (store, keys) = populated();
    let mut i = 0usize;
    c.bench_function("get_hit", |b| {
        b.iter(|| {
            i = (i + 1) % KEYS;
            black_box(store.get(&keys[i]).expect("string key"));
        })
    });
}

fn bench_set(c: &mut Criterion) {
    let (store, keys) = populated();
    let value = vec![b'y'; 64];
    let mut i = 0usize;
    c.bench_function("set_overwrite", |b| {
        b.iter(|| {
            i = (i + 1) % KEYS;
            black_box(
                store
                    .set(&keys[i], value.clone(), None, false, false)
                    .expect("unlimited store"),
            );
        })
    });
}

fn bench_mixed(c: &mut Criterion) {
    let (store, keys) = populated();
    let value = vec![b'z'; 64];
    let mut i = 0usize;
    c.bench_function("mixed_80_get_20_set", |b| {
        b.iter(|| {
            i = (i + 7919) % KEYS;
            if i.is_multiple_of(5) {
                black_box(
                    store
                        .set(&keys[i], value.clone(), None, false, false)
                        .expect("unlimited store"),
                );
            } else {
                black_box(store.get(&keys[i]).expect("string key"));
            }
        })
    });
}

fn bench_multithread(c: &mut Criterion) {
    const THREADS: usize = 8;
    const OPS: usize = 2_000;
    let (store, keys) = populated();
    c.bench_function("mixed_8_threads", |b| {
        b.iter(|| {
            thread::scope(|s| {
                for t in 0..THREADS {
                    let store = store.clone();
                    let keys = &keys;
                    s.spawn(move || {
                        let value = vec![b'w'; 64];
                        for n in 0..OPS {
                            let i = (t * OPS + n * 31) % KEYS;
                            if n.is_multiple_of(5) {
                                store
                                    .set(&keys[i], value.clone(), None, false, false)
                                    .expect("unlimited store");
                            } else {
                                black_box(store.get(&keys[i]).expect("string key"));
                            }
                        }
                    });
                }
            });
        })
    });
}

criterion_group!(
    benches,
    bench_get_hit,
    bench_set,
    bench_mixed,
    bench_multithread
);
criterion_main!(benches);
