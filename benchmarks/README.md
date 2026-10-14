# Benchmarks

`compare.py` measures pydirs against a dict baseline and against Redis. It
reports three things, because an in-process cache and a network server are not
directly comparable.

1. **Python embedded.** `pydirs.Cache` (in-process) against a plain `dict`,
   and against `redis-py` talking to `redis-server` over localhost TCP.
2. **Wire protocol.** `redis-benchmark` against `pydirs.Server` and against
   `redis-server`, with the same RESP clients.
3. **Size and memory.** Wheel and binary sizes, and RSS growth for 1M keys of
   64-byte values.

## Running it

Requirements: `redis-server` and `redis-benchmark` on `PATH`, `redis-py`
installed, and a release build of the extension (`maturin develop --release`).

```bash
python benchmarks/compare.py                       # defaults
python benchmarks/compare.py --n 200000 --redis-port 6390 --tc-port 6391
```

Results depend on the machine, OS, Python version and Redis version. Record
those with any numbers you publish. The README's Performance section shows the
numbers from one run, labelled with its hardware and versions.

## Rust benchmark

`benches/store_bench.rs` is a Criterion benchmark for the core store. Run it with:

```bash
cargo bench --no-default-features
```

It measures the Rust core directly, without the Python or socket layers.

## Adding a result

Re-run `compare.py` on the hardware you want to report, and update the table in
the README with the machine details next to it. Do not quote numbers from another
machine.
