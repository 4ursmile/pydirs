# Persistence

`tinycache` can keep its data on disk, so a cache survives a restart. Persistence
is opt-in: nothing is written unless you pass a `persist_dir`, or start the server
with `--dir`.

This page describes the design and states exactly what is and is not guaranteed.
Read the [limits](#what-is-guaranteed-and-what-is-not) section before relying on it.

## Enabling it

```python
from tinycache import Cache

with Cache(persist_dir="/var/lib/myapp/cache", fsync="everysec") as c:
    c.set("user:1", "Ada")
# leaving the block writes a final snapshot and releases the directory
```

```bash
tinycache-server --dir /var/lib/tinycache --fsync everysec --snapshot-interval 300
```

| Option | Default | Meaning |
|---|---|---|
| `persist_dir` / `--dir` | none (off) | Directory that holds the files below. Created if missing. |
| `fsync` / `--fsync` | `"everysec"` | When WAL writes are flushed to disk. See [fsync policy](#fsync-policy). |
| `snapshot_interval` / `--snapshot-interval` | `300` | Seconds between background snapshots, taken only when there are unsaved writes. `0` disables the timer. |

## File layout

Everything lives in one directory, `persist_dir`.

| File | Purpose |
|---|---|
| `snapshot.tcs` | The full dataset as of one generation. Written by `save()`, the background snapshotter, `SAVE`/`BGSAVE`, and `close()`. |
| `snapshot.tmp` | A snapshot being written. Renamed to `snapshot.tcs` when complete. Deleted on open if it is left behind, because an unfinished save never replaced anything. |
| `wal-<n>.log` | Write-ahead log for generation `n`: every mutation made after the rotation into `n`. |
| `LOCK` | Holds the owning process id. Only one live process may open the directory. |

The snapshot starts with the magic `TCSNAP01`, a format version, and the
generation it belongs to. It ends with a CRC-32 over the whole file. Each WAL
record is framed as `[length u32][crc32 u32][payload]`.

## Snapshot and WAL design

Every mutation is appended to the active WAL, which is the file for the current
generation. A snapshot writes the complete state and starts a new generation:

1. **Rotate.** Every shard is read-locked, and the WAL moves to generation `g+1`.
   Because writers take a shard lock before they log, no write can fall between
   the snapshot and the new log.
2. **Write.** The shards are streamed into `snapshot.tmp`, which is then fsynced.
3. **Replace.** `snapshot.tmp` is renamed over `snapshot.tcs`, and the directory
   is fsynced (Unix only).
4. **Clean up.** WAL files older than `g+1` are deleted.

A crash at any step leaves a state that recovery reads back exactly. Before step
3 the old snapshot and the old logs are intact. After step 3 the new snapshot
already holds everything the old logs did.

### Recovery

On `open`:

1. Delete `snapshot.tmp` if it exists.
2. Load `snapshot.tcs` (if present). Its checksum is verified before any record is
   applied, so a damaged file never reaches the store.
3. Replay every `wal-n.log` with `n >= g` (the snapshot's generation), in order.
   Older logs are superseded and are deleted.
4. A torn or checksum-failing record at the end of a WAL file is the expected
   result of a crash during a write. The file is truncated at the first bad frame,
   and a warning is recorded in `persist_stats()["last_error"]`. Records after that
   point are discarded.
5. Expired keys are removed, and the removals are logged.

Recovery uses a frozen clock during replay, so no key expires while it is being
rebuilt. The real clock is installed afterwards.

## Expiry and downtime

TTLs are stored as absolute times (milliseconds since the Unix epoch), not as
remaining durations. A key with a TTL therefore keeps expiring while the process
is down. If you restart after the deadline, the key is gone. If you restart before
it, the key comes back with the time that is left.

## fsync policy

WAL records are always handed to the operating system with a single `write`.
The `fsync` policy controls only when the data is forced to the physical disk.

| Policy | When data is synced | Survives a process kill (SIGKILL) | Survives power loss |
|---|---|---|---|
| `always` | After every record | Yes | Intended to, **not verified** |
| `everysec` | About once per second, from a background thread | Yes | Up to about one second of writes may be lost. Not verified. |
| `never` | Left to the operating system | Yes | Not guaranteed |

The cost of `always` is one fsync per write. `everysec` is the usual balance.
`never` is the fastest and the least durable.

Snapshots are always fsynced before they replace the old one, whatever the WAL
policy.

## Locking

A `LOCK` file holds the pid of the owner. Opening a directory fails with
`CacheLockedError` if the lock is held by another live process, or by another
`Cache` in the same process.

A stale lock, left by a process that died, is taken over when its pid is no
longer alive. On Windows, liveness is checked with `OpenProcess`. If that check
cannot decide, the lock is treated as live, which is the safe direction. A pid
that has been reused by an unrelated live process also blocks the open, and the
operator has to remove `LOCK` by hand.

Only one process may own a persist directory. Other processes should talk to
the owner with `tinycache.Client`. See [README: Sharing a cache between processes](../README.md#sharing-a-cache-between-processes).

## Snapshots on demand

- `cache.save()` (Python), or `SAVE` / `Client.save()` on the server, writes a
  snapshot now and blocks until it is done.
- `BGSAVE` / `Client.bgsave()` starts one on a background thread and returns at
  once. Only one background save runs at a time, so a second `BGSAVE` while one
  is running is refused with `ERR Background save already in progress`.
- `LASTSAVE` / `Client.lastsave()` returns the Unix time of the last successful
  snapshot in this process, or 0.

## Errors

| Situation | Python exception | CLI |
|---|---|---|
| Directory held by another live process | `CacheLockedError` (a subclass of `OSError`) | exit code 1 |
| `snapshot.tcs` fails its checksum or structure check | `CorruptDataError` (a subclass of `ValueError`) | exit code 1 |
| `save()` (or `SAVE` on a server without `--dir`) when persistence is off | `ValueError` (`ERR persistence is not enabled` on the wire) | not applicable: the CLI enables persistence only with `--dir` |
| I/O failure during a save | `OSError` | logged; see `last_error` |

A corrupt snapshot makes the open fail. The store does **not** fall back to an
older state. The logs that would have been needed were deleted when that snapshot
was written. To start empty, move the damaged files aside.

## `persist_stats`

`cache.persist_stats` is a property. It returns `None` when persistence is off,
and otherwise a dict with these keys:

| Key | Meaning |
|---|---|
| `last_save_ms` | Wall-clock milliseconds of the last successful snapshot in this process, or 0. |
| `snapshot_bytes` | Size of the current `snapshot.tcs` in bytes, or 0. |
| `wal_bytes` | Bytes in the active WAL generation. |
| `loaded_keys` | Live keys present after recovery. |
| `last_error` | The most recent persistence error or recovery warning, or `None`. A successful save clears it. |

## What is guaranteed and what is not

Be precise about this. These statements come from the code and its tests.

**Tested:**

- A kill with SIGKILL (not a power cut) of a process that has written keys: the
  keys are recovered on the next open. The test runs with `fsync="always"`.
- A kill after `save()`: the saved snapshot is kept.
- A torn WAL tail is truncated and reported, not treated as fatal.
- A corrupt snapshot (bad checksum or magic) makes the open fail with
  `CorruptDataError`.
- A lock held by a killed process is taken over. A lock held by a live
  subprocess is refused.
- `close()` writes a final snapshot and releases the directory, so another cache
  can open it.
- Background snapshots, and the `SIGTERM`/`SIGINT` paths of the CLI, write a
  snapshot before the process exits.

**Not verified:**

- **Power-loss durability** of `always` and `everysec`. Nothing tests an actual
  power failure, a kernel crash, or a disk that reorders writes. The design calls
  fsync at the right points, but that has not been demonstrated.
- **Windows stale-lock takeover.** The `OpenProcess` path was written and
  type-checked, but it has not been run on Windows.
- **Directory fsync** is Unix-only. On other systems a rename or new file may not
  be durable after a crash.

**Limits by design:**

- **Single node.** No replication, no failover, no clustering. Losing the disk
  loses the data unless you copy the directory yourself.
- **One owner per directory.** Share the data through a running server and
  `Client`, not by opening the same directory from two processes.
- **No history.** Only the current state is kept. There is no point-in-time
  recovery beyond the latest snapshot and the current log.
- **Mutations after `close()` are not logged.** The store stays readable after
  `close()`, but writes made after it are not durable.
- **No fallback after corruption.** See the errors section above.
- **Memory is still the working set.** Persistence does not let the data set
  exceed RAM.

## Running as a service (untested examples)

These examples are a starting point. They have not been run on a real host. Check
the paths, the user, and the password handling before using them.

### systemd

```ini
# /etc/systemd/system/tinycache.service
[Unit]
Description=tinycache server
After=network.target

[Service]
User=tinycache
Group=tinycache
Environment=TINYCACHE_PASSWORD=change-me
ExecStart=/usr/bin/env tinycache-server --host 127.0.0.1 --port 6380 \
    --dir /var/lib/tinycache --fsync everysec --snapshot-interval 300 \
    --max-memory 512mb --policy allkeys-lru
# SIGTERM makes the server save a final snapshot before it exits.
KillSignal=SIGTERM
TimeoutStopSec=30
Restart=on-failure

[Install]
WantedBy=multi-user.target
```

Keep the password out of the unit file in production: put it in a root-readable
`EnvironmentFile=` instead, and make sure it is not world-readable.

### Dockerfile

```dockerfile
# Dockerfile (untested example)
FROM python:3.12-slim
RUN pip install --no-cache-dir tinycache-rs
RUN useradd --system --home /data tinycache && mkdir -p /data && chown tinycache /data
USER tinycache
VOLUME ["/data"]
EXPOSE 6380
# Bind to all interfaces only with a password set, or the server refuses to start.
ENV TINYCACHE_PASSWORD=change-me
CMD ["tinycache-server", "--host", "0.0.0.0", "--port", "6380", "--dir", "/data"]
```

Do not ship `TINYCACHE_PASSWORD=change-me` as a real default. Pass the password
at run time with `-e` or a secret.
