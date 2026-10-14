//! pydirs: a lightweight, Redis-like in-memory cache.
//!
//! The Rust core lives in [`core`] and the RESP2 server in [`server`]. With the
//! default `python` feature, this crate also builds the PyO3 extension module
//! `_pydirs`, which the pure-Python `pydirs` package wraps.

pub mod core;
pub mod server;

#[cfg(feature = "python")]
use pyo3::prelude::*;

#[cfg(feature = "python")]
mod bindings {
    use std::io;
    use std::path::{Path, PathBuf};
    use std::sync::Mutex;

    use pyo3::create_exception;
    use pyo3::exceptions::{PyMemoryError, PyOSError, PyTypeError, PyValueError};
    use pyo3::prelude::*;
    use pyo3::types::{
        PyBool, PyBytes, PyDict, PyFloat, PyInt, PySet, PyString, PyStringMethods, PyTuple,
    };

    use crate::core::{Config, FsyncPolicy, PersistConfig, Policy, Store, StoreError};
    use crate::server::{self, ServerHandle, ServerOptions};

    create_exception!(
        _pydirs,
        CacheLockedError,
        PyOSError,
        "The persistence directory is in use by another live process."
    );
    create_exception!(
        _pydirs,
        CorruptDataError,
        PyValueError,
        "The persisted snapshot failed its checksum or structure check."
    );

    /// Maps a core error onto the Python exception documented in docs/api.md.
    fn to_py_err(e: StoreError) -> PyErr {
        let msg = e.to_string();
        match e {
            StoreError::WrongType => PyTypeError::new_err(msg),
            StoreError::Oom => PyMemoryError::new_err(msg),
            StoreError::NotInteger | StoreError::Overflow => PyValueError::new_err(msg),
        }
    }

    /// Maps an error from opening a persistence directory. `WouldBlock` means the lock is
    /// held, `InvalidData` means the snapshot is corrupt, anything else is a plain OSError.
    fn open_err(dir: &Path, e: io::Error) -> PyErr {
        match e.kind() {
            io::ErrorKind::WouldBlock => CacheLockedError::new_err(format!(
                "data directory {} is in use by another live process",
                dir.display()
            )),
            io::ErrorKind::InvalidData => {
                CorruptDataError::new_err(format!("cannot load data from {}: {e}", dir.display()))
            }
            _ => PyOSError::new_err(format!("cannot open data directory {}: {e}", dir.display())),
        }
    }

    fn save_err(e: io::Error) -> PyErr {
        PyOSError::new_err(format!("save failed: {e}"))
    }

    fn parse_fsync(name: &str) -> PyResult<FsyncPolicy> {
        match name {
            "always" => Ok(FsyncPolicy::Always),
            "everysec" => Ok(FsyncPolicy::EverySec),
            "never" => Ok(FsyncPolicy::Never),
            other => Err(PyValueError::new_err(format!(
                "unknown fsync {other:?}; expected 'always', 'everysec' or 'never'"
            ))),
        }
    }

    /// Converts a key or value (bytes, str, int or float) into the bytes the core stores.
    /// `bool` is rejected even though it subclasses `int`, because `str(True)` is surprising.
    pub(crate) fn to_bytes(obj: &Bound<'_, PyAny>) -> PyResult<Vec<u8>> {
        if let Ok(b) = obj.downcast::<PyBytes>() {
            return Ok(b.as_bytes().to_vec());
        }
        if let Ok(s) = obj.downcast::<PyString>() {
            return Ok(s.to_cow()?.as_bytes().to_vec());
        }
        if obj.is_instance_of::<PyBool>() {
            return Err(PyTypeError::new_err(
                "expected bytes, str, int or float, not bool",
            ));
        }
        if obj.is_instance_of::<PyInt>() || obj.is_instance_of::<PyFloat>() {
            return Ok(obj.str()?.to_cow()?.as_bytes().to_vec());
        }
        Err(PyTypeError::new_err(format!(
            "expected bytes, str, int or float, not {}",
            obj.get_type().name()?
        )))
    }

    /// Converts every item of an iterable of keys.
    fn collect_bytes(obj: &Bound<'_, PyAny>) -> PyResult<Vec<Vec<u8>>> {
        obj.try_iter()?
            .map(|item| to_bytes(&item?))
            .collect::<PyResult<Vec<_>>>()
    }

    /// Converts a mapping (anything with `items()`) into key/value byte pairs.
    fn mapping_items(obj: &Bound<'_, PyAny>) -> PyResult<Vec<(Vec<u8>, Vec<u8>)>> {
        if !obj.hasattr("items")? {
            return Err(PyTypeError::new_err(format!(
                "expected a mapping, not {}",
                obj.get_type().name()?
            )));
        }
        obj.call_method0("items")?
            .try_iter()?
            .map(|kv| {
                let kv = kv?;
                let (k, v): (Bound<'_, PyAny>, Bound<'_, PyAny>) = kv.extract()?;
                Ok((to_bytes(&k)?, to_bytes(&v)?))
            })
            .collect()
    }

    /// Borrows owned byte strings as slices for the core's `&[&[u8]]` signatures.
    fn as_refs(items: &[Vec<u8>]) -> Vec<&[u8]> {
        items.iter().map(Vec::as_slice).collect()
    }

    /// Validates a strictly positive Python int (bool is rejected).
    fn positive_int(obj: &Bound<'_, PyAny>, name: &str) -> PyResult<u64> {
        if obj.is_instance_of::<PyBool>() || !obj.is_instance_of::<PyInt>() {
            return Err(PyValueError::new_err(format!(
                "{name} must be a positive integer"
            )));
        }
        let v: i64 = obj
            .extract()
            .map_err(|_| PyValueError::new_err(format!("{name} is out of range")))?;
        u64::try_from(v)
            .ok()
            .filter(|&n| n > 0)
            .ok_or_else(|| PyValueError::new_err(format!("{name} must be a positive integer")))
    }

    /// Resolves the `ex` (seconds) / `px` (milliseconds) pair into a TTL in ms.
    fn ttl_ms(
        ex: Option<&Bound<'_, PyAny>>,
        px: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Option<u64>> {
        match (ex, px) {
            (Some(_), Some(_)) => Err(PyValueError::new_err("ex and px are mutually exclusive")),
            (Some(e), None) => positive_int(e, "ex")?
                .checked_mul(1000)
                .map(Some)
                .ok_or_else(|| PyValueError::new_err("ex is too large")),
            (None, Some(p)) => positive_int(p, "px").map(Some),
            (None, None) => Ok(None),
        }
    }

    fn parse_policy(name: &str) -> PyResult<Policy> {
        match name {
            "noeviction" => Ok(Policy::NoEviction),
            "allkeys-lru" => Ok(Policy::AllKeysLru),
            "volatile-lru" => Ok(Policy::VolatileLru),
            other => Err(PyValueError::new_err(format!(
                "unknown policy {other:?}; expected 'noeviction', 'allkeys-lru' or 'volatile-lru'"
            ))),
        }
    }

    /// Bytes-level cache. The Python `pydirs.Cache` subclass adds decoding,
    /// JSON handling and dict-style sugar on top of this class.
    #[pyclass(name = "Cache", module = "pydirs._pydirs", subclass)]
    pub struct PyCache {
        store: Store,
        persist_dir: Option<PathBuf>,
    }

    #[pymethods]
    impl PyCache {
        #[new]
        #[pyo3(signature = (max_memory=0, policy="noeviction", shards=64, persist_dir=None,
                            fsync="everysec", snapshot_interval=300))]
        #[allow(clippy::too_many_arguments)]
        fn new(
            py: Python<'_>,
            max_memory: usize,
            policy: &str,
            shards: usize,
            persist_dir: Option<PathBuf>,
            fsync: &str,
            snapshot_interval: i64,
        ) -> PyResult<Self> {
            if shards == 0 || !shards.is_power_of_two() || shards > 65_536 {
                return Err(PyValueError::new_err(
                    "shards must be a power of two between 1 and 65536",
                ));
            }
            let policy = parse_policy(policy)?;
            let fsync = parse_fsync(fsync)?;
            let snapshot_interval_secs = u64::try_from(snapshot_interval)
                .map_err(|_| PyValueError::new_err("snapshot_interval must be >= 0"))?;
            let persist = persist_dir.clone().map(|dir| PersistConfig {
                dir,
                fsync,
                snapshot_interval_secs,
            });
            let cfg = Config {
                shards,
                max_memory,
                policy,
                persist,
                ..Config::default()
            };
            let store = match persist_dir.as_deref() {
                Some(dir) => {
                    let dir = dir.to_path_buf();
                    py.allow_threads(move || Store::open(cfg))
                        .map_err(|e| open_err(&dir, e))?
                }
                None => Store::new(cfg),
            };
            Ok(Self { store, persist_dir })
        }

        fn close(&self, py: Python<'_>) {
            let store = &self.store;
            py.allow_threads(move || store.close());
        }

        /// Writes a snapshot now. Raises OSError on I/O failure, ValueError if persistence is off.
        fn save(&self, py: Python<'_>) -> PyResult<()> {
            if self.persist_dir.is_none() {
                return Err(PyValueError::new_err("persistence is not enabled"));
            }
            let store = &self.store;
            py.allow_threads(move || store.save()).map_err(save_err)
        }

        /// True when this cache was opened with a `persist_dir`.
        #[getter]
        fn persistent(&self) -> bool {
            self.persist_dir.is_some()
        }

        /// Persistence counters as a dict, or None when persistence is off.
        #[getter]
        fn persist_stats<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyDict>>> {
            let Some(stats) = self.store.persist_stats() else {
                return Ok(None);
            };
            let d = PyDict::new(py);
            d.set_item("last_save_ms", stats.last_save_ms)?;
            d.set_item("snapshot_bytes", stats.snapshot_bytes)?;
            d.set_item("wal_bytes", stats.wal_bytes)?;
            d.set_item("loaded_keys", stats.loaded_keys)?;
            d.set_item("last_error", stats.last_error)?;
            Ok(Some(d))
        }

        // ---- strings ----------------------------------------------------

        #[allow(clippy::too_many_arguments)]
        #[pyo3(signature = (key, value, ex=None, px=None, nx=false, xx=false))]
        fn set(
            &self,
            py: Python<'_>,
            key: &Bound<'_, PyAny>,
            value: &Bound<'_, PyAny>,
            ex: Option<Bound<'_, PyAny>>,
            px: Option<Bound<'_, PyAny>>,
            nx: bool,
            xx: bool,
        ) -> PyResult<bool> {
            let ttl = ttl_ms(ex.as_ref(), px.as_ref())?;
            if nx && xx {
                return Err(PyValueError::new_err("nx and xx are mutually exclusive"));
            }
            let k = to_bytes(key)?;
            let v = to_bytes(value)?;
            let store = &self.store;
            py.allow_threads(move || store.set(&k, v, ttl, nx, xx))
                .map_err(to_py_err)
        }

        fn get<'py>(
            &self,
            py: Python<'py>,
            key: &Bound<'py, PyAny>,
        ) -> PyResult<Option<Bound<'py, PyBytes>>> {
            let k = to_bytes(key)?;
            let store = &self.store;
            let v = py.allow_threads(move || store.get(&k)).map_err(to_py_err)?;
            Ok(v.map(|b| PyBytes::new(py, &b)))
        }

        fn mget<'py>(
            &self,
            py: Python<'py>,
            keys: &Bound<'py, PyAny>,
        ) -> PyResult<Vec<Option<Bound<'py, PyBytes>>>> {
            let ks = collect_bytes(keys)?;
            let store = &self.store;
            let vals = py.allow_threads(move || store.mget(&as_refs(&ks)));
            Ok(vals
                .into_iter()
                .map(|v| v.map(|b| PyBytes::new(py, &b)))
                .collect())
        }

        fn mset(&self, py: Python<'_>, mapping: &Bound<'_, PyAny>) -> PyResult<()> {
            let items = mapping_items(mapping)?;
            let store = &self.store;
            py.allow_threads(move || store.mset(items))
                .map_err(to_py_err)
        }

        #[pyo3(signature = (key, amount=1))]
        fn incr(&self, py: Python<'_>, key: &Bound<'_, PyAny>, amount: i64) -> PyResult<i64> {
            let k = to_bytes(key)?;
            let store = &self.store;
            py.allow_threads(move || store.incr_by(&k, amount))
                .map_err(to_py_err)
        }

        #[pyo3(signature = (key, amount=1))]
        fn decr(&self, py: Python<'_>, key: &Bound<'_, PyAny>, amount: i64) -> PyResult<i64> {
            let k = to_bytes(key)?;
            let delta = amount
                .checked_neg()
                .ok_or_else(|| PyValueError::new_err("decrement would overflow"))?;
            let store = &self.store;
            py.allow_threads(move || store.incr_by(&k, delta))
                .map_err(to_py_err)
        }

        fn append(
            &self,
            py: Python<'_>,
            key: &Bound<'_, PyAny>,
            value: &Bound<'_, PyAny>,
        ) -> PyResult<usize> {
            let k = to_bytes(key)?;
            let v = to_bytes(value)?;
            let store = &self.store;
            py.allow_threads(move || store.append(&k, &v))
                .map_err(to_py_err)
        }

        // ---- keys -------------------------------------------------------

        #[pyo3(signature = (*keys))]
        fn delete(&self, py: Python<'_>, keys: &Bound<'_, PyTuple>) -> PyResult<usize> {
            let ks = keys
                .iter()
                .map(|k| to_bytes(&k))
                .collect::<PyResult<Vec<_>>>()?;
            let store = &self.store;
            Ok(py.allow_threads(move || store.delete(&as_refs(&ks))))
        }

        fn exists(&self, py: Python<'_>, key: &Bound<'_, PyAny>) -> PyResult<bool> {
            let k = to_bytes(key)?;
            let store = &self.store;
            Ok(py.allow_threads(move || store.exists(&k)))
        }

        fn expire(
            &self,
            py: Python<'_>,
            key: &Bound<'_, PyAny>,
            seconds: &Bound<'_, PyAny>,
        ) -> PyResult<bool> {
            let ms = positive_int(seconds, "seconds")?
                .checked_mul(1000)
                .ok_or_else(|| PyValueError::new_err("seconds is too large"))?;
            let k = to_bytes(key)?;
            let store = &self.store;
            Ok(py.allow_threads(move || store.expire_ms(&k, ms)))
        }

        fn pexpire(
            &self,
            py: Python<'_>,
            key: &Bound<'_, PyAny>,
            milliseconds: &Bound<'_, PyAny>,
        ) -> PyResult<bool> {
            let ms = positive_int(milliseconds, "milliseconds")?;
            let k = to_bytes(key)?;
            let store = &self.store;
            Ok(py.allow_threads(move || store.expire_ms(&k, ms)))
        }

        fn persist(&self, py: Python<'_>, key: &Bound<'_, PyAny>) -> PyResult<bool> {
            let k = to_bytes(key)?;
            let store = &self.store;
            Ok(py.allow_threads(move || store.persist(&k)))
        }

        /// Remaining TTL in whole seconds, rounded up. -2 if missing, -1 if no expiry.
        fn ttl(&self, py: Python<'_>, key: &Bound<'_, PyAny>) -> PyResult<i64> {
            let k = to_bytes(key)?;
            let store = &self.store;
            let p = py.allow_threads(move || store.pttl(&k));
            Ok(if p < 0 {
                p
            } else {
                p / 1000 + i64::from(p % 1000 != 0)
            })
        }

        fn pttl(&self, py: Python<'_>, key: &Bound<'_, PyAny>) -> PyResult<i64> {
            let k = to_bytes(key)?;
            let store = &self.store;
            Ok(py.allow_threads(move || store.pttl(&k)))
        }

        #[pyo3(signature = (pattern="*"))]
        fn keys<'py>(&self, py: Python<'py>, pattern: &str) -> Vec<Bound<'py, PyBytes>> {
            let pat = pattern.to_owned();
            let store = &self.store;
            let found = py.allow_threads(move || store.keys(&pat));
            found.iter().map(|k| PyBytes::new(py, k)).collect()
        }

        fn flush(&self, py: Python<'_>) {
            let store = &self.store;
            py.allow_threads(move || store.flush());
        }

        #[getter]
        fn used_memory(&self, py: Python<'_>) -> usize {
            let store = &self.store;
            py.allow_threads(move || store.used_memory())
        }

        fn __len__(&self, py: Python<'_>) -> usize {
            let store = &self.store;
            py.allow_threads(move || store.dbsize())
        }

        // ---- hashes -----------------------------------------------------

        fn hset(
            &self,
            py: Python<'_>,
            key: &Bound<'_, PyAny>,
            field: &Bound<'_, PyAny>,
            value: &Bound<'_, PyAny>,
        ) -> PyResult<bool> {
            let k = to_bytes(key)?;
            let f = to_bytes(field)?;
            let v = to_bytes(value)?;
            let store = &self.store;
            py.allow_threads(move || store.hset(&k, &f, v))
                .map_err(to_py_err)
        }

        fn hget<'py>(
            &self,
            py: Python<'py>,
            key: &Bound<'py, PyAny>,
            field: &Bound<'py, PyAny>,
        ) -> PyResult<Option<Bound<'py, PyBytes>>> {
            let k = to_bytes(key)?;
            let f = to_bytes(field)?;
            let store = &self.store;
            let v = py
                .allow_threads(move || store.hget(&k, &f))
                .map_err(to_py_err)?;
            Ok(v.map(|b| PyBytes::new(py, &b)))
        }

        #[pyo3(signature = (key, *fields))]
        fn hdel(
            &self,
            py: Python<'_>,
            key: &Bound<'_, PyAny>,
            fields: &Bound<'_, PyTuple>,
        ) -> PyResult<usize> {
            let k = to_bytes(key)?;
            let fs = fields
                .iter()
                .map(|f| to_bytes(&f))
                .collect::<PyResult<Vec<_>>>()?;
            let store = &self.store;
            py.allow_threads(move || store.hdel(&k, &as_refs(&fs)))
                .map_err(to_py_err)
        }

        fn hgetall<'py>(
            &self,
            py: Python<'py>,
            key: &Bound<'py, PyAny>,
        ) -> PyResult<Bound<'py, PyDict>> {
            let k = to_bytes(key)?;
            let store = &self.store;
            let pairs = py
                .allow_threads(move || store.hgetall(&k))
                .map_err(to_py_err)?;
            let d = PyDict::new(py);
            for (f, v) in pairs {
                d.set_item(PyBytes::new(py, &f), PyBytes::new(py, &v))?;
            }
            Ok(d)
        }

        fn hlen(&self, py: Python<'_>, key: &Bound<'_, PyAny>) -> PyResult<usize> {
            let k = to_bytes(key)?;
            let store = &self.store;
            py.allow_threads(move || store.hlen(&k)).map_err(to_py_err)
        }

        // ---- lists ------------------------------------------------------

        #[pyo3(signature = (key, *values))]
        fn lpush(
            &self,
            py: Python<'_>,
            key: &Bound<'_, PyAny>,
            values: &Bound<'_, PyTuple>,
        ) -> PyResult<usize> {
            let k = to_bytes(key)?;
            let vs = values
                .iter()
                .map(|v| to_bytes(&v))
                .collect::<PyResult<Vec<_>>>()?;
            let store = &self.store;
            py.allow_threads(move || store.lpush(&k, vs))
                .map_err(to_py_err)
        }

        #[pyo3(signature = (key, *values))]
        fn rpush(
            &self,
            py: Python<'_>,
            key: &Bound<'_, PyAny>,
            values: &Bound<'_, PyTuple>,
        ) -> PyResult<usize> {
            let k = to_bytes(key)?;
            let vs = values
                .iter()
                .map(|v| to_bytes(&v))
                .collect::<PyResult<Vec<_>>>()?;
            let store = &self.store;
            py.allow_threads(move || store.rpush(&k, vs))
                .map_err(to_py_err)
        }

        fn lpop<'py>(
            &self,
            py: Python<'py>,
            key: &Bound<'py, PyAny>,
        ) -> PyResult<Option<Bound<'py, PyBytes>>> {
            let k = to_bytes(key)?;
            let store = &self.store;
            let v = py
                .allow_threads(move || store.lpop(&k))
                .map_err(to_py_err)?;
            Ok(v.map(|b| PyBytes::new(py, &b)))
        }

        fn rpop<'py>(
            &self,
            py: Python<'py>,
            key: &Bound<'py, PyAny>,
        ) -> PyResult<Option<Bound<'py, PyBytes>>> {
            let k = to_bytes(key)?;
            let store = &self.store;
            let v = py
                .allow_threads(move || store.rpop(&k))
                .map_err(to_py_err)?;
            Ok(v.map(|b| PyBytes::new(py, &b)))
        }

        fn lrange<'py>(
            &self,
            py: Python<'py>,
            key: &Bound<'py, PyAny>,
            start: i64,
            stop: i64,
        ) -> PyResult<Vec<Bound<'py, PyBytes>>> {
            let k = to_bytes(key)?;
            let store = &self.store;
            let items = py
                .allow_threads(move || store.lrange(&k, start, stop))
                .map_err(to_py_err)?;
            Ok(items.iter().map(|b| PyBytes::new(py, b)).collect())
        }

        fn llen(&self, py: Python<'_>, key: &Bound<'_, PyAny>) -> PyResult<usize> {
            let k = to_bytes(key)?;
            let store = &self.store;
            py.allow_threads(move || store.llen(&k)).map_err(to_py_err)
        }

        // ---- sets -------------------------------------------------------

        #[pyo3(signature = (key, *members))]
        fn sadd(
            &self,
            py: Python<'_>,
            key: &Bound<'_, PyAny>,
            members: &Bound<'_, PyTuple>,
        ) -> PyResult<usize> {
            let k = to_bytes(key)?;
            let ms = members
                .iter()
                .map(|m| to_bytes(&m))
                .collect::<PyResult<Vec<_>>>()?;
            let store = &self.store;
            py.allow_threads(move || store.sadd(&k, ms))
                .map_err(to_py_err)
        }

        #[pyo3(signature = (key, *members))]
        fn srem(
            &self,
            py: Python<'_>,
            key: &Bound<'_, PyAny>,
            members: &Bound<'_, PyTuple>,
        ) -> PyResult<usize> {
            let k = to_bytes(key)?;
            let ms = members
                .iter()
                .map(|m| to_bytes(&m))
                .collect::<PyResult<Vec<_>>>()?;
            let store = &self.store;
            py.allow_threads(move || store.srem(&k, &as_refs(&ms)))
                .map_err(to_py_err)
        }

        fn smembers<'py>(
            &self,
            py: Python<'py>,
            key: &Bound<'py, PyAny>,
        ) -> PyResult<Bound<'py, PySet>> {
            let k = to_bytes(key)?;
            let store = &self.store;
            let members = py
                .allow_threads(move || store.smembers(&k))
                .map_err(to_py_err)?;
            PySet::new(py, members.iter().map(|m| PyBytes::new(py, m)))
        }

        fn sismember(
            &self,
            py: Python<'_>,
            key: &Bound<'_, PyAny>,
            member: &Bound<'_, PyAny>,
        ) -> PyResult<bool> {
            let k = to_bytes(key)?;
            let m = to_bytes(member)?;
            let store = &self.store;
            py.allow_threads(move || store.sismember(&k, &m))
                .map_err(to_py_err)
        }

        fn scard(&self, py: Python<'_>, key: &Bound<'_, PyAny>) -> PyResult<usize> {
            let k = to_bytes(key)?;
            let store = &self.store;
            py.allow_threads(move || store.scard(&k)).map_err(to_py_err)
        }
    }

    /// Server state guarded by a mutex. Every lock is taken with the GIL released,
    /// so a thread that holds the lock never waits on the GIL.
    struct ServerState {
        handle: Option<ServerHandle>,
        /// Port of the most recent bind, or the configured port before the first start.
        last_port: u16,
    }

    /// RESP2 server that serves a `Cache` over TCP.
    #[pyclass(name = "Server", module = "pydirs._pydirs", subclass)]
    pub struct PyServer {
        store: Store,
        host: String,
        configured_port: u16,
        options: ServerOptions,
        state: Mutex<ServerState>,
    }

    #[pymethods]
    impl PyServer {
        #[new]
        #[pyo3(signature = (cache, host="127.0.0.1", port=6380, password=None))]
        fn new(
            cache: PyRef<'_, PyCache>,
            host: &str,
            port: i64,
            password: Option<&Bound<'_, PyAny>>,
        ) -> PyResult<Self> {
            let port = u16::try_from(port)
                .map_err(|_| PyValueError::new_err("port must be between 0 and 65535"))?;
            let password = match password {
                None => None,
                Some(p) => {
                    let s = p
                        .downcast::<PyString>()
                        .map_err(|_| PyTypeError::new_err("password must be a str"))?;
                    let text = s.to_cow()?.into_owned();
                    if text.is_empty() {
                        return Err(PyValueError::new_err("password must not be empty"));
                    }
                    Some(text)
                }
            };
            Ok(Self {
                store: cache.store.clone(),
                host: host.to_owned(),
                configured_port: port,
                options: ServerOptions { password },
                state: Mutex::new(ServerState {
                    handle: None,
                    last_port: port,
                }),
            })
        }

        /// Binds and starts accepting connections. Calling it on a running server does nothing.
        /// Raises OSError if the address cannot be bound.
        fn start(&self, py: Python<'_>) -> PyResult<()> {
            py.allow_threads(|| {
                let mut st = self.state.lock().unwrap_or_else(|p| p.into_inner());
                if st.handle.is_some() {
                    return Ok(());
                }
                let handle = server::start_with_options(
                    self.store.clone(),
                    &self.host,
                    self.configured_port,
                    self.options.clone(),
                )
                .map_err(|e| {
                    PyOSError::new_err(format!(
                        "cannot bind {}:{}: {e}",
                        self.host, self.configured_port
                    ))
                })?;
                st.last_port = handle.port();
                st.handle = Some(handle);
                Ok(())
            })
        }

        /// Stops accepting connections and closes open ones. Safe to call more than once.
        fn stop(&self, py: Python<'_>) {
            py.allow_threads(|| {
                let mut st = self.state.lock().unwrap_or_else(|p| p.into_inner());
                if let Some(handle) = st.handle.take() {
                    st.last_port = handle.port();
                    handle.stop();
                }
            });
        }

        /// The bound port. With port=0 this is the port the OS picked after `start()`.
        #[getter]
        fn port(&self) -> u16 {
            let st = self.state.lock().unwrap_or_else(|p| p.into_inner());
            st.handle.as_ref().map_or(st.last_port, ServerHandle::port)
        }

        /// True between a successful `start()` and the next `stop()`.
        #[getter]
        fn running(&self) -> bool {
            let st = self.state.lock().unwrap_or_else(|p| p.into_inner());
            st.handle.is_some()
        }
    }

    impl Drop for PyServer {
        fn drop(&mut self) {
            if let Ok(st) = self.state.get_mut() {
                if let Some(handle) = st.handle.take() {
                    handle.stop();
                }
            }
        }
    }
}

#[cfg(feature = "python")]
#[pymodule]
fn _pydirs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<bindings::PyCache>()?;
    m.add_class::<bindings::PyServer>()?;
    m.add(
        "CacheLockedError",
        m.py().get_type::<bindings::CacheLockedError>(),
    )?;
    m.add(
        "CorruptDataError",
        m.py().get_type::<bindings::CorruptDataError>(),
    )?;
    Ok(())
}
