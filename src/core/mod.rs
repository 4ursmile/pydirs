//! In-process storage engine: sharded store, values, expiry, eviction and persistence.

mod evict;
mod expiry;
mod persist;
mod store;
mod value;

pub use persist::{FsyncPolicy, PersistConfig, PersistStats};
pub use store::{Clock, Config, Policy, Result, Store, StoreError};

#[doc(hidden)]
pub use persist::SaveStep;
