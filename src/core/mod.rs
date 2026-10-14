//! In-process storage engine: sharded store, values, expiry and eviction.

mod evict;
mod expiry;
mod store;
mod value;

pub use store::{Clock, Config, Policy, Result, Store, StoreError};
