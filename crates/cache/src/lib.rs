mod bootstrap;
pub mod config;
mod drivers;
pub mod error;
mod provider;
mod registry;
mod store;
mod value;

pub use bootstrap::{register, register_with_config};
#[cfg(feature = "file")]
pub use drivers::file::{FileCacheDriver, FileCacheStore};
pub use drivers::orbit::{
    OrbitCacheDriver, OrbitCacheStore, OrbitCacheStoreConfig,
};
#[cfg(feature = "redis")]
pub use drivers::redis::RedisConfig;
#[cfg(feature = "redis")]
pub use drivers::redis::RedisTlsConfig;
#[cfg(feature = "redis")]
pub use drivers::redis::{RedisCacheDriver, RedisCacheStoreConfig};
#[cfg(feature = "redis")]
pub use drivers::redis::{RedisCacheStore, RedisProvider};
pub use store::CacheRepository;

pub use self::config::{
    CacheConfig, CacheConfig as CacheBootstrapConfig, FileCacheStoreConfig,
};
pub use self::error::{Error, Result};
pub use self::provider::CacheProvider;
pub use self::registry::CacheRegistry;
pub use self::store::{CacheDriver, CacheStore, CacheStoreResolver};
pub use self::value::{
    ArchivedCacheJson, CacheJson, CacheValue, CacheValueRef, ValueError,
    ValueResult,
};
