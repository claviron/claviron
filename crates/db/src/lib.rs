pub mod config;
pub mod error;
#[cfg(all(feature = "server", feature = "postgres"))]
pub mod listener;
#[cfg(feature = "server")]
pub mod provider;

#[cfg(feature = "mysql")]
pub mod mysql;
#[cfg(feature = "postgres")]
pub mod postgres;
#[cfg(feature = "sqlite")]
pub mod sqlite;

#[cfg(feature = "sqlite")]
pub use config::SqliteEntry;
pub use config::{
    BackendConnections, ConnectInfo, ConnectionEntry, DbFileConfig, PoolConfig,
};
pub use error::{Error, Result};
#[cfg(all(feature = "server", feature = "postgres"))]
pub use listener::{ChannelListener, Notice};
#[cfg(all(feature = "server", feature = "postgres"))]
pub use provider::PostgresConnected;
#[cfg(feature = "server")]
pub use provider::{DbService, DbState, Pool, register, register_with_config};
