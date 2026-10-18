extern crate self as claviron;

/// The axum this workspace resolves. A consumer that builds routes with
/// `claviron::axum` cannot end up with two of them.
#[cfg(feature = "axum")]
pub use axum;
#[cfg(feature = "application")]
pub use claviron_app as app;
#[cfg(feature = "auth")]
pub use claviron_auth as auth;
#[cfg(feature = "bench")]
pub use claviron_bench as bench;
#[cfg(feature = "server")]
pub mod server;
#[cfg(feature = "broadcast")]
pub use claviron_broadcast as broadcast;
#[cfg(feature = "cache")]
pub use claviron_cache as cache;
#[cfg(feature = "db")]
pub use claviron_db as db;
#[cfg(feature = "edge")]
pub use claviron_edge as edge;
/// Host and listener configuration types. `edge` owns the runtime;
/// this is the shape its yaml parses into, which a consumer needs to
/// name a listener policy of its own.
#[cfg(feature = "edge")]
pub use claviron_edge::config as edge_config;
#[cfg(feature = "ffi")]
pub use claviron_ffi as ffi;
#[cfg(feature = "files")]
pub use claviron_files as files;
#[cfg(feature = "geoip")]
pub use claviron_geoip as geoip;
pub use claviron_helpers as helpers;
#[cfg(feature = "hosting")]
pub use claviron_hosting as hosting;
#[cfg(feature = "http")]
pub use claviron_http as http;
#[cfg(feature = "laravel")]
pub use claviron_laravel as laravel;
#[cfg(feature = "logging")]
pub use claviron_logging as logging;
#[cfg(feature = "lua")]
pub use claviron_lua as lua;
#[cfg(feature = "metrics")]
pub use claviron_metrics as metrics;
#[cfg(feature = "orbit")]
pub use claviron_orbit as orbit;
#[cfg(feature = "php")]
pub use claviron_php as php;
#[cfg(feature = "php_embed")]
pub use claviron_php_embed as ephp;
#[cfg(feature = "php_fcgi")]
pub use claviron_php_fcgi as fcgi;
#[cfg(feature = "resolver")]
pub use claviron_resolver as resolver;
#[cfg(feature = "runner")]
pub use claviron_runner as runner;
#[cfg(feature = "runtime")]
pub use claviron_runtime as runtime;
#[cfg(feature = "runtime")]
pub use claviron_runtime::*;
#[cfg(feature = "supervisor")]
pub use claviron_supervisor as supervisor;
#[cfg(feature = "sys")]
pub use claviron_sys as sys;
#[cfg(feature = "tasks")]
pub use claviron_tasks as tasks;
#[cfg(feature = "tls")]
pub use claviron_tls as tls;
#[cfg(feature = "ssl")]
pub use claviron_types::ssl;
#[cfg(feature = "upstream")]
pub use claviron_upstream as upstream;
#[cfg(feature = "websocket")]
pub use claviron_websocket as websocket;
pub use error::{Error, Result};
pub use orbitive;
#[cfg(feature = "clients")]
pub mod clients;
#[cfg(feature = "server")]
pub use server::{Server, ServerOptions, bootstrap};
mod error;
