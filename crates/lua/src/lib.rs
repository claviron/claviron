//! Lua 5.5 embedded in Claviron.
//!
//! Every script runs the way PHP runs one: a fresh sandboxed state, the
//! registered extensions installed into it, the script driven to its end as a
//! future on Tokio, and the state dropped. Nothing a script keeps outlives it;
//! what must be shared goes through Orbit. See `ARCHITECTURE.md`.

#[cfg(feature = "app")]
pub mod app;
#[cfg(feature = "server")]
pub mod config;
#[cfg(any(feature = "postgres", feature = "mysql", feature = "sqlite"))]
pub mod db;
mod engine;
#[cfg(feature = "orbit")]
pub mod events;
mod execution;
#[cfg(feature = "fetch")]
pub mod fetch;
#[cfg(feature = "server")]
pub mod host;
#[cfg(feature = "orbit")]
pub mod invoke;
pub mod json;
#[cfg(feature = "log")]
pub mod log;
#[cfg(feature = "orbit")]
pub mod orbit;
mod packages;
pub mod regex;
pub mod router;
#[cfg(feature = "axum")]
pub mod routing;
#[cfg(feature = "app")]
pub mod serve;
#[cfg(any(feature = "websocket", feature = "webtransport"))]
mod socket;
#[cfg(feature = "orbit")]
pub mod stream;
#[cfg(feature = "tasks")]
pub mod task;
#[cfg(feature = "websocket")]
pub mod websocket;
#[cfg(feature = "webtransport")]
pub mod webtransport;

pub use engine::{Engine, Error, Extension, Output, Script};
pub use execution::Execution;
pub use json::JsonExtension;
pub use mlua;
pub use regex::RegexExtension;
pub use router::RouterExtension;

#[cfg(test)]
#[path = "../tests/tools.rs"]
mod tools_tests;
