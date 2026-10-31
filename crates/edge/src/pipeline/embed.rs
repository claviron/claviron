//! Native embedded runtime adapters.
//!
//! Edge owns mount selection and request handoff, not program execution or
//! engine lifecycle. Routing tests use prepared targets and test handlers;
//! execution tests belong to the runtime that owns the engine.
//!
//! Ordinary HTTP backends run after application and file dispatch. Optional
//! socket adapters register with the native protocol/session registries and
//! retain their own dispatch precedence. Router adapters belong to the
//! Application boundary; embedded backends never construct an Axum router.

#[cfg(feature = "lua")]
pub mod lua;
#[cfg(any(feature = "php_embed", feature = "php_fcgi"))]
pub mod php;
