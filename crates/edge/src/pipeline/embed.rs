//! Native embedded runtime adapters.
//!
//! Ordinary HTTP backends run after application and file dispatch. Optional
//! socket adapters register with the native protocol/session registries and
//! retain their own dispatch precedence. Router adapters belong to the
//! Application boundary; embedded backends never construct an Axum router.

#[cfg(feature = "lua")]
pub mod lua;
#[cfg(any(feature = "php_embed", feature = "php_fcgi"))]
pub mod php;
