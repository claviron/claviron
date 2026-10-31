//! Native embedded runtime backends, after application and file dispatch.
//!
//! Router adapters belong to the Application boundary. An embedded backend
//! receives the already-owned RouteBody and never constructs an Axum router.

#[cfg(feature = "lua")]
pub mod lua;
#[cfg(any(feature = "php_embed", feature = "php_fcgi"))]
pub mod php;
