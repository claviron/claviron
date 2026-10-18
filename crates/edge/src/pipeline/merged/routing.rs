//! Boot-compiled, segment-aware routing primitives.
//!
//! HTTP owns shared routing contracts and selected-prefix dispatch. Edge
//! prepares host-specific application engines and consumer-owned directives at boot.

pub use claviron_http::config::routes::RouteTable;

pub(crate) use claviron_http::routing::{
    ConfiguredRoute, RouteDispatch, dispatch,
};

pub use crate::pipeline::merged::extensions::{
    RouteCondition, RouteContext, RouteEffect, RouteExtension, RouteLayer,
    RuleContext, directive,
};
