//! Compiling a host's site into a prepared plan. The plan itself is
//! `claviron_pipeline::plan`.

mod binding;
pub(crate) mod compile;
mod registration;

pub(crate) use binding::HostPolicy;
pub use claviron_pipeline::plan::*;
pub use registration::LayerContext;
pub(in crate::pipeline) use registration::LayerRegistration;
