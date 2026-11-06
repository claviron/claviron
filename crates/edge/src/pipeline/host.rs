//! Compiling a host's site into a prepared plan. The plan itself is
//! `claviron_pipeline::plan`.

pub(crate) mod compile;
mod registration;

pub use claviron_pipeline::plan::*;
pub use registration::LayerContext;
pub(in crate::pipeline) use registration::LayerRegistration;
