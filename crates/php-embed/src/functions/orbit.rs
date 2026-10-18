//! PHP functions backed by Orbit primitives.

pub(crate) mod cell;
pub(crate) mod counter;
#[cfg(feature = "tasks")]
pub(crate) mod invoke;
pub(crate) mod lock;
