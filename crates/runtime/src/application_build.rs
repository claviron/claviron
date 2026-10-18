//! Optional identity supplied by the embedding application, not inferred from
//! the runtime crate's package version or from the executable filename.

/// Register during application bootstrap with `env!("CARGO_PKG_VERSION")`.
/// Every re-executed worker runs the same registration.
#[derive(Clone, Debug)]
pub struct ApplicationBuild {
    pub version: String,
}
