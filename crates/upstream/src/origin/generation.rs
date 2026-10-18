//! Owns pool and resolution generation identities.

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PoolGeneration(pub u64);

/// One owner's monotonically published address-set revision, separate from
/// worker-local connection pool generations. Zero precedes the first answer.
#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    serde::Serialize,
    serde::Deserialize,
)]
pub struct ResolutionGeneration(pub u64);

impl PoolGeneration {
    pub const fn initial() -> Self {
        Self(0)
    }
}
