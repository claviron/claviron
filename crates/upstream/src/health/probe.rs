//! Typed probe decisions and the common owned-verdict application boundary.
pub(crate) mod failure;
mod l4;
mod l7;
mod message;
mod outcome;
pub(crate) use failure::OriginReachabilityFailure;
// Ordinals are the existing metric-slot ABI, not operational configuration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(usize)]
pub(crate) enum ProbeKind {
    Drop = 0,
    Recovery = 1,
    Active = 2,
}
impl ProbeKind {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Drop => "drop",
            Self::Recovery => "recovery",
            Self::Active => "active",
        }
    }
    #[cfg(feature = "metrics")]
    pub(crate) const fn index(self) -> usize {
        self as usize
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(usize)]
pub(crate) enum ProbeOutcome {
    Ok = 0,
    Fail = 1,
    Timeout = 2,
}
impl ProbeOutcome {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Fail => "fail",
            Self::Timeout => "timeout",
        }
    }
    #[cfg(feature = "metrics")]
    pub(crate) const fn index(self) -> usize {
        self as usize
    }
}
