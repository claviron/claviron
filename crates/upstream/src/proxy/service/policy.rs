//! Owns the caller-supplied upgrade and extended CONNECT policy.

#[derive(Clone, Copy, Debug, Default)]
pub struct UpstreamRequestPolicy {
    pub allow_h1_upgrade: bool,
    pub allow_connect_protocol: bool,
}
