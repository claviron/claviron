//! What changed in the hosting data, told to every process of the fleet.
//!
//! PostgreSQL announces its own changes over NOTIFY, which reaches every
//! server sharing the database. SQLite has no such channel, and its file is
//! written only by processes of this host's fleet, so the store that wrote
//! says it: after the commit, as a fleet event every process receives, the
//! writer included. The certificate and alias listeners read the data again.

use serde::{Deserialize, Serialize};

/// Which side of the hosting data a committed write touched. It names no
/// record: a listener reads its server's side again, so a lost detail cannot
/// leave it half-updated.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HostingChanged {
    /// Vhosts, their aliases, or which servers they are on.
    Vhosts,
    /// Certificates, their aliases, or which servers they are on.
    Certificates,
}

#[cfg(feature = "server")]
impl claviron_runtime::events::Event for HostingChanged {}

#[cfg(feature = "server")]
impl claviron_orbit::FleetEventCodec for HostingChanged {
    const TOPIC: &'static str = "claviron.hosting.changed.v1";

    fn encode_orbit(&self) -> Vec<u8> {
        // A unit variant serializes without failure.
        serde_json::to_vec(self).expect("hosting event serialization")
    }

    fn decode_orbit(bytes: &[u8]) -> Result<Self, String> {
        serde_json::from_slice(bytes).map_err(|error| error.to_string())
    }
}
