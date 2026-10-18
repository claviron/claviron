//! A revision wake-up only. Peers recover full membership from the table even
//! when this event is lost/coalesced or predates their subscription.
use claviron_orbit::FleetEventCodec;
use claviron_runtime::events::Event;
use serde::{Deserialize, Serialize};

use super::ResolutionGeneration;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolutionChanged {
    pub profile: String,
    pub identity: String,
    pub revision: u64,
    pub generation: ResolutionGeneration,
}
impl Event for ResolutionChanged {}
impl FleetEventCodec for ResolutionChanged {
    const TOPIC: &'static str = "claviron.upstream.resolution.changed.v1";
    fn encode_orbit(&self) -> Vec<u8> {
        // String/integer serialization is infallible for this fixed envelope.
        serde_json::to_vec(self).expect("resolution event serialization")
    }
    fn decode_orbit(bytes: &[u8]) -> Result<Self, String> {
        let event: Self =
            serde_json::from_slice(bytes).map_err(|error| error.to_string())?;
        // Identity is the same full escaped key bound as the table, not an
        // address list squeezed into an event frame.
        if event.profile.is_empty()
            || event.profile.len() > crate::state::layout::KEY_MAX
            || event.identity.is_empty()
            || event.identity.len() > crate::state::layout::KEY_MAX
            || event.identity.contains('\0')
            || event.revision == 0
        {
            return Err("invalid resolution wake-up identity/revision".into());
        }
        Ok(event)
    }
}
