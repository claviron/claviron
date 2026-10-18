//! Owns fleet-carried origin health and recovery handoff events.

use claviron_orbit::FleetEventCodec;
use claviron_runtime::events::Event;
use serde::{Deserialize, Serialize};

pub(crate) type UpstreamOriginHealth = crate::origin::OriginHealth;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub(crate) struct OriginHealthEvent {
    pub origin_id: crate::origin::OriginId,
    pub profile: String,
    pub origin_name: Option<String>,
    pub origin_source: String,
    pub health: UpstreamOriginHealth,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub(crate) struct RecoveryVacatedEvent {
    pub profile: String,
    pub origin_name: Option<String>,
    pub origin_source: String,
    pub previous_owner: String,
    pub target_owner: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub(crate) struct OriginServingEvent {
    pub origin_id: crate::origin::OriginId,
    pub profile: String,
    pub origin_name: Option<String>,
    pub origin_source: String,
    pub serving: crate::origin::Serving,
}
impl Event for OriginServingEvent {}
impl FleetEventCodec for OriginServingEvent {
    const TOPIC: &'static str = "claviron.upstream.origin.serving.v1";
    fn encode_orbit(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap_or_default()
    }
    fn decode_orbit(bytes: &[u8]) -> Result<Self, String> {
        serde_json::from_slice(bytes).map_err(|err| err.to_string())
    }
}
impl Event for OriginHealthEvent {}

impl Event for RecoveryVacatedEvent {}

impl FleetEventCodec for OriginHealthEvent {
    const TOPIC: &'static str = "claviron.upstream.origin.health.v2";

    fn encode_orbit(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap_or_default()
    }

    fn decode_orbit(bytes: &[u8]) -> Result<Self, String> {
        serde_json::from_slice(bytes).map_err(|err| err.to_string())
    }
}

impl FleetEventCodec for RecoveryVacatedEvent {
    const TOPIC: &'static str = "claviron.upstream.recovery.vacated.v1";

    fn encode_orbit(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap_or_default()
    }

    fn decode_orbit(bytes: &[u8]) -> Result<Self, String> {
        serde_json::from_slice(bytes).map_err(|err| err.to_string())
    }
}
