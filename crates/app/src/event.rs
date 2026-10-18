//! User-level events exchanged by Claviron-hosted applications.

use claviron_orbit::{EVENT_PAYLOAD_MAX, FleetEventCodec};
use claviron_runtime::events::Event;
use uuid::Uuid;

use crate::ApplicationId;

mod value;

pub use value::{EventArrayKey, EventValue};

const APPLICATION_EVENT_ID_BYTES: usize = 16;
const APPLICATION_EVENT_HEADER_BYTES: usize =
    APPLICATION_EVENT_ID_BYTES + 2 + 2 + 2;
const ORBIT_EVENT_HEADER_BYTES: usize = 12;

/// The one Orbit wire topic shared by language-neutral application events.
pub const APPLICATION_EVENT_TOPIC: &str = "claviron.application.event.v3";

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct EventTopic(String);

impl EventTopic {
    pub fn new(
        topic: impl Into<String>
    ) -> Result<Self, ApplicationEventError> {
        let topic = topic.into();
        let normalized = topic.trim();
        if normalized.is_empty() {
            return Err(ApplicationEventError::EmptyTopic);
        }
        if normalized.len() > u16::MAX as usize {
            return Err(ApplicationEventError::TopicTooLong(normalized.len()));
        }
        Ok(Self(normalized.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for EventTopic {
    fn fmt(
        &self,
        formatter: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl AsRef<str> for EventTopic {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl std::str::FromStr for EventTopic {
    type Err = ApplicationEventError;

    fn from_str(topic: &str) -> Result<Self, Self::Err> {
        Self::new(topic)
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ApplicationEventError {
    #[error(
        "application event target component is too long: len={0} max=65535"
    )]
    TargetTooLong(usize),
    #[error("application event topic cannot be empty")]
    EmptyTopic,
    #[error("application event topic is too long: len={0} max=65535")]
    TopicTooLong(usize),
    #[error("application event frame is too large: len={actual} max={max}")]
    FrameTooLarge { actual: usize, max: usize },
    #[error("application event payload is invalid: {0}")]
    InvalidPayload(String),
    #[error("application event payload length overflow")]
    LengthOverflow,
}

/// A fleet-broadcast event shared by Claviron-hosted Rust, PHP and JavaScript
/// applications.
///
/// The outer Orbit topic identifies this stable codec. `topic` is the dynamic
/// domain topic applications publish and subscribe to. Delivery is bounded,
/// fire-and-forget and has no replay or acknowledgement semantics.
#[derive(Clone, Debug, PartialEq)]
pub struct ApplicationEvent {
    id: Uuid,
    target: Option<ApplicationId>,
    topic: EventTopic,
    payload: EventValue,
}

impl ApplicationEvent {
    pub fn new(
        topic: impl Into<String>,
        payload: EventValue,
    ) -> Result<Self, ApplicationEventError> {
        Self::with_id(Uuid::new_v4(), None, topic, payload)
    }

    /// Build an event private to one named language application across the
    /// fleet. Other runtimes and other applications discard it in Rust,
    /// before it reaches user code.
    pub fn for_application(
        target: ApplicationId,
        topic: impl Into<String>,
        payload: EventValue,
    ) -> Result<Self, ApplicationEventError> {
        Self::with_id(Uuid::new_v4(), Some(target), topic, payload)
    }

    pub fn with_id(
        id: Uuid,
        target: Option<ApplicationId>,
        topic: impl Into<String>,
        payload: EventValue,
    ) -> Result<Self, ApplicationEventError> {
        let event =
            Self { id, target, topic: EventTopic::new(topic)?, payload };
        event.validate_size()?;
        Ok(event)
    }

    pub fn id(&self) -> Uuid {
        self.id
    }

    pub fn topic(&self) -> &EventTopic {
        &self.topic
    }

    pub fn target(&self) -> Option<&ApplicationId> {
        self.target.as_ref()
    }

    pub fn payload(&self) -> &EventValue {
        &self.payload
    }

    pub fn into_parts(
        self
    ) -> (Uuid, Option<ApplicationId>, EventTopic, EventValue) {
        (self.id, self.target, self.topic, self.payload)
    }

    fn encoded_len(&self) -> Result<usize, ApplicationEventError> {
        let (runtime, application) =
            self.target.as_ref().map_or((0, 0), |target| {
                (target.runtime().as_str().len(), target.name().as_str().len())
            });
        if runtime > u16::MAX as usize {
            return Err(ApplicationEventError::TargetTooLong(runtime));
        }
        if application > u16::MAX as usize {
            return Err(ApplicationEventError::TargetTooLong(application));
        }
        let payload = self
            .payload
            .encoded_len()
            .map_err(ApplicationEventError::InvalidPayload)?;
        APPLICATION_EVENT_HEADER_BYTES
            .checked_add(runtime)
            .and_then(|len| len.checked_add(application))
            .and_then(|len| len.checked_add(self.topic.as_str().len()))
            .and_then(|len| len.checked_add(payload))
            .ok_or(ApplicationEventError::LengthOverflow)
    }

    fn validate_size(&self) -> Result<(), ApplicationEventError> {
        let event = self.encoded_len()?;
        let actual = ORBIT_EVENT_HEADER_BYTES
            .checked_add(APPLICATION_EVENT_TOPIC.len())
            .and_then(|len| len.checked_add(event))
            .ok_or(ApplicationEventError::LengthOverflow)?;
        if actual > EVENT_PAYLOAD_MAX {
            return Err(ApplicationEventError::FrameTooLarge {
                actual,
                max: EVENT_PAYLOAD_MAX,
            });
        }
        Ok(())
    }
}

impl Event for ApplicationEvent {}

/// The key an application event crosses a link by: the domain topic inside
/// the envelope. Every application event shares one ring topic, so routing by
/// that would put them all behind one interest. `None` for anything that is
/// not an application event, or that does not decode as one.
pub fn link_routing_key(event: &claviron_orbit::FleetEvent) -> Option<Vec<u8>> {
    if event.topic != APPLICATION_EVENT_TOPIC {
        return None;
    }
    let decoded = ApplicationEvent::decode_orbit(&event.payload).ok()?;
    Some(decoded.topic().as_str().as_bytes().to_vec())
}

impl FleetEventCodec for ApplicationEvent {
    const TOPIC: &'static str = APPLICATION_EVENT_TOPIC;

    fn encode_orbit(&self) -> Vec<u8> {
        let runtime = self
            .target
            .as_ref()
            .map_or(&[][..], |target| target.runtime().as_str().as_bytes());
        let application = self
            .target
            .as_ref()
            .map_or(&[][..], |target| target.name().as_str().as_bytes());
        let topic = self.topic.as_str().as_bytes();
        let payload = self.payload.encode();
        let mut encoded = Vec::with_capacity(
            APPLICATION_EVENT_HEADER_BYTES
                + runtime.len()
                + application.len()
                + topic.len()
                + payload.len(),
        );
        encoded.extend_from_slice(self.id.as_bytes());
        encoded.extend_from_slice(&(runtime.len() as u16).to_le_bytes());
        encoded.extend_from_slice(&(application.len() as u16).to_le_bytes());
        encoded.extend_from_slice(&(topic.len() as u16).to_le_bytes());
        encoded.extend_from_slice(runtime);
        encoded.extend_from_slice(application);
        encoded.extend_from_slice(topic);
        encoded.extend_from_slice(&payload);
        encoded
    }

    fn decode_orbit(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() < APPLICATION_EVENT_HEADER_BYTES {
            return Err("application event is too short".to_owned());
        }
        let id = Uuid::from_slice(&bytes[..APPLICATION_EVENT_ID_BYTES])
            .map_err(|error| {
                format!("application event ID is invalid: {error}")
            })?;
        let runtime_len = u16::from_le_bytes([
            bytes[APPLICATION_EVENT_ID_BYTES],
            bytes[APPLICATION_EVENT_ID_BYTES + 1],
        ]) as usize;
        let application_len = u16::from_le_bytes([
            bytes[APPLICATION_EVENT_ID_BYTES + 2],
            bytes[APPLICATION_EVENT_ID_BYTES + 3],
        ]) as usize;
        let topic_len = u16::from_le_bytes([
            bytes[APPLICATION_EVENT_ID_BYTES + 4],
            bytes[APPLICATION_EVENT_ID_BYTES + 5],
        ]) as usize;
        let runtime_start = APPLICATION_EVENT_HEADER_BYTES;
        let application_start = runtime_start
            .checked_add(runtime_len)
            .ok_or_else(|| "application event length overflow".to_owned())?;
        let topic_start = application_start
            .checked_add(application_len)
            .ok_or_else(|| "application event length overflow".to_owned())?;
        let payload_start = topic_start
            .checked_add(topic_len)
            .ok_or_else(|| "application event length overflow".to_owned())?;
        if payload_start > bytes.len() {
            return Err(
                "application event target or topic exceeds its payload"
                    .to_owned(),
            );
        }
        let runtime =
            std::str::from_utf8(&bytes[runtime_start..application_start])
                .map_err(|error| {
                    format!(
                        "application event target runtime is not UTF-8: {error}"
                    )
                })?;
        let application =
            std::str::from_utf8(&bytes[application_start..topic_start])
                .map_err(|error| {
                    format!(
                        "application event target name is not UTF-8: {error}"
                    )
                })?;
        let target = if runtime.is_empty() && application.is_empty() {
            None
        } else if runtime.is_empty() || application.is_empty() {
            return Err("application event target is incomplete".to_owned());
        } else {
            Some(ApplicationId::new(
                crate::ApplicationRuntime::new(runtime)
                    .map_err(|error| error.to_string())?,
                crate::ApplicationName::new(application)
                    .map_err(|error| error.to_string())?,
            ))
        };
        let topic = std::str::from_utf8(&bytes[topic_start..payload_start])
            .map_err(|error| {
                format!("application event topic is not UTF-8: {error}")
            })?;
        Self::with_id(
            id,
            target,
            topic,
            EventValue::decode(&bytes[payload_start..])?,
        )
        .map_err(|error| error.to_string())
    }
}

#[cfg(test)]
#[path = "../tests/event.rs"]
mod tests;
