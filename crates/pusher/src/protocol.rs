use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

const MAX_EVENT_DATA_BYTES: usize = 10 * 1024;
const MAX_EVENT_CHANNELS: usize = 100;

#[derive(Debug, Deserialize)]
pub struct ClientMessage {
    pub event: String,
    #[serde(default)]
    pub data: serde_json::Value,
    #[serde(default)]
    pub channel: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct SubscriptionRequest {
    pub channel: String,
    #[serde(default)]
    pub auth: Option<String>,
    #[serde(default)]
    pub channel_data: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct UnsubscribeRequest {
    pub channel: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TriggerEvent {
    pub name: String,
    pub data: serde_json::Value,
    #[serde(default)]
    pub channels: Vec<String>,
    #[serde(default)]
    pub channel: Option<String>,
    #[serde(default)]
    pub socket_id: Option<String>,
    #[serde(default)]
    pub info: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PublishedEvent {
    pub name: String,
    pub data: String,
    pub channels: Vec<String>,
    pub socket_id: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PresenceMember {
    pub user_id: String,
    pub user_info: serde_json::Value,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
pub struct PresenceSnapshot {
    pub ids: Vec<String>,
    pub hash: BTreeMap<String, serde_json::Value>,
    pub count: usize,
}

#[derive(Debug, thiserror::Error, Eq, PartialEq)]
pub enum PresenceMemberError {
    #[error("presence channel_data is not valid JSON: {0}")]
    Json(String),
    #[error("presence channel_data requires a scalar user_id")]
    UserId,
}

impl PresenceMember {
    pub fn parse(channel_data: &str) -> Result<Self, PresenceMemberError> {
        let mut data = serde_json::from_str::<serde_json::Value>(channel_data)
            .map_err(|error| PresenceMemberError::Json(error.to_string()))?;
        let object = data.as_object_mut().ok_or(PresenceMemberError::UserId)?;
        let user_id = match object.remove("user_id") {
            Some(serde_json::Value::String(id)) if !id.is_empty() => id,
            Some(serde_json::Value::Number(id)) => id.to_string(),
            _ => return Err(PresenceMemberError::UserId),
        };
        let user_info =
            object.remove("user_info").unwrap_or_else(|| serde_json::json!({}));
        Ok(Self { user_id, user_info })
    }
}

#[derive(Debug, thiserror::Error, Eq, PartialEq)]
pub enum TriggerEventError {
    #[error("event name cannot be empty")]
    EmptyName,
    #[error("an event requires one or more channels")]
    MissingChannel,
    #[error("an event cannot contain both channel and channels")]
    AmbiguousChannels,
    #[error("an event cannot target more than 100 channels")]
    TooManyChannels,
    #[error("channel names cannot be empty")]
    EmptyChannel,
    #[error("event data exceeds 10 KiB")]
    DataTooLarge,
    #[error("event data cannot be encoded: {0}")]
    Data(String),
    #[error("Pusher event info queries are not supported yet")]
    UnsupportedInfo,
}

impl TriggerEvent {
    pub fn validate(self) -> Result<PublishedEvent, TriggerEventError> {
        if self.name.trim().is_empty() {
            return Err(TriggerEventError::EmptyName);
        }
        if self.info.is_some() {
            return Err(TriggerEventError::UnsupportedInfo);
        }
        if self.channel.is_some() && !self.channels.is_empty() {
            return Err(TriggerEventError::AmbiguousChannels);
        }
        let channels = match self.channel {
            Some(channel) => vec![channel],
            None => self.channels,
        };
        if channels.is_empty() {
            return Err(TriggerEventError::MissingChannel);
        }
        if channels.len() > MAX_EVENT_CHANNELS {
            return Err(TriggerEventError::TooManyChannels);
        }
        if channels.iter().any(|channel| channel.trim().is_empty()) {
            return Err(TriggerEventError::EmptyChannel);
        }
        let data = match self.data {
            serde_json::Value::String(data) => data,
            data => serde_json::to_string(&data)
                .map_err(|error| TriggerEventError::Data(error.to_string()))?,
        };
        if data.len() > MAX_EVENT_DATA_BYTES {
            return Err(TriggerEventError::DataTooLarge);
        }
        Ok(PublishedEvent {
            name: self.name,
            data,
            channels,
            socket_id: self.socket_id,
        })
    }
}

#[derive(Serialize)]
pub struct ServerMessage<'a> {
    pub event: &'a str,
    pub data: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channel: Option<&'a str>,
}

impl<'a> ServerMessage<'a> {
    pub fn connection_established(
        socket_id: &str
    ) -> Result<String, serde_json::Error> {
        let data = serde_json::to_string(&serde_json::json!({
            "socket_id": socket_id,
            "activity_timeout": 120
        }))?;
        serde_json::to_string(&Self {
            event: "pusher:connection_established",
            data,
            channel: None,
        })
    }

    pub fn pong() -> Result<String, serde_json::Error> {
        serde_json::to_string(&Self {
            event: "pusher:pong",
            data: "{}".to_owned(),
            channel: None,
        })
    }

    pub fn subscription_succeeded(
        channel: &'a str
    ) -> Result<String, serde_json::Error> {
        serde_json::to_string(&Self {
            event: "pusher_internal:subscription_succeeded",
            data: "{}".to_owned(),
            channel: Some(channel),
        })
    }

    pub fn presence_subscription_succeeded(
        channel: &'a str,
        presence: PresenceSnapshot,
    ) -> Result<String, serde_json::Error> {
        let data = serde_json::to_string(
            &serde_json::json!({ "presence": presence }),
        )?;
        serde_json::to_string(&Self {
            event: "pusher_internal:subscription_succeeded",
            data,
            channel: Some(channel),
        })
    }

    pub fn presence_member_added(
        channel: &'a str,
        member: &PresenceMember,
    ) -> Result<String, serde_json::Error> {
        let data = serde_json::to_string(member)?;
        serde_json::to_string(&Self {
            event: "pusher_internal:member_added",
            data,
            channel: Some(channel),
        })
    }

    pub fn presence_member_removed(
        channel: &'a str,
        user_id: &str,
    ) -> Result<String, serde_json::Error> {
        let data =
            serde_json::to_string(&serde_json::json!({ "user_id": user_id }))?;
        serde_json::to_string(&Self {
            event: "pusher_internal:member_removed",
            data,
            channel: Some(channel),
        })
    }

    pub fn subscription_error(
        channel: &'a str,
        message: &str,
    ) -> Result<String, serde_json::Error> {
        let data = serde_json::to_string(&serde_json::json!({
            "type": "AuthError",
            "error": message,
            "status": 401
        }))?;
        serde_json::to_string(&Self {
            event: "pusher:subscription_error",
            data,
            channel: Some(channel),
        })
    }

    pub fn channel_event(
        event: &'a str,
        channel: &'a str,
        data: String,
    ) -> Result<String, serde_json::Error> {
        serde_json::to_string(&Self { event, data, channel: Some(channel) })
    }
}

pub fn subscription(message: &ClientMessage) -> Option<SubscriptionRequest> {
    if message.event != "pusher:subscribe" {
        return None;
    }
    let value = match &message.data {
        serde_json::Value::String(json) => serde_json::from_str(json).ok()?,
        value => value.clone(),
    };
    serde_json::from_value(value).ok()
}

/// Decode a leave request from either Pusher data representation.
pub fn unsubscribe(message: &ClientMessage) -> Option<UnsubscribeRequest> {
    if message.event != "pusher:unsubscribe" {
        return None;
    }
    let value = match &message.data {
        serde_json::Value::String(json) => serde_json::from_str(json).ok()?,
        value => value.clone(),
    };
    serde_json::from_value(value).ok()
}

#[cfg(test)]
#[path = "../tests/protocol.rs"]
mod tests;
