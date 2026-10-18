//! ApplicationEvent adapter for the same typed change delivered to local hooks.

use claviron_app::{ApplicationEvent, EventArrayKey, EventValue};
use claviron_runtime::AppState;
use tracing::{trace, warn};

use super::{OccupancyChange, OccupancyKind};

pub(super) fn emit(
    state: &AppState,
    change: &OccupancyChange,
) {
    let topic = match change.kind {
        OccupancyKind::Occupied => "channel_occupied",
        OccupancyKind::Vacated => "channel_vacated",
    };
    match ApplicationEvent::new(topic, transition_payload(change)) {
        Ok(event) => {
            state.events().emit(event);
            trace!(topic, app_id = %change.app_id, channel = %change.channel, "Broadcast occupancy event emitted");
        }
        Err(error) => {
            warn!(topic, app_id = %change.app_id, channel = %change.channel, %error, "Broadcast occupancy event rejected");
        }
    }
}

pub(super) fn transition_payload(change: &OccupancyChange) -> EventValue {
    EventValue::Array(vec![
        field("app_id", EventValue::String(change.app_id.as_bytes().to_vec())),
        field(
            "channel",
            EventValue::String(change.channel.as_bytes().to_vec()),
        ),
        field("time", EventValue::String(change.time.as_bytes().to_vec())),
        field(
            "user_id",
            change.user_id.as_ref().map_or(EventValue::Null, |id| {
                EventValue::String(id.as_bytes().to_vec())
            }),
        ),
        field(
            "user_info",
            change.user_info.as_ref().map_or(EventValue::Null, json_value),
        ),
    ])
}

pub(super) fn field(
    name: &str,
    value: EventValue,
) -> (EventArrayKey, EventValue) {
    (EventArrayKey::String(name.to_owned()), value)
}

fn json_value(value: &serde_json::Value) -> EventValue {
    match value {
        serde_json::Value::Null => EventValue::Null,
        serde_json::Value::Bool(value) => EventValue::Bool(*value),
        serde_json::Value::Number(value) => value
            .as_i64()
            .map(EventValue::Int)
            .or_else(|| value.as_f64().map(EventValue::Float))
            .unwrap_or(EventValue::Null),
        serde_json::Value::String(value) => {
            EventValue::String(value.as_bytes().to_vec())
        }
        serde_json::Value::Array(values) => EventValue::Array(
            values
                .iter()
                .enumerate()
                .map(|(index, value)| {
                    (EventArrayKey::Int(index as i64), json_value(value))
                })
                .collect(),
        ),
        serde_json::Value::Object(values) => EventValue::Array(
            values
                .iter()
                .map(|(key, value)| {
                    (EventArrayKey::String(key.clone()), json_value(value))
                })
                .collect(),
        ),
    }
}
