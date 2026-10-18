//! Opt-in, one-way auth lifecycle translation into the application event bus.

use claviron_app::{
    ApplicationEvent, ApplicationEventError, EventArrayKey, EventValue,
};
use claviron_auth::events::AuthEvent;
use claviron_runtime::{AppState, registry};

/// Protocol topic shared with PHP's AuthEvent decoder, not an operator setting.
pub const TOPIC: &str = "claviron.auth.event.v1";

pub fn application_event(
    event: &AuthEvent
) -> Result<ApplicationEvent, ApplicationEventError> {
    let (kind, subject, guard, remember) = match event {
        AuthEvent::Login { guard, subject, remember, .. } => {
            ("login", subject, Some(guard), Some(*remember))
        }
        AuthEvent::Logout { guard, subject, .. } => {
            ("logout", subject, Some(guard), None)
        }
        AuthEvent::Registered { subject } => {
            ("registered", subject, None, None)
        }
    };
    let text = |name: &str, value: &str| {
        (
            EventArrayKey::String(name.into()),
            EventValue::String(value.as_bytes().to_vec()),
        )
    };
    let mut fields = vec![
        (EventArrayKey::String("version".into()), EventValue::Int(1)),
        text("kind", kind),
        text("subject", subject),
    ];
    if let Some(guard) = guard {
        fields.push(text("guard", guard));
    }
    if let Some(remember) = remember {
        fields.push((
            EventArrayKey::String("remember".into()),
            EventValue::Bool(remember),
        ));
    }
    if let AuthEvent::Logout { username: Some(username), .. } = event {
        fields.push(text("username", username));
    }
    if let AuthEvent::Login { context: Some(context), .. } = event {
        fields.push(text("username", &context.username));
        fields.push(text("name", &context.name));
        let occurred_at = context
            .occurred_at
            .to_offset(time::UtcOffset::UTC)
            .format(&time::format_description::well_known::Rfc3339)
            .map_err(|error| {
                ApplicationEventError::InvalidPayload(error.to_string())
            })?;
        fields.push(text("occurred_at", &occurred_at));
        fields.push((
            EventArrayKey::String("ip".into()),
            context.ip.map_or(EventValue::Null, |ip| {
                EventValue::String(ip.to_string().into_bytes())
            }),
        ));
        fields.push((
            EventArrayKey::String("identity".into()),
            context.identity.map_or(EventValue::Null, |identity| {
                EventValue::String(identity.to_string().into_bytes())
            }),
        ));
    }
    ApplicationEvent::new(TOPIC, EventValue::Array(fields))
}

/// Register once. The host also registers ApplicationEvent's fleet transport
/// and one PHP task subscribes to this topic; there is no reverse auth relay.
pub fn register(state: &AppState) -> registry::Result<()> {
    state.events().listen::<AuthEvent, _>(|state, event| async move {
        let event = application_event(&event)
            .map_err(|error| registry::Error::msg(error.to_string()))?;
        state.events().emit(event);
        Ok(())
    })
}

#[cfg(test)]
#[path = "../tests/auth.rs"]
mod tests;
