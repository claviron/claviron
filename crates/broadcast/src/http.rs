use std::sync::Arc;

use axum::body::Body;
use axum::extract::Extension;
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use claviron_pusher::{
    PusherAppKey, PusherAppSecret, TriggerEvent, TriggerEventError,
    authenticate,
};
use http::{Request, StatusCode};
use http_body_util::{BodyExt, Limited};
use tracing::debug;

use crate::BroadcastService;

const MAX_PUBLISH_BODY_BYTES: usize = 64 * 1024;

#[derive(Clone)]
struct PublishBinding {
    service: Arc<BroadcastService>,
    application: Arc<str>,
    key: PusherAppKey,
    secret: Option<PusherAppSecret>,
}

pub fn pusher_routes(service: Arc<BroadcastService>) -> Router {
    let mut router = Router::new();
    for (name, application) in service.applications().applications() {
        let path = format!("/apps/{}/events", application.id);
        let binding = PublishBinding {
            service: Arc::clone(&service),
            application: Arc::from(name.as_str()),
            key: application.key.clone(),
            secret: application.secret.clone(),
        };
        router = router
            .route(&path, post(publish).layer(Extension(Arc::new(binding))));
    }
    router
}

async fn publish(
    Extension(binding): Extension<Arc<PublishBinding>>,
    request: Request<Body>,
) -> Response {
    let Some(secret) = binding.secret.as_ref() else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "Pusher publishing is not configured",
        );
    };
    let (parts, body) = request.into_parts();
    let body = match Limited::new(body, MAX_PUBLISH_BODY_BYTES).collect().await
    {
        Ok(body) => body.to_bytes(),
        Err(_) => {
            return error(
                StatusCode::PAYLOAD_TOO_LARGE,
                "Pusher request body is too large",
            );
        }
    };
    if let Err(authentication) = authenticate(
        parts.method.as_str(),
        parts.uri.path(),
        parts.uri.query(),
        &body,
        &binding.key,
        secret,
    ) {
        debug!(application = %binding.application, error = %authentication, "Pusher publish authentication rejected");
        return error(StatusCode::UNAUTHORIZED, "Pusher authentication failed");
    }

    let event = match serde_json::from_slice::<TriggerEvent>(&body) {
        Ok(event) => match event.validate() {
            Ok(event) => event,
            Err(TriggerEventError::DataTooLarge) => {
                return error(
                    StatusCode::PAYLOAD_TOO_LARGE,
                    "Pusher event data is too large",
                );
            }
            Err(event) => {
                return error(StatusCode::BAD_REQUEST, event.to_string());
            }
        },
        Err(event) => {
            return error(
                StatusCode::BAD_REQUEST,
                format!("invalid Pusher event: {event}"),
            );
        }
    };

    match binding.service.publish(&binding.application, &event) {
        Ok(delivered) => {
            debug!(application = %binding.application, delivered, event = event.name, "Pusher event published locally");
            Json(serde_json::json!({})).into_response()
        }
        Err(crate::Error::Unavailable) => error(
            StatusCode::SERVICE_UNAVAILABLE,
            "Broadcast service is not running",
        ),
        Err(publish) => {
            error(StatusCode::INTERNAL_SERVER_ERROR, publish.to_string())
        }
    }
}

fn error(
    status: StatusCode,
    message: impl Into<String>,
) -> Response {
    (status, Json(serde_json::json!({ "error": message.into() })))
        .into_response()
}
