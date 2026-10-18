#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("an existing authenticated session is required")]
    SessionRequired,
    #[error("message stream error: {0}")]
    Stream(#[from] std::io::Error),
    #[error("WebSocket error: {0}")]
    WebSocket(#[from] fastwebsockets::WebSocketError),
    #[error("invalid Pusher JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("invalid Pusher event: {0}")]
    Event(#[from] claviron_pusher::TriggerEventError),
    #[error("invalid Pusher presence member: {0}")]
    Presence(#[from] claviron_pusher::PresenceMemberError),
    #[error("Broadcast fleet transport error: {0}")]
    Fleet(String),
    #[error("Broadcast service is not running")]
    Unavailable,
    #[error("Broadcast application key is not registered")]
    UnknownApplication,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Why a session was not admitted — the same answer whichever door the client
/// came through; each door turns it into its own refusal.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum SessionRefusal {
    #[error("the broadcast application is not configured")]
    UnknownApplication,
    #[error("the broadcast service is not running")]
    NotRunning,
    #[error("the broadcast service is stopping")]
    Stopping,
}

impl SessionRefusal {
    /// The HTTP status a door answers with: an unknown application is not
    /// found, the others are the service's own state.
    pub fn status(self) -> http::StatusCode {
        match self {
            Self::UnknownApplication => http::StatusCode::NOT_FOUND,
            Self::NotRunning | Self::Stopping => {
                http::StatusCode::SERVICE_UNAVAILABLE
            }
        }
    }
}
