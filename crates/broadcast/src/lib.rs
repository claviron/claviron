mod admission;
mod channels;
mod config;
mod error;
mod fleet;
pub mod groups;
pub mod hooks;
mod http;
mod service;
mod session;
pub mod stream;
mod upgrade;

pub use admission::SessionAuthenticator;
pub use config::{
    BroadcastApplicationConfig, BroadcastConfig, BroadcastConfigError,
};
pub use error::{Error, Result, SessionRefusal};
pub use http::pusher_routes;
pub use service::{
    BroadcastService, GroupLayer, register, register_with_config,
};
pub use session::BroadcastSession;
pub use upgrade::BroadcastUpgradeHandler;

/// Wire compatibility, independent of the broadcast service's transport.
pub mod protocol {
    pub use claviron_pusher as pusher;
}
