//! Stable hosting persistence API.
//!
//! SQLx is an implementation detail of [`HostingStore`]. Runtime and
//! downstream consumers exchange plain domain values and [`HostingError`],
//! so storage changes stay inside this crate.

mod error;
mod event;
#[cfg(any(feature = "postgres", feature = "sqlite"))]
mod store;
mod types;

// Hosting data needs somewhere to live, and there is no default to fall back on.
#[cfg(all(
    feature = "server",
    not(any(feature = "postgres", feature = "sqlite"))
))]
compile_error!("claviron-hosting needs a store: enable `postgres` or `sqlite`");

pub use claviron_types::CertificateKind;
pub use error::{HostingError, Result};
pub use event::HostingChanged;
#[cfg(any(feature = "postgres", feature = "sqlite"))]
pub use store::HostingStore;
pub use types::{
    Certificate, CertificateAlias, CertificateIssuer, HostedCertificate,
    HostedVhost, IssuerKind, NewCertificate, PublicCertificate, Server, Vhost,
    VhostAlias, VhostImport,
};

#[cfg(all(feature = "server", any(feature = "postgres", feature = "sqlite")))]
mod service;
#[cfg(all(
    feature = "server",
    any(feature = "postgres", feature = "sqlite")
))]
pub use service::{HostingService, register};
