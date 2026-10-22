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

pub use claviron_types::CertificateKind;
pub use error::{HostingError, Result};
pub use event::HostingChanged;
#[cfg(any(feature = "postgres", feature = "sqlite"))]
pub use store::HostingStore;
pub use types::{
    Certificate, CertificateAlias, CertificateIssuer, HostedCertificate,
    HostedVhost, IssuerKind, NewCertificate, Server, Vhost, VhostAlias,
    VhostImport,
};

#[cfg(all(feature = "server", any(feature = "postgres", feature = "sqlite")))]
mod service;
#[cfg(all(
    feature = "server",
    any(feature = "postgres", feature = "sqlite")
))]
pub use service::{HostingService, register};
