//! Shared DNS client capability, independent of transport and origin policy.
//!
//! Use `Resolver` for a custom implementation or enable `hickory` for the
//! built-in `HickoryResolver`. Query/answer/error types never expose Hickory.
mod config;
mod error;
#[cfg(feature = "hickory")]
mod hickory;

use std::future::Future;
use std::net::{Ipv4Addr, Ipv6Addr};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

pub use config::ResolverConfig;
pub use error::{ResolveError, ResolveErrorKind};
#[cfg(feature = "hickory")]
pub use hickory::{HickoryFactory, HickoryResolver};

/// Supported query types. Address-family/SRV expansion is consumer policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum RecordType {
    A,
    Aaaa,
    Srv,
    Txt,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Query<'a> {
    pub name: &'a str,
    pub record_type: RecordType,
}
impl<'a> Query<'a> {
    pub fn new(
        name: &'a str,
        record_type: RecordType,
    ) -> Self {
        Self { name, record_type }
    }
}

/// One DNS value; TXT string boundaries and arbitrary bytes are preserved.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum Record {
    A(Ipv4Addr),
    Aaaa(Ipv6Addr),
    Srv { priority: u16, weight: u16, port: u16, target: String },
    Txt(Vec<Vec<u8>>),
}

/// Remaining answer validity, not a fresh TTL on each cache read.
/// `valid_until` is process-local and must not be serialized into shared memory.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Answer {
    pub records: Vec<Record>,
    pub valid_until: Instant,
}

/// DNS is a cold control path; boxing enables custom object-safe implementations.
pub type ResolveFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Answer, ResolveError>> + Send + 'a>>;

/// A shared resolver capability. Implementations own caching and query work;
/// consumers own origin policy, refresh scheduling and total execution budgets.
pub trait Resolver: Send + Sync {
    fn lookup<'a>(
        &'a self,
        query: Query<'a>,
    ) -> ResolveFuture<'a>;
}

/// Opens one [`Resolver`] per consumer configuration.
///
/// The application decides which resolver its consumers use by binding one
/// implementation as `dyn ResolverFactory` in its registry; a consumer that
/// needs DNS resolves that binding and fails its boot when none exists. This
/// crate supplies no default: `HickoryFactory` is available behind the
/// `hickory` feature for an application that wants the built-in driver.
pub trait ResolverFactory: Send + Sync {
    fn open(
        &self,
        config: &ResolverConfig,
    ) -> Result<Arc<dyn Resolver>, ResolveError>;
}
