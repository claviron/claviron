//! Owns the H1 request leg, exclusive pool leases and response-body lifetime.

mod body;
pub(crate) mod conn;
mod lease;
pub(crate) mod pool;

pub(crate) use body::PooledResponseBody;

mod handler;
mod tunnel;
