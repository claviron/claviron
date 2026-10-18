//! Owns the H2 request leg, multiplexed connections and stream permits.

mod body;
pub(crate) mod conn;
pub(crate) mod permit;
pub(crate) mod pool;

pub(crate) use body::H2PooledResponseBody;

pub(crate) use crate::proxy::h2::pool::H2AcquiredStream;

mod handler;
mod tunnel;

mod executor;
