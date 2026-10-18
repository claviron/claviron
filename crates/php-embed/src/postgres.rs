//! The request's PostgreSQL connection, for Rust called from PHP.
//!
//! PHP running inside Claviron borrows a connection the runtime already owns,
//! held for the length of the request. This module lets Rust borrow the same
//! one.
//!
//! That is the point of it. Critical work — anything where a wrong answer is a
//! financial, consistency, concurrency or audit bug — belongs in Rust and is
//! called from PHP, and for the two halves to be one unit of work they have to
//! be one database session. A host function invoked from PHP runs on the PHP
//! thread, which is where the lease lives, so a transaction opened in PHP
//! encloses the Rust writes and a transaction opened in Rust encloses PHP's.
//! Drawing a separate pooled connection instead would commit the Rust half
//! outside whatever PHP had open, and no amount of care on either side would
//! make that atomic.
//!
//! The same guarantees apply to a borrow taken here as to PHP's own statements:
//! a transaction cannot outlive the request, and nothing left on the connection
//! reaches the next one. See
//! `functions/postgres/session.rs` for how the scope is opened and closed.

pub use crate::functions::postgres::{SessionError, with_request_connection};
