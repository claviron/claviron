//! Application-owned user, role, and permission management contracts.
//!
//! Enable `user-management` to use this module. A host supplies a shared
//! [`UserManager`] with its own persistence, editable schemas, validation,
//! authorization, and management scope. This module registers nothing and
//! does not require HTTP, a server runtime, or a database.
//!
//! The actor is a trusted, already-authenticated caller supplied by the host,
//! never an identity deserialized from a management request. The managed record
//! is a separate projection and need not implement [`crate::Authenticated`].
//! Every operation must enforce both caller authorization and the provider's
//! management scope, including direct lookups, creation, deletion, and
//! relationship changes. Discovery capabilities and UI hints are not grants.
//!
//! Schemas describe exposed input/output fields, never database columns by
//! reflection. Providers reject unknown or noneditable inputs, do not return
//! stored credentials, and validate again when committing a mutation. Record
//! and schema revisions are opaque concurrency tokens, not timestamps or
//! client-selected scope. No callback, SQL predicate, or executable UI is sent.

mod error;
mod provider;
mod record;
mod request;
mod schema;

pub use error::{ManagementError, Result, Violation};
pub use provider::UserManager;
pub use record::{
    AssignmentKey, Assignments, Document, Page, Record, RecordKey, Relation,
    Resource,
};
pub use request::{Create, Delete, List, ReplaceAssignments, Update};
pub use schema::{Form, Operation, Schema};

#[cfg(test)]
#[path = "../tests/management.rs"]
mod tests;
