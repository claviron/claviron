//! Application-owned role/permission CRUD and direct assignment management.
//!
//! The `authorization` feature is independent of `users`. Opaque subject references
//! do not require a user CRUD provider or a particular user model. The application
//! supplies authentication, current authority, persistence and management scope.
//! Every write checks current authority and revisions atomically. Shared catalog
//! administration must not be inferred from permission to edit a subset of users.
mod provider;
mod record;
mod request;
pub use crate::model::{
    Create, Delete, Document, Form, List, ManagementError, Operation, Page,
    Record, RecordKey, Resource, Result, Schema, Update, Violation,
};
pub use provider::AuthorizationManager;
pub use record::{
    AssignmentKey, AssignmentSchema, Assignments, Owner, Relation,
};
pub use request::ReplaceAssignments;

#[cfg(test)]
#[path = "../tests/authorization.rs"]
mod tests;
