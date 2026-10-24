//! Application-owned role/permission CRUD and direct assignment management.
//!
//! This module owns [`RolePermissionManager`] and its assignment types. It manages
//! authorization data; it is not the API for checking an individual request's
//! permission. User profile CRUD belongs to `users`. Shared record, request and
//! form types come from the internal `model` module, without duplicate types.
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
pub use provider::RolePermissionManager;
pub use record::{
    AssignmentKey, AssignmentSchema, Assignments, Owner, Relation,
};
pub use request::ReplaceAssignments;

#[cfg(test)]
#[path = "../tests/authorization.rs"]
mod tests;
