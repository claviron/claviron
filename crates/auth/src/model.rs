//! Internal management data shared by `users` and `authorization`.
//!
//! Records, CRUD inputs, forms, revisions, pages and errors live here so both
//! manager contracts use the same types. These are administrative projections,
//! not database models or authenticated user/session state. This module owns no
//! provider, storage, authorization decision or transport handler.
//! Consumers obtain these types through the selected public management module;
//! enabling either feature makes this shared implementation available.
mod error;
mod record;
mod request;
mod schema;
pub use error::{ManagementError, Result, Violation};
pub use record::{Document, Page, Record, RecordKey, Resource};
pub use request::{Create, Delete, List, Update};
pub use schema::{Form, Operation, Schema};
