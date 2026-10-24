//! Shared transport-neutral documents for the independently selected managers.
mod error;
mod record;
mod request;
mod schema;
pub use error::{ManagementError, Result, Violation};
pub use record::{Document, Page, Record, RecordKey, Resource};
pub use request::{Create, Delete, List, Update};
pub use schema::{Form, Operation, Schema};
