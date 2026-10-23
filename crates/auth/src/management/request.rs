use std::num::NonZeroU32;

use super::{AssignmentKey, Document, RecordKey, Resource};

/// The host bounds page size before invoking its provider. There is no
/// unbounded/default page size and no client-supplied management scope or SQL.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct List {
    pub resource: Resource,
    pub page_size: NonZeroU32,
    pub cursor: Option<String>,
    /// Optional application-defined search text, never an executable filter.
    pub query: Option<String>,
}

/// Validate against the selected create form and current application rules.
/// The provider, not the caller, allocates the returned record reference.
#[derive(Clone, PartialEq)]
pub struct Create {
    pub resource: Resource,
    pub schema_revision: String,
    pub data: Document,
}

/// Sparse top-level replacement, not recursive merge or JSON Patch.
/// Absent keys remain unchanged; present values replace the entire field,
/// including arrays/objects. Null is an explicit value, accepted only when
/// the downstream schema and business rules allow it. It does not delete a key.
/// The update form describes the editable projection, not necessarily this
/// sparse payload. Validate changed fields and the resulting candidate state;
/// a required unchanged field need not be resent by the client.
///
/// Both revisions must be nonempty and match at commit. Scope, authorization,
/// validation, and optimistic concurrency belong to the same mutation boundary.
#[derive(Clone, PartialEq)]
pub struct Update {
    pub key: RecordKey,
    pub revision: String,
    pub schema_revision: String,
    pub changes: Document,
}

/// Delete only the referenced revision within the provider's permitted scope.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Delete {
    pub key: RecordKey,
    pub revision: String,
}

/// Atomically replace a complete assignment set; an empty set clears it.
/// Every reference must be nonempty and unique. Do not silently omit hidden
/// assignments from a replaceable set: expose the complete editable set or
/// reject replacement. The provider checks every target and prevents privilege
/// escalation or changes to shared roles outside its management authority.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplaceAssignments {
    pub key: AssignmentKey,
    pub revision: String,
    pub members: Vec<String>,
}
