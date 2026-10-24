use serde_json::{Map, Value};

/// A JSON object with application-defined field names. Large numeric IDs and
/// exact decimals should use strings when consumed by JavaScript clients.
pub type Document = Map<String, Value>;

/// Management resources, not database tables or authenticated principal types.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Resource {
    #[cfg(feature = "users")]
    User,
    #[cfg(feature = "authorization")]
    Role,
    #[cfg(feature = "authorization")]
    Permission,
}

/// Provider-defined public reference. The reference must be nonempty and is
/// opaque to consumers; it need not expose a local numeric primary key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordKey {
    pub resource: Resource,
    pub reference: String,
}

/// An administrative projection, not an ORM entity or authenticated session.
/// Revision is nonempty and changes whenever a mutation would conflict.
/// Payloads deliberately do not implement Debug: editable data may be sensitive.
#[derive(Clone, PartialEq)]
pub struct Record {
    pub key: RecordKey,
    pub revision: String,
    pub data: Document,
}

/// A bounded page. Cursors are opaque, scoped to the provider, query, and actor;
/// they do not bypass scope or authorization checks on the next request.
#[derive(Clone, PartialEq)]
pub struct Page {
    pub records: Vec<Record>,
    pub next_cursor: Option<String>,
}
