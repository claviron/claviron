use super::{Document, Resource};

/// Discoverable operations, not a cached authorization decision.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Operation {
    List,
    Read,
    Create,
    Update,
    Delete,
    ReadAssignments,
    ReplaceAssignments,
}

/// An application-supplied JSON Schema 2020-12 object and optional declarative
/// UI description. The schema explicitly declares its dialect with `$schema`.
/// UI metadata uses JSON Forms UI Schema; no renderer is required here.
/// Consumers implement an explicit supported subset and reject unsupported forms,
/// and never fetch arbitrary remote references or execute supplied code.
/// Do not include credentials or secrets in schema defaults/examples.
#[derive(Clone, PartialEq)]
pub struct Form {
    pub schema: Document,
    pub ui: Option<Document>,
}

/// The provider's current management view for this caller and resource.
/// There is no required email, username, birth date, type, or admin flag.
/// Create/update forms are independent of the read-only output projection.
/// A nonempty revision identifies this complete description, including UI and
/// capabilities, within the provider/actor context. Clients cache per server
/// connection and provider, not globally across servers or authenticated actors.
#[derive(Clone, PartialEq)]
pub struct Schema {
    pub resource: Resource,
    pub revision: String,
    pub record: Document,
    pub create: Option<Form>,
    pub update: Option<Form>,
    pub operations: Vec<Operation>,
}
