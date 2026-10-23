/// Safe validation feedback supplied by the downstream implementation.
/// `path` is a JSON Pointer into the submitted document; empty denotes the
/// whole document. Messages/codes must not contain submitted secrets or SQL.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Violation {
    pub path: String,
    pub code: String,
    pub message: String,
}

/// Transport-independent management failures. Infrastructure diagnostics stay
/// in the host; no raw database/framework error crosses this boundary.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ManagementError {
    #[error("management operation is not supported")]
    Unsupported,
    #[error("management operation is forbidden")]
    Forbidden,
    #[error("managed record was not found")]
    NotFound,
    #[error("managed record or assignment revision changed")]
    Conflict,
    #[error("management schema changed")]
    SchemaChanged,
    #[error("management input failed validation")]
    Validation(Vec<Violation>),
    #[error("management provider is unavailable")]
    Unavailable,
}

pub type Result<T> = std::result::Result<T, ManagementError>;
