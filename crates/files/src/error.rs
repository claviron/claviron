use http::StatusCode;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileServeError {
    NotFound,
    Forbidden,
    DynamicFile,
    Internal,
}

impl FileServeError {
    pub fn status_code(self) -> StatusCode {
        match self {
            FileServeError::NotFound => StatusCode::NOT_FOUND,
            FileServeError::Forbidden => StatusCode::FORBIDDEN,
            FileServeError::DynamicFile => StatusCode::FORBIDDEN,
            FileServeError::Internal => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}
