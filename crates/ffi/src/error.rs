use http::StatusCode;
use thiserror::Error;

use crate::ForeignResponse;

pub type ForeignResult<T = ()> = Result<T, ForeignError>;

#[derive(Debug, Error)]
pub enum ForeignError {
    #[error("failed loading foreign library `{path}`: {detail}")]
    Load { path: String, detail: String },
    #[error("foreign symbol `{name}` is unavailable in `{path}`: {detail}")]
    Symbol { path: String, name: String, detail: String },
    #[error("foreign symbol `{name}` returned code={code}: {detail}")]
    Call { name: String, code: i32, detail: String },
    #[error("foreign response is invalid: {detail}")]
    InvalidResponse { detail: String },
}

impl ForeignError {
    pub fn status_code(&self) -> StatusCode {
        match self {
            Self::Load { .. } | Self::Symbol { .. } => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
            Self::Call { .. } | Self::InvalidResponse { .. } => {
                StatusCode::BAD_GATEWAY
            }
        }
    }

    pub fn load(
        path: impl Into<String>,
        detail: impl Into<String>,
    ) -> Self {
        Self::Load { path: path.into(), detail: detail.into() }
    }

    pub fn symbol(
        path: impl Into<String>,
        name: impl Into<String>,
        detail: impl Into<String>,
    ) -> Self {
        Self::Symbol {
            path: path.into(),
            name: name.into(),
            detail: detail.into(),
        }
    }

    pub fn call(
        name: impl Into<String>,
        code: i32,
        detail: impl Into<String>,
    ) -> Self {
        Self::Call { name: name.into(), code, detail: detail.into() }
    }

    pub fn invalid_response(detail: impl Into<String>) -> Self {
        Self::InvalidResponse { detail: detail.into() }
    }

    pub fn into_foreign_response(self) -> ForeignResponse {
        ForeignResponse::text(self.to_string()).with_status(self.status_code())
    }
}

impl From<ForeignError> for ForeignResponse {
    fn from(error: ForeignError) -> Self {
        error.into_foreign_response()
    }
}

#[cfg(feature = "axum")]
impl axum::response::IntoResponse for ForeignError {
    fn into_response(self) -> axum::response::Response {
        self.into_foreign_response().into_response()
    }
}
