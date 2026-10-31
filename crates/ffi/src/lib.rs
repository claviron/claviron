//! Foreign handler primitives for Claviron routes.
//!
//! This crate does not add a new vhost `RustRouter` and does not own routing.
//! A normal Rust route handler can use these types to call a foreign
//! implementation and turn the result back into a Claviron/Axum response.
//!
//! The intended flow is deliberately small:
//!
//! ```text
//! Claviron route -> Rust binding -> foreign handler -> ForeignResponse
//! ```

pub mod abi;
pub mod error;
pub mod handler;
pub mod headers;
pub mod response;

pub use abi::{
    CLAVIRON_FOREIGN_ABI_VERSION, FOREIGN_ABI_VERSION_SYMBOL,
    FOREIGN_FREE_RESPONSE_SYMBOL, FOREIGN_FREE_STREAM_CHUNK_SYMBOL,
    FOREIGN_FREE_STREAM_RESPONSE_SYMBOL, FOREIGN_FREE_STREAM_SYMBOL,
    FOREIGN_HANDLE_STREAM_SYMBOL, FOREIGN_HANDLE_SYMBOL,
    FOREIGN_STREAM_NEXT_SYMBOL, ForeignAbiVersionFn, ForeignFreeResponseFn,
    ForeignFreeStreamChunkFn, ForeignFreeStreamFn, ForeignFreeStreamResponseFn,
    ForeignHandleFn, ForeignHandleStreamFn, ForeignReturnCode,
    ForeignStreamChunkAbi, ForeignStreamChunkOwner, ForeignStreamNextCode,
    ForeignStreamNextFn, ForeignStreamResponseAbi,
};
pub use error::{ForeignError, ForeignResult};
pub use handler::{ForeignCall, ForeignHandler};
pub use headers::{ForeignHeaderAbi, ForeignHeaderBlock, ForeignHeaderSlice};
pub use response::ForeignResponse;

#[cfg(test)]
#[path = "../tests/lib.rs"]
mod tests;
