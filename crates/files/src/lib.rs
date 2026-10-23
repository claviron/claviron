mod adapter;
#[cfg(feature = "axum")]
mod bindings;
pub mod caching;
pub mod config;
mod error;
pub mod mime;
mod response;
#[cfg(feature = "axum")]
pub mod routing;
mod service;
mod site;
mod stream;

pub use adapter::{Handler, ResourceAdapter, SharedHandler};
#[cfg(feature = "axum")]
pub use bindings::{
    FileHandlerState, axum_file_handler, axum_selected_static_mount_handler,
    axum_static_mount_handler,
};
pub use caching::reload::{FileCacheProvider, register};
pub use service::{
    StaticMount, try_files_for_site, try_files_probe, try_handle_files,
};
pub use site::FileSite;
