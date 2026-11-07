//! The C shapes of `include/claviron_module.h`, field for field. Nothing else
//! lives here: no Tokio, no HTTP.

use std::ffi::c_void;

pub const ABI_VERSION: u32 = 1;

pub const OK: u32 = 0;
pub const INVALID_ARGUMENT: u32 = 2;
pub const CLOSED: u32 = 3;
pub const BUSY: u32 = 4;
pub const UNSUPPORTED: u32 = 5;
pub const PANIC: u32 = 6;

pub const LOG_ERROR: u32 = 1;
pub const LOG_WARN: u32 = 2;
pub const LOG_INFO: u32 = 3;

pub const FIELD_METHOD: u32 = 1;
pub const FIELD_SCHEME: u32 = 2;
pub const FIELD_PATH: u32 = 3;
pub const FIELD_QUERY: u32 = 4;
pub const FIELD_PEER: u32 = 5;
pub const FIELD_LOCAL: u32 = 6;
pub const FIELD_VERSION: u32 = 7;
pub const FIELD_HEADER_NAME: u32 = 8;
pub const FIELD_HEADER_VALUE: u32 = 9;

pub const MESSAGE_TEXT: u32 = 1;
pub const MESSAGE_BINARY: u32 = 2;
pub const MESSAGE_CLOSE: u32 = 3;

pub const ABI_VERSION_SYMBOL: &[u8] = b"claviron_module_abi_version\0";
pub const REGISTER_SYMBOL: &[u8] = b"claviron_module_register\0";

pub type AbiVersionFn = unsafe extern "C" fn() -> u32;
pub type RegisterFn = unsafe extern "C" fn(*const Host, *mut Registrar) -> u32;

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct Slice {
    pub data: *const u8,
    pub length: u64,
}

impl Slice {
    pub fn of(bytes: &[u8]) -> Self {
        Self { data: bytes.as_ptr(), length: bytes.len() as u64 }
    }

    /// # Safety
    /// `data` must point at `length` readable bytes for the returned lifetime.
    pub unsafe fn bytes<'a>(self) -> &'a [u8] {
        if self.data.is_null() || self.length == 0 {
            return &[];
        }
        unsafe { std::slice::from_raw_parts(self.data, self.length as usize) }
    }
}

/// Opaque to a module; see `registrar::Registration`.
pub struct Registrar {
    _private: [u8; 0],
}

/// Opaque to a module; see `service::Cancel`.
pub struct Cancel {
    _private: [u8; 0],
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct BootInfo {
    pub struct_size: u32,
    pub config_dir: Slice,
    pub environment: Slice,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct Lifecycle {
    pub struct_size: u32,
    pub context: *mut c_void,
    pub boot: Option<unsafe extern "C" fn(*const BootInfo, *mut c_void) -> u32>,
    pub reload: Option<unsafe extern "C" fn(*mut c_void) -> u32>,
    pub finalize: Option<unsafe extern "C" fn(*mut c_void)>,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct Service {
    pub struct_size: u32,
    pub context: *mut c_void,
    pub run: Option<unsafe extern "C" fn(*mut Cancel, *mut c_void) -> u32>,
}

/// Opaque to a module; see `request::Exchange`.
pub struct Request {
    _private: [u8; 0],
}

pub type RequestFn = unsafe extern "C" fn(*mut Request, *mut c_void) -> u32;
pub type BodyFn =
    unsafe extern "C" fn(*mut Request, *const u8, u64, u32, *mut c_void) -> u32;
pub type ClosedFn = unsafe extern "C" fn(*mut Request, u32, *mut c_void);

#[repr(C)]
#[derive(Clone, Copy)]
pub struct HttpHandler {
    pub struct_size: u32,
    pub request: Option<RequestFn>,
    pub body: Option<BodyFn>,
    pub writable: Option<RequestFn>,
    pub closed: Option<ClosedFn>,
}

pub type MessageFn = unsafe extern "C" fn(
    *mut Request,
    u32,
    u32,
    *const u8,
    u64,
    *mut c_void,
) -> u32;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct SessionHandler {
    pub struct_size: u32,
    pub open: Option<RequestFn>,
    pub message: Option<MessageFn>,
    pub writable: Option<RequestFn>,
    pub closed: Option<ClosedFn>,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct Application {
    pub struct_size: u32,
    pub context: *mut c_void,
    pub start: Option<unsafe extern "C" fn(*mut c_void) -> u32>,
    pub stop: Option<unsafe extern "C" fn(*mut c_void) -> u32>,
    pub http: *const HttpHandler,
    pub websocket: *const SessionHandler,
}

/// Opaque to a module; see `handlers::Call`.
pub struct Call {
    _private: [u8; 0],
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct Operation {
    pub struct_size: u32,
    pub context: *mut c_void,
    pub call:
        Option<unsafe extern "C" fn(*mut Call, Slice, *mut c_void) -> u32>,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct Subscription {
    pub struct_size: u32,
    pub context: *mut c_void,
    pub event: Option<unsafe extern "C" fn(Slice, Slice, *mut c_void)>,
}

pub type InvokeCallback = unsafe extern "C" fn(u32, Slice, *mut c_void);

#[repr(C)]
#[derive(Clone, Copy)]
pub struct RequestPolicy {
    pub struct_size: u32,
    pub max_request_body: u64,
    pub max_response_body: u64,
}

#[repr(C)]
pub struct Host {
    pub struct_size: u32,
    pub abi_version: u32,
    pub register_lifecycle:
        unsafe extern "C" fn(*mut Registrar, *const Lifecycle) -> u32,
    pub register_service:
        unsafe extern "C" fn(*mut Registrar, Slice, *const Service) -> u32,
    pub register_application:
        unsafe extern "C" fn(*mut Registrar, Slice, *const Application) -> u32,
    pub register_operation:
        unsafe extern "C" fn(*mut Registrar, Slice, *const Operation) -> u32,
    pub register_subscription:
        unsafe extern "C" fn(*mut Registrar, Slice, *const Subscription) -> u32,
    pub log: unsafe extern "C" fn(u32, Slice),
    pub cancelled: unsafe extern "C" fn(*const Cancel) -> u32,
    pub cancel_wait: unsafe extern "C" fn(*const Cancel) -> u32,
    pub cancel_notify: unsafe extern "C" fn(
        *mut Cancel,
        Option<unsafe extern "C" fn(*mut c_void)>,
        *mut c_void,
    ) -> u32,
    pub request_field:
        unsafe extern "C" fn(*const Request, u32, u64, *mut Slice) -> u32,
    pub request_header_count: unsafe extern "C" fn(*const Request) -> u64,
    pub request_policy:
        unsafe extern "C" fn(*const Request, *mut RequestPolicy) -> u32,
    pub request_set_data:
        unsafe extern "C" fn(*mut Request, *mut c_void) -> u32,
    pub request_data: unsafe extern "C" fn(*const Request) -> *mut c_void,
    pub request_read_body: unsafe extern "C" fn(*mut Request) -> u32,
    pub response_begin: unsafe extern "C" fn(*mut Request, u32) -> u32,
    pub response_header:
        unsafe extern "C" fn(*mut Request, Slice, Slice) -> u32,
    pub response_write: unsafe extern "C" fn(*mut Request, Slice) -> u32,
    pub response_finish: unsafe extern "C" fn(*mut Request) -> u32,
    pub session_accept: unsafe extern "C" fn(*mut Request, u64) -> u32,
    pub session_send:
        unsafe extern "C" fn(*mut Request, u32, u32, Slice) -> u32,
    pub call_reply: unsafe extern "C" fn(*mut Call, Slice) -> u32,
    pub call_fail: unsafe extern "C" fn(*mut Call, Slice) -> u32,
    pub publish_event: unsafe extern "C" fn(Slice, Slice) -> u32,
    pub invoke: unsafe extern "C" fn(
        Slice,
        Slice,
        Option<InvokeCallback>,
        *mut c_void,
    ) -> u32,
}
