//! The C shapes of `include/claviron_module.h`, field for field. Nothing else
//! lives here: no Tokio, no HTTP.

use std::ffi::c_void;

pub const ABI_VERSION: u32 = 1;

pub const OK: u32 = 0;
pub const INVALID_ARGUMENT: u32 = 2;
pub const PANIC: u32 = 6;

pub const LOG_ERROR: u32 = 1;
pub const LOG_WARN: u32 = 2;
pub const LOG_INFO: u32 = 3;

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

#[repr(C)]
pub struct Host {
    pub struct_size: u32,
    pub abi_version: u32,
    pub register_lifecycle:
        unsafe extern "C" fn(*mut Registrar, *const Lifecycle) -> u32,
    pub register_service:
        unsafe extern "C" fn(*mut Registrar, Slice, *const Service) -> u32,
    pub log: unsafe extern "C" fn(u32, Slice),
    pub cancelled: unsafe extern "C" fn(*const Cancel) -> u32,
    pub cancel_wait: unsafe extern "C" fn(*const Cancel) -> u32,
    pub cancel_notify: unsafe extern "C" fn(
        *mut Cancel,
        Option<unsafe extern "C" fn(*mut c_void)>,
        *mut c_void,
    ) -> u32,
}
