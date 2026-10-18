//! C-compatible request/response shapes.
//!
//! These types are intentionally plain. They describe the low-level ABI
//! contract a C, Go `c-shared`, Zig, or C++ library can implement. The safe
//! Rust route-facing API lives in `response` and `handler`.

use std::ffi::c_void;
use std::{ptr, slice};

use bytes::Bytes;

/// Current Claviron foreign handler ABI version.
pub const CLAVIRON_FOREIGN_ABI_VERSION: u32 = 1;

/// Canonical ABI version symbol exported by foreign libraries.
pub const FOREIGN_ABI_VERSION_SYMBOL: &[u8] = b"claviron_foreign_abi_version\0";

/// Canonical request handler symbol exported by foreign libraries.
pub const FOREIGN_HANDLE_SYMBOL: &[u8] = b"claviron_foreign_handle\0";

/// Canonical response-free symbol exported by foreign libraries.
pub const FOREIGN_FREE_RESPONSE_SYMBOL: &[u8] =
    b"claviron_foreign_free_response\0";

/// Canonical streaming request handler symbol exported by foreign libraries.
pub const FOREIGN_HANDLE_STREAM_SYMBOL: &[u8] =
    b"claviron_foreign_handle_stream\0";

/// Canonical streaming chunk-pull symbol exported by foreign libraries.
pub const FOREIGN_STREAM_NEXT_SYMBOL: &[u8] = b"claviron_foreign_stream_next\0";

/// Canonical streaming response metadata-free symbol exported by foreign libraries.
pub const FOREIGN_FREE_STREAM_RESPONSE_SYMBOL: &[u8] =
    b"claviron_foreign_free_stream_response\0";

/// Canonical streaming chunk-free symbol exported by foreign libraries.
pub const FOREIGN_FREE_STREAM_CHUNK_SYMBOL: &[u8] =
    b"claviron_foreign_free_stream_chunk\0";

/// Canonical stream handle-free symbol exported by foreign libraries.
pub const FOREIGN_FREE_STREAM_SYMBOL: &[u8] = b"claviron_foreign_free_stream\0";

/// C-compatible ABI return codes.
///
/// The native handler returns one of these values from
/// `claviron_foreign_handle`. Code `0` means the response struct is populated
/// and may still describe an HTTP error. Non-zero codes mean the call itself
/// failed before a trustworthy `ForeignResponseAbi` could be produced.
#[repr(i32)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ForeignReturnCode {
    Success = 0,
    InvalidRequest = 1,
    HandlerError = 2,
    InternalError = 3,
}

impl ForeignReturnCode {
    pub const fn as_i32(self) -> i32 {
        self as i32
    }

    pub const fn detail(code: i32) -> &'static str {
        match code {
            0 => "success",
            1 => "invalid request",
            2 => "handler error",
            3 => "internal error",
            _ => "unknown foreign return code",
        }
    }
}

/// C-compatible stream pull return codes.
///
/// `Chunk` means the chunk struct is populated and must later be released via
/// the foreign library's `claviron_foreign_free_stream_chunk` symbol. `End`
/// means the stream is complete and no chunk was produced.
#[repr(i32)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ForeignStreamNextCode {
    Chunk = 0,
    End = 1,
    HandlerError = 2,
    InternalError = 3,
}

impl ForeignStreamNextCode {
    pub const fn as_i32(self) -> i32 {
        self as i32
    }

    pub const fn detail(code: i32) -> &'static str {
        match code {
            0 => "chunk",
            1 => "end",
            2 => "handler error",
            3 => "internal error",
            _ => "unknown foreign stream return code",
        }
    }
}

/// C-compatible ABI version function.
pub type ForeignAbiVersionFn = unsafe extern "C" fn() -> u32;

/// Borrowed byte slice crossing the C ABI boundary.
///
/// The pointed-to memory is owned by the caller and is only valid for the
/// duration of the foreign call. The Rust `usize` fields in this ABI are
/// documented as C `size_t` fields in generated or hand-written C headers:
///
/// ```c
/// const uint8_t* ptr;
/// size_t len;
/// ```
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct ForeignSlice {
    pub ptr: *const u8,
    pub len: usize,
}

impl ForeignSlice {
    pub const fn empty() -> Self {
        Self { ptr: ptr::null(), len: 0 }
    }

    pub fn from_slice(bytes: &[u8]) -> Self {
        if bytes.is_empty() {
            Self::empty()
        } else {
            Self { ptr: bytes.as_ptr(), len: bytes.len() }
        }
    }

    /// View the borrowed ABI slice as Rust bytes.
    ///
    /// # Safety
    ///
    /// `ptr` must be valid for reads of `len` bytes for the returned
    /// lifetime. If `len` is zero, `ptr` may be null.
    pub unsafe fn as_bytes<'a>(&self) -> &'a [u8] {
        if self.len == 0 {
            &[]
        } else {
            assert!(
                !self.ptr.is_null(),
                "foreign slice pointer is null with non-zero length"
            );
            unsafe { slice::from_raw_parts(self.ptr, self.len) }
        }
    }
}

/// Owned byte buffer crossing the C ABI boundary.
///
/// The allocator that creates a buffer must also free it. When Rust returns a
/// `ForeignBuffer`, callers can release it with `claviron_ffi_free_buffer`.
/// The Rust `usize` fields map to C `size_t` in the ABI contract.
#[repr(C)]
#[derive(Debug)]
pub struct ForeignBuffer {
    pub ptr: *mut u8,
    pub len: usize,
    pub capacity: usize,
}

impl ForeignBuffer {
    pub const fn empty() -> Self {
        Self { ptr: ptr::null_mut(), len: 0, capacity: 0 }
    }

    pub fn from_vec(mut bytes: Vec<u8>) -> Self {
        if bytes.is_empty() {
            Self::empty()
        } else {
            let out = Self {
                ptr: bytes.as_mut_ptr(),
                len: bytes.len(),
                capacity: bytes.capacity(),
            };
            std::mem::forget(bytes);
            out
        }
    }

    /// Reclaim a Rust-owned ABI buffer.
    ///
    /// # Safety
    ///
    /// The buffer must have been created by `ForeignBuffer::from_vec` using
    /// this Rust allocator, and it must not have been reclaimed already.
    pub unsafe fn into_vec(self) -> Vec<u8> {
        if self.capacity == 0 {
            Vec::new()
        } else {
            assert!(
                !self.ptr.is_null(),
                "foreign buffer pointer is null with non-zero capacity"
            );
            unsafe { Vec::from_raw_parts(self.ptr, self.len, self.capacity) }
        }
    }
}

/// Release a buffer allocated by Rust for a foreign caller.
///
/// # Safety
///
/// `buffer` must have been allocated by `ForeignBuffer::from_vec` with this
/// Rust allocator and must not have been freed already. Passing a buffer
/// allocated by foreign code is undefined behavior.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn claviron_ffi_free_buffer(buffer: ForeignBuffer) {
    unsafe {
        drop(buffer.into_vec());
    }
}

/// Borrowed request data for a foreign HTTP handler.
///
/// Headers are JSON bytes by design. That keeps the first ABI contract small
/// and avoids exporting Rust's `HeaderMap` shape. The schema is a stable array
/// of pairs so duplicate headers can be represented:
///
/// ```json
/// [["content-type", "application/json"], ["x-foo", "bar"]]
/// ```
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct ForeignRequestAbi {
    pub method: ForeignSlice,
    pub path: ForeignSlice,
    pub query: ForeignSlice,
    pub headers_json: ForeignSlice,
    pub body: ForeignSlice,
}

/// Owned response data returned by a foreign HTTP handler.
///
/// `status` must be a valid HTTP response status in the inclusive
/// `100..=599` range. Rust bindings must reject anything outside that range
/// before constructing an HTTP response.
#[repr(C)]
#[derive(Debug)]
pub struct ForeignResponseAbi {
    pub status: u16,
    pub headers_json: ForeignBuffer,
    pub body: ForeignBuffer,
}

impl ForeignResponseAbi {
    pub const fn empty(status: u16) -> Self {
        Self {
            status,
            headers_json: ForeignBuffer::empty(),
            body: ForeignBuffer::empty(),
        }
    }
}

/// Owned response metadata and stream handle returned by a foreign streaming
/// HTTP handler.
///
/// `status` follows the same `100..=599` validation rule as
/// `ForeignResponseAbi`. `headers_json` contains response metadata and is freed
/// through `claviron_foreign_free_stream_response`. `stream` is an opaque handle
/// passed to `claviron_foreign_stream_next` and eventually released through
/// `claviron_foreign_free_stream`.
#[repr(C)]
#[derive(Debug)]
pub struct ForeignStreamResponseAbi {
    pub status: u16,
    pub headers_json: ForeignBuffer,
    pub stream: *mut c_void,
}

impl ForeignStreamResponseAbi {
    pub const fn empty(status: u16) -> Self {
        Self {
            status,
            headers_json: ForeignBuffer::empty(),
            stream: ptr::null_mut(),
        }
    }
}

/// One owned response-body chunk produced by a foreign stream.
///
/// The foreign library owns this chunk buffer. Rust must release every produced
/// chunk with `claviron_foreign_free_stream_chunk`. Bindings should prefer
/// `ForeignStreamChunkOwner` to attach that free callback to `Bytes` drop
/// lifecycle instead of copying the chunk.
#[repr(C)]
#[derive(Debug)]
pub struct ForeignStreamChunkAbi {
    pub body: ForeignBuffer,
}

impl ForeignStreamChunkAbi {
    pub const fn empty() -> Self {
        Self { body: ForeignBuffer::empty() }
    }
}

/// Rust owner for a foreign stream chunk.
///
/// This is the zero-copy bridge between the C ABI and Rust HTTP bodies. The
/// owner holds the chunk ABI struct plus the foreign chunk-free callback. When
/// converted with `Bytes::from_owner`, the foreign bytes are borrowed by
/// `Bytes` and the foreign free function runs only when the final `Bytes`
/// clone is dropped.
pub struct ForeignStreamChunkOwner {
    chunk: ForeignStreamChunkAbi,
    free_chunk: ForeignFreeStreamChunkFn,
}

unsafe impl Send for ForeignStreamChunkOwner {}

impl ForeignStreamChunkOwner {
    /// Adopt a chunk produced by `claviron_foreign_stream_next`.
    ///
    /// # Safety
    ///
    /// `chunk` must have been initialized by the same foreign library that
    /// exported `free_chunk`. After this call, the owner is responsible for
    /// releasing the chunk and the caller must not call `free_chunk` for it.
    pub unsafe fn new(
        chunk: ForeignStreamChunkAbi,
        free_chunk: ForeignFreeStreamChunkFn,
    ) -> Self {
        Self { chunk, free_chunk }
    }

    /// Convert the adopted chunk into `Bytes` without copying.
    pub fn into_bytes(self) -> Bytes {
        Bytes::from_owner(self)
    }
}

impl AsRef<[u8]> for ForeignStreamChunkOwner {
    fn as_ref(&self) -> &[u8] {
        if self.chunk.body.len == 0 {
            &[]
        } else {
            assert!(
                !self.chunk.body.ptr.is_null(),
                "foreign stream chunk pointer is null with non-zero length"
            );
            unsafe {
                slice::from_raw_parts(
                    self.chunk.body.ptr.cast_const(),
                    self.chunk.body.len,
                )
            }
        }
    }
}

impl Drop for ForeignStreamChunkOwner {
    fn drop(&mut self) {
        unsafe {
            (self.free_chunk)(&mut self.chunk);
        }
    }
}

/// C-compatible handler function.
///
/// Return `0` for success. Non-zero return codes are mapped by the Rust binding
/// into `ForeignError::Call`.
pub type ForeignHandleFn = unsafe extern "C" fn(
    request: *const ForeignRequestAbi,
    response: *mut ForeignResponseAbi,
) -> i32;

/// C-compatible response-free function for buffers allocated by the foreign
/// library.
pub type ForeignFreeResponseFn =
    unsafe extern "C" fn(response: *mut ForeignResponseAbi);

/// C-compatible streaming handler function.
///
/// Return `0` for success. Non-zero return codes are mapped by the Rust binding
/// into `ForeignError::Call`.
pub type ForeignHandleStreamFn = unsafe extern "C" fn(
    request: *const ForeignRequestAbi,
    response: *mut ForeignStreamResponseAbi,
) -> i32;

/// Pull the next body chunk from a foreign stream handle.
pub type ForeignStreamNextFn = unsafe extern "C" fn(
    stream: *mut c_void,
    chunk: *mut ForeignStreamChunkAbi,
) -> i32;

/// Free streaming response metadata allocated by the foreign library.
///
/// This releases fields such as `headers_json`. It must not free the opaque
/// stream handle; the handle has its own lifecycle.
pub type ForeignFreeStreamResponseFn =
    unsafe extern "C" fn(response: *mut ForeignStreamResponseAbi);

/// Free one body chunk allocated by the foreign library.
pub type ForeignFreeStreamChunkFn =
    unsafe extern "C" fn(chunk: *mut ForeignStreamChunkAbi);

/// Free or cancel the opaque foreign stream handle.
pub type ForeignFreeStreamFn = unsafe extern "C" fn(stream: *mut c_void);
