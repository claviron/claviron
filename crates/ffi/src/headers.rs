//! Header primitives for the C ABI surface.
//!
//! Do not expose `http::HeaderMap` across FFI. Foreign handlers should see a
//! borrowed, flat snapshot: many `(name, value)` pairs, valid only for the
//! duration of the call. Rust remains responsible for validating response
//! headers before they enter the HTTP layer.

use std::marker::PhantomData;
use std::{ptr, slice};

use http::HeaderMap;
use http::header::{HeaderName, HeaderValue};

use crate::abi::ForeignSlice;
use crate::{ForeignError, ForeignResult};

/// One borrowed HTTP header pair crossing the C ABI boundary.
///
/// Both fields are borrowed byte slices. Header names are lower-case ASCII in
/// Rust's `HeaderMap`; values are raw HTTP header bytes.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct ForeignHeaderAbi {
    pub name: ForeignSlice,
    pub value: ForeignSlice,
}

impl ForeignHeaderAbi {
    pub const fn empty() -> Self {
        Self { name: ForeignSlice::empty(), value: ForeignSlice::empty() }
    }

    pub fn from_name_value(
        name: &HeaderName,
        value: &HeaderValue,
    ) -> Self {
        Self {
            name: ForeignSlice::from_slice(name.as_str().as_bytes()),
            value: ForeignSlice::from_slice(value.as_bytes()),
        }
    }

    /// Decode the borrowed header name.
    ///
    /// # Safety
    ///
    /// The header's borrowed slices must be valid for reads for the returned
    /// lifetime.
    pub unsafe fn header_name(&self) -> ForeignResult<HeaderName> {
        let name = unsafe { self.name.as_bytes() };
        HeaderName::from_bytes(name).map_err(|_| {
            ForeignError::invalid_response("invalid response header name")
        })
    }

    /// Decode the borrowed header value.
    ///
    /// # Safety
    ///
    /// The header's borrowed slices must be valid for reads for the returned
    /// lifetime.
    pub unsafe fn header_value(&self) -> ForeignResult<HeaderValue> {
        let value = unsafe { self.value.as_bytes() };
        HeaderValue::from_bytes(value).map_err(|_| {
            ForeignError::invalid_response("invalid response header value")
        })
    }
}

/// Borrowed slice of C ABI header pairs.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct ForeignHeaderSlice {
    pub ptr: *const ForeignHeaderAbi,
    pub len: usize,
}

impl ForeignHeaderSlice {
    pub const fn empty() -> Self {
        Self { ptr: ptr::null(), len: 0 }
    }

    pub fn from_slice(headers: &[ForeignHeaderAbi]) -> Self {
        if headers.is_empty() {
            Self::empty()
        } else {
            Self { ptr: headers.as_ptr(), len: headers.len() }
        }
    }

    /// View the borrowed ABI header slice as Rust header pairs.
    ///
    /// # Safety
    ///
    /// `ptr` must be valid for reads of `len` header pairs for the returned
    /// lifetime. If `len` is zero, `ptr` may be null.
    pub unsafe fn as_slice<'a>(&self) -> &'a [ForeignHeaderAbi] {
        if self.len == 0 {
            &[]
        } else {
            assert!(
                !self.ptr.is_null(),
                "foreign header pointer is null with non-zero length"
            );
            unsafe { slice::from_raw_parts(self.ptr, self.len) }
        }
    }
}

/// Rust-owned header snapshot for a single foreign call.
///
/// The vector owns only the ABI pair array. The pair fields borrow from the
/// source `HeaderMap`, so this value must not outlive the map it was built
/// from.
#[derive(Debug)]
pub struct ForeignHeaderBlock<'a> {
    headers: Vec<ForeignHeaderAbi>,
    _source: PhantomData<&'a HeaderMap>,
}

impl<'a> ForeignHeaderBlock<'a> {
    pub fn from_header_map(headers: &'a HeaderMap) -> Self {
        let headers = headers
            .iter()
            .map(|(name, value)| ForeignHeaderAbi::from_name_value(name, value))
            .collect();
        Self { headers, _source: PhantomData }
    }

    pub fn as_slice(&self) -> &[ForeignHeaderAbi] {
        &self.headers
    }

    pub fn as_abi(&self) -> ForeignHeaderSlice {
        ForeignHeaderSlice::from_slice(&self.headers)
    }

    pub fn len(&self) -> usize {
        self.headers.len()
    }

    pub fn is_empty(&self) -> bool {
        self.headers.is_empty()
    }
}

/// Insert a foreign response header into a Rust `HeaderMap`.
///
/// # Safety
///
/// The foreign header's borrowed slices must be valid for reads for the
/// duration of this call.
pub unsafe fn insert_foreign_header(
    map: &mut HeaderMap,
    header: &ForeignHeaderAbi,
) -> ForeignResult<()> {
    let name = unsafe { header.header_name()? };
    let value = unsafe { header.header_value()? };
    map.append(name, value);
    Ok(())
}
