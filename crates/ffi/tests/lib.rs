use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;
use http::StatusCode;
use http::header::{ACCEPT, CONTENT_TYPE, HeaderValue};

use crate::abi::{
    ForeignBuffer, ForeignFreeStreamChunkFn, ForeignStreamChunkAbi,
};
use crate::{ForeignError, ForeignHeaderBlock, ForeignResponse};

#[test]
fn json_response_sets_content_type() {
    let response =
        ForeignResponse::json_bytes(Bytes::from_static(br#"{"ok":true}"#));

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(CONTENT_TYPE).unwrap(),
        "application/json"
    );
    assert_eq!(response.body(), &Bytes::from_static(br#"{"ok":true}"#));
}

#[test]
fn rust_owned_foreign_buffer_round_trips() {
    let buffer = ForeignBuffer::from_vec(b"hello".to_vec());

    let bytes = unsafe { buffer.into_vec() };

    assert_eq!(bytes, b"hello");
}

#[test]
fn error_maps_to_response_status() {
    let response = ForeignError::symbol(
        "/opt/app/libscore.dylib",
        "score_json",
        "symbol not found",
    )
    .into_foreign_response();

    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(
        response.headers().get(CONTENT_TYPE).unwrap(),
        "text/plain; charset=utf-8"
    );
    assert!(
        std::str::from_utf8(response.body())
            .unwrap()
            .contains("libscore.dylib")
    );
    assert!(
        std::str::from_utf8(response.body()).unwrap().contains("score_json")
    );
}

#[test]
fn header_block_borrows_header_map() {
    let mut headers = http::HeaderMap::new();
    headers
        .insert(ACCEPT, HeaderValue::from_static("application/octet-stream"));

    let block = ForeignHeaderBlock::from_header_map(&headers);
    let abi = block.as_abi();
    let pairs = unsafe { abi.as_slice() };

    assert_eq!(pairs.len(), 1);
    assert_eq!(unsafe { pairs[0].name.as_bytes() }, b"accept");
    assert_eq!(
        unsafe { pairs[0].value.as_bytes() },
        b"application/octet-stream"
    );
}

#[test]
fn stream_abi_empty_response_has_no_handle() {
    let response = crate::abi::ForeignStreamResponseAbi::empty(200);
    let chunk = crate::abi::ForeignStreamChunkAbi::empty();

    assert_eq!(response.status, 200);
    assert!(response.stream.is_null());
    assert_eq!(response.headers_json.len, 0);
    assert_eq!(chunk.body.len, 0);
}

#[test]
fn stream_chunk_owner_frees_on_bytes_drop() {
    static FREED: AtomicUsize = AtomicUsize::new(0);

    unsafe extern "C" fn free_chunk(chunk: *mut ForeignStreamChunkAbi) {
        if chunk.is_null() {
            return;
        }
        let chunk = unsafe { &mut *chunk };
        let buffer = std::mem::replace(&mut chunk.body, ForeignBuffer::empty());
        unsafe {
            drop(buffer.into_vec());
        }
        FREED.fetch_add(1, Ordering::SeqCst);
    }

    FREED.store(0, Ordering::SeqCst);
    let chunk = ForeignStreamChunkAbi {
        body: ForeignBuffer::from_vec(b"hello".to_vec()),
    };
    let owner = unsafe {
        crate::ForeignStreamChunkOwner::new(
            chunk,
            free_chunk as ForeignFreeStreamChunkFn,
        )
    };
    let bytes = owner.into_bytes();

    assert_eq!(&bytes[..], b"hello");
    assert_eq!(FREED.load(Ordering::SeqCst), 0);

    drop(bytes);
    assert_eq!(FREED.load(Ordering::SeqCst), 1);
}
