# claviron-ffi

Foreign HTTP handler ABI, response types, and explicit buffer ownership.

A normal Claviron route can call a foreign implementation without replacing
host admission, routing, or the transport stack. This crate supplies the boundary
types; the integration owns loading the library, resolving symbols, and mounting
the handler.

Applications access the core surface as `claviron::ffi` through the root `ffi`
feature. FFI is not an application engine, dynamic loader, process sandbox, or
additional virtual-host kind.

## Buffered Rust interface

`ForeignCall` accepts `bytes::Bytes` and returns a boxed, sendable future whose
output is `ForeignResponse`. Compatible async closures implement the trait too.
The route chooses how request context becomes input bytes. The binding converts
loading, symbol, ABI, or call failures into a response before returning through
this intentionally infallible interface.

`ForeignResponse` carries a typed HTTP status, headers, and a buffered body:

```rust
use claviron::ffi::ForeignResponse;

fn ready_response() -> ForeignResponse {
    ForeignResponse::text("foreign integration ready")
}
```

Constructors support bytes, text, JSON bytes, and an explicit status. JSON helpers
set a content type; they do not parse or validate JSON. `try_with_header` checks
header syntax, and `into_http_response()` produces `http::Response<Bytes>`
without requiring an Axum conversion.

Optional implementation features `axum` and `http` add Axum `IntoResponse` and
Claviron `RouteResponse` conversion respectively. The root `ffi` feature currently
selects the implementation with `http`, so its `RouteResponse` conversion is
available through the facade. This does not enable the separate FFI `axum`
conversion automatically.

## C ABI and response styles

The current ABI version is `1`. Foreign integrations export the version symbol
and the functions for the response style they implement:

| Style | Symbols |
| --- | --- |
| Version | `claviron_foreign_abi_version` |
| Buffered | `claviron_foreign_handle`, `claviron_foreign_free_response` |
| Streaming | `claviron_foreign_handle_stream`, `claviron_foreign_stream_next`, `claviron_foreign_free_stream_response`, `claviron_foreign_free_stream_chunk`, `claviron_foreign_free_stream` |

`ffi::abi::ForeignRequestAbi` borrows method, path, query, header JSON, and body
bytes for the call. Header JSON is an array of name/value pairs, preserving the
ability to represent duplicate headers. The Rust header-pair borrowing helpers
are a separate representation, not a replacement for this ABI field.

A buffered response returns status, header JSON, and owned body buffers. A
streaming response returns status, header JSON, and an opaque stream handle;
subsequent calls pull individual body chunks. The binding must check ABI version,
return codes, status (`100..=599`), and header syntax before exposing a response.
These declarations do not perform symbol lookup or automatically validate raw
foreign pointers.

The layouts use `repr(C)` and native pointer-sized lengths. This is an in-process
ABI for compatible builds, not a portable serialized network protocol.

## Memory belongs to its allocator

`ForeignSlice` is borrowed: its backing memory must remain valid for the foreign
call. `ForeignBuffer` is owned: allocation and release must use the corresponding
allocator. In particular, `claviron_ffi_free_buffer` may release only buffers
created through `ForeignBuffer::from_vec` with that Rust allocator, never an
arbitrary foreign allocation.

Streaming separates three ownership objects:

- Response metadata is released by `claviron_foreign_free_stream_response`.
- Each produced body chunk is released by `claviron_foreign_free_stream_chunk`.
- The stream handle is released or cancelled by `claviron_foreign_free_stream`.

`ForeignStreamChunkOwner` can adopt a valid foreign chunk and its matching free
callback. `into_bytes()` attaches that owner to `Bytes` without copying the body;
the callback runs when the last owning `Bytes` reference is dropped. Adoption is
unsafe and transfers responsibility: do not also free the same chunk manually.

Keep the library loaded while any function pointer, stream handle, or chunk owner
from it remains alive. The chunk owner does not retain a dynamic-library handle
or implement cleanup of the separate stream handle on the binding's behalf.

## Execution and cleanup remain integration responsibilities

A foreign call is not made nonblocking or interruptible by wrapping its result
in a future. The integration owns execution placement and must keep synchronous
stream pulls short enough for its lifecycle contract. Rust cancellation cannot
preempt an arbitrary in-progress native call.

Normal completion, errors, client disconnect, body drop, and shutdown must all
release the resources owned by that integration exactly once. Response streaming
is available, but request bodies in ABI v1 are still buffered `ForeignSlice`
values; request streaming needs a separate contract.

## Related crates

- [HTTP](../http/README.md): transport-neutral request and response bodies.
- [Edge](../edge/README.md): host admission and route composition.
- [Runtime](../runtime/README.md): service lifecycle and owned-work cancellation.

## Status and license

Part of Claviron's alpha development series; APIs may change. This README
describes the current source tree, not the placeholder package on crates.io.

Licensed under either [MIT](https://github.com/claviron/claviron/blob/main/LICENSE-MIT)
or [Apache-2.0](https://github.com/claviron/claviron/blob/main/LICENSE-APACHE), at your option.
