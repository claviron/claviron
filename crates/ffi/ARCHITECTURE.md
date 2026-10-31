# claviron-ffi Architecture

`ffi` is Claviron's foreign handler boundary. It lets a normal
Claviron route call native code and turn the result back into an HTTP response
without changing the vhost pipeline.

It is intentionally small:

```text
Claviron route -> Rust binding -> foreign handler -> HTTP-shaped result
```

This crate is not a router, not a vhost `RustRouter`, and not a dynamic loader. It
defines the stable ABI shapes and the safe Rust ownership helpers needed by
crates that do load and call foreign code.

## Why This Exists

Rust is the host language of Claviron, but useful systems code often already
exists outside Rust: C libraries, C++ engines, Go `c-shared` libraries,
platform syscalls wrapped in C, or domain libraries that would be wasteful to
rewrite.

The goal is not "write web apps in C". The goal is:

- keep Claviron's TLS, h2/h3, config, static files, upstreams, deny rules, and
  runtime lifecycle
- let a route call a foreign implementation for the one piece of work where
  native code is the right tool
- keep memory ownership and shutdown behavior explicit enough to audit

## Crate Boundaries

- `abi.rs` owns the C-compatible structs, symbol names, return codes, and
  memory ownership helpers.
- `response.rs` owns the safe buffered response shape used by Rust route code.
- `http` owns the minimal buffered callable trait.
- `headers.rs` owns stable header-pair borrowing helpers.

This crate deliberately does not know how a foreign library is found. Loading,
symbol lookup, config, route mounting, and provider lifecycle belong to the app
or provider crate that owns that integration.

## Integration Shapes

FFI is a handler primitive. It can be mounted into Claviron in several ways.

### 1. Plain Route Merge

The simplest shape is a normal Rust router that calls a foreign library:

```rust
async fn handle_foreign(
    State(state): State<AppState>,
    body: Bytes,
) -> Response {
    let engine = state.registry_ref().get::<ForeignEngine>()?;
    engine.call(body).await.into_response()
}
```

This is best for app-owned routes and experiments. It keeps vhost dispatch
simple: the app router is merged like any other Rust router.

### 2. Routable Provider

When an integration owns its own config and route set, it should be a
`Routable` provider in the vhost pipeline. The provider can read config at boot,
load the foreign library once, and expose routes for matching hosts.

```text
Provider boot -> read provider config -> load foreign library
Routable routes -> mounted into matching vhost
Request -> route handler -> foreign call
```

This is the right shape for reusable Claviron extensions: foreign diagnostics,
native scoring engines, protocol bridges, or project-specific integrations.

### 3. Bypass Adapter

Use Bypass only when the integration needs to participate in the axum-free
bypass chain: FCGI-style adapters, raw resource serving, or a transport-facing
handler that should receive `Request<RouteBody>` directly.

Most FFI integrations should not start here. A Bypass adapter is useful, but it
has a stronger contract and belongs near the vhost dispatch boundary.

```text
transport body -> RouteBody -> OtherService -> foreign adapter
```

## ABI Contracts

Foreign libraries export their ABI version:

```text
claviron_foreign_abi_version
```

They may expose either or both response styles.

Buffered response:

```text
claviron_foreign_handle
claviron_foreign_free_response
```

Streaming response:

```text
claviron_foreign_handle_stream
claviron_foreign_stream_next
claviron_foreign_free_stream_response
claviron_foreign_free_stream_chunk
claviron_foreign_free_stream
```

Buffered and streaming are deliberately separate. A buffered handler returns a
complete body. A streaming handler returns headers plus an opaque stream handle,
then Rust repeatedly pulls chunks.

## Buffered Response

Buffered calls are good for small or naturally completed work:

- compact JSON responses
- score/calculation calls
- small binary transforms
- "call returns bytes" integrations

Flow:

```text
Rust builds ForeignRequestAbi
C fills ForeignResponseAbi { status, headers_json, body }
Rust validates status and headers
Rust copies or adopts body according to binding policy
Rust calls claviron_foreign_free_response
```

Tiny C-side sketch:

```c
int claviron_foreign_handle(const ForeignRequestAbi *req, ForeignResponseAbi *out) {
    out->status = 200;
    out->headers_json = make_buffer("[[\"content-type\",\"application/json\"]]");
    out->body = make_buffer("{\"ok\":true}");
    return 0;
}
```

This is intentionally boring. Boring is good for FFI.

## Streaming Response

Streaming calls are for responses that should not be completed in memory before
the first byte is sent:

- event streams
- generated binary output
- long exports
- log tails
- foreign proxies
- large native results

Flow:

```text
Rust builds ForeignRequestAbi
C fills ForeignStreamResponseAbi { status, headers_json, stream }
Rust validates status and headers
Rust pulls chunks with claviron_foreign_stream_next
Each chunk becomes Bytes
completion/error/cancel/shutdown calls claviron_foreign_free_stream
```

The streaming contract has three separate ownership objects:

- response metadata, freed with `claviron_foreign_free_stream_response`
- body chunks, freed with `claviron_foreign_free_stream_chunk`
- the opaque stream handle, freed or cancelled with `claviron_foreign_free_stream`

Keep those paths separate. Normal completion, chunk failure, browser close, and
graceful shutdown become easy to reason about.

## Stream Chunk Ownership

`ForeignStreamChunkAbi` carries a foreign-owned body buffer. Rust bindings
should adopt chunks with `ForeignStreamChunkOwner`:

```rust
let mut chunk = ForeignStreamChunkAbi::empty();
let code = unsafe { stream_next(stream, &mut chunk) };

if code == ForeignStreamNextCode::Chunk.as_i32() {
    let owner = unsafe { ForeignStreamChunkOwner::new(chunk, free_stream_chunk) };
    let bytes = owner.into_bytes();
    // bytes now owns the foreign chunk lifecycle
}
```

Lifecycle:

```text
C allocates chunk buffer
Rust wraps chunk + free callback in ForeignStreamChunkOwner
Bytes::from_owner borrows the foreign buffer
last Bytes drop calls claviron_foreign_free_stream_chunk
```

That is the zero-copy response-body path. A binding may still copy a chunk if
it wants the simpler policy, but streaming integrations should default to owner
adoption.

## Shutdown And Timing

Foreign code should not sleep inside `stream_next` for long intervals. Let the
Rust body stream own timing, because Rust can race sleeps against Claviron's
shutdown token.

Good:

```text
Rust waits/cancels -> calls stream_next -> C produces one chunk quickly
```

Bad:

```text
Rust calls stream_next -> C sleeps for seconds -> shutdown waits behind C
```

The foreign stream handle must be released on:

- normal end
- foreign error
- client disconnect
- route/body drop
- graceful shutdown cancellation

## Request Streaming

Current request bodies are still passed as `ForeignSlice`, so they are buffered
before the foreign call. That is fine for small inputs and the first reference
streaming examples.

True request streaming is a separate future ABI shape:

```text
foreign handler pulls request body chunks
foreign handler produces response body chunks
```

Do not overload the current buffered request field to mean streaming. Response
streaming and request streaming have different lifecycles and should stay
auditable.

## Invariants

- `ForeignSlice` is borrowed. The caller owns it and it is valid only during
  the foreign call.
- `ForeignBuffer` is owned by the allocator that created it. The same side must
  free it.
- Response statuses must be validated as `100..=599` before constructing an
  HTTP response.
- Header JSON uses a stable pair array:

```json
[["content-type", "application/json"], ["x-foo", "bar"]]
```

- Stream chunks must be freed exactly once.
- Stream handles must be freed exactly once.
- A library handle must remain loaded while any function pointer, stream
  handle, or chunk owner from that library can still be alive.

## Feature Flags

- `axum` enables Axum response conversions.
- `http` enables integration with `http`.

The core ABI structs stay framework-free.
