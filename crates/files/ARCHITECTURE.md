# File serving - Architecture

`files` is the site-oriented static file service. It turns a
`FileSite` view plus the canonical Claviron request context into a
`RouteResponse`: directory indexes, explicit `statics:` mounts,
the opt-in `try_files` fallback, and SPA fallback. It does not resolve
hosts, terminate protocols, or run dynamic code.

SPA fallback accepts HTTP's `SpaConfig`: `entry`, `reject_ajax` and optional
`caching` (the existing `CachingConfig`). Scalar `spa: home.html` retains the
previous behavior. `reject_ajax` rejects `X-Requested-With: XMLHttpRequest`
only after static mounts and `try_files` fail, with JSON 404, no-store and an
error log containing host/path, not query data. It does not rewrite paths.
Absent SPA caching preserves the existing shell policy; supplied caching uses
the same memory/file cache as other files. Only raw shell bytes belong there;
session/CSRF personalization remains the consuming application's responsibility.
Legacy `FileSite::spa()` implementors keep working; owners supplying typed
options also expose `spa_config()`.

The crate sits below vhost dispatch and above the shared HTTP body type:

```text
claviron-edge / app bootstrap
        |
        | HTTP FilesConfig / FileSite + RequestContext
        v
claviron-files
        |
        | RouteResponse / RouteBody
        v
claviron-http
```

The same file service is used from both host integration modes:

- Configured native host pipelines prepare Static and TryFiles steps using
  the shared file service. Residual directory-index/SPA fallback stays behind
  the selected backend, with statics and TryFiles removed to avoid a second
  probe. These calls do not require Axum or its file middleware.
- `RustRouter::Other(OtherService)` - the axum-free extension boundary.
  An application-owned `OtherHandler` can explicitly compose `ResourceAdapter`
  or the public file-service functions before its own fallback. There is no
  common `Asset` dispatch variant or implicit file pass. `UpstreamRouter` owns
  its configured file fallback separately.

That sharing works because this crate's core API is not an axum API.
It consumes `RequestContext` and returns `ServiceResult<RouteResponse>`
where `RouteResponse` uses Claviron's boxed `RouteBody`. Axum bindings
are only an adapter layer around the same core service.

## Entry Point

`try_handle_files(state, cfg, req)` is the default public request entry point.
Its configuration boundary is `FileSite`, not Edge's `HostConfig`.
`claviron-http::config::files::FilesConfig` owns the pure root/index/try_files/
SPA/static declarations and implements that view in this crate. Custom
`FileSite` implementations (including standalone runners and bypass hosts)
remain supported without changing their APIs. Cache execution, disk probing,
path safety and response construction remain owned here.

Edge prepares owned HTTP declarations at its composition boundary; the root
is anchored once rather than resolved through HostConfig on each request.
StaticMount compiles its RouteMapping, including root/directory joining, in
new(). Dispatch only performs checked prefix removal and existing file safety
and cache work. Static mounts still carry typed prefixes for direct/runnable
callers. Edge's native plan runs Routable, Static, TryFiles, then the configured
backend; disabled probes install no step. Its residual FilesConfig excludes
statics and TryFiles while retaining index/SPA fallback behind the backend.

With the existing `axum` feature, `routing::build_router`, `routing::mount`
and `routing::mount_selected` prepare the general fallback and explicit mounts
inside this crate. They consume HTTP declarations, never HostConfig. The caller
keeps their positions relative to application owners: general fallback behind
them, explicit static mounts outside them. No cache or dispatch behavior changes.
Only this opt-in feature enables the HTTP crate's Axum conversion helpers.
The ordinary dependency on claviron-http selects no Axum feature; Edge's
Axum application capability no longer selects these file adapters implicitly.

`try_files_probe(root, uri, headers)` is the smaller reusable primitive for
callers that only want the `try_files` filesystem probe before their own
dynamic/backend fallback. The combined file-service entry point retains this local order:

1. Directory index files.
2. Explicit `statics:` mounts and their per-rule cache policies.
3. Host-level `try_files` fallback, only when `HostConfig.try_files` is true.
4. SPA fallback.
5. `Unhandled`, so the caller may continue the dispatch chain.

Host ACL and configured redirects are not file policies.
`claviron-edge::HostService` applies them before
Axum, bypass or native CONNECT dispatch, including paths never reaching files.
Direct callers of this file primitive must supply their own admission boundary;
passing a HostConfig alone does not authorize a request or apply host replies.
There is no second response/ACL enforcement in the file pipeline.

`try_files` intentionally requires an extension in the URL path and refuses
hidden or parent-path segments. Dynamic file extensions are never served as
literal static files; those are left to dynamic handlers.

## Policy Sources

`axum_static_mount_handler` is the narrow pre-runner adapter: only explicit,
segment-matched `statics:` mounts are probed, with the same
cache, conditional response and dynamic-file protections. A mount miss follows
`none_found`; no index, general `try_files`, redirect or SPA probe is added here.
The general file fallback retains the ordered pipeline above.

For a static owner already selected by an external RouteTable, `StaticMount`
and its `axum_selected_static_mount_handler` adapter dispatch one HTTP
StaticConfig directly. They do not enumerate FileSite mounts. Checked prefix
removal still maps the unchanged URI to a relative filename; segment boundaries,
decoding, traversal rejection, dynamic-file exclusion, cache policy and
`none_found` keep their existing semantics. Direct FileSite/runner callers
retain list selection, now with one scan rather than an admission pre-scan
followed by a second scan.

Edge declares static directories through `routes.<prefix>.path`, with one
host-level `root` and a default mapping from URL components to directories.
The prepared `HostConfig::statics` view is compiled from
these routes, including route-local directory policies declared through
`statics: [{dir: assets, rules: ...}]`, for direct and axum-free callers. Each
directory's URL prefix and disk path are derived from the same parent mapping;
its rules see names below that directory. This is not a second YAML mapping
surface. Native Rust intercepts use `ResourceAdapter::handle_with_site` with
the file view already selected by route dispatch. The adapter's ordinary
`handle` retains its prepared-site behavior for bypass consumers.

HTTP's `RouteMapping` pairs the selected disk directory with its URL prefix.
`FileSite::mapping` exposes it and `route_for` permits axum-free callers to
select a prepared child. Static delivery, directory indexes and
`try_files_for_site` use the path below that prefix. They never rewrite the
HTTP request. The legacy direct `try_files_probe` primitive still accepts a
root and an unmodified root-relative URI for callers without route mappings.

Explicit static mounts' data
shape lives in `claviron-http::config::statics`; this crate composes that pure data into
runtime `CachePolicy` values and applies the actual header/cache behavior.

Static URL mounts are typed `PathPrefix` values, validated and normalized at
config load, with segment-safe matching supplied by that type rather than a
second file-service matcher. Their filesystem directories are `PathBuf`; an
absolute directory stays absolute and a relative directory stays site-root
relative. This does not alter decoding, traversal protection or dynamic-file
exclusion. Prefix ownership will move to the shared edge route key during the
unified-routing migration; the current mount API still carries its typed path.

`try_files` uses the conservative default server cache:

- no positive client freshness promise by default,
- server-side cache allowed,
- timestamp validation enabled,
- validators still emitted and honored.

That split is deliberate. Unknown fallback URLs should not become immutable or
long-lived in browsers by accident, but normal HTTP validators still reduce
traffic when clients revalidate.

This is the intended mode for updatable files: user uploads, generated files
written back to disk, origin-fetched material, or any file whose URL may stay the
same while the bytes can change. The server may keep a cached copy, but
timestamp validation ties that copy to the current file metadata; clients get
ETag / Last-Modified validators so repeat requests can become `304 Not
Modified` without promising that the file is immutable.

## Freshness And Validators

Client freshness and validators are separate HTTP concepts in this crate.

`CachePolicy.client_ttl_secs` controls `Cache-Control` and `Expires`:

- `Some(n)` emits a positive `max-age=n`.
- `Some(0)` emits the explicit no-cache/no-store header set.
- `None` emits no explicit client freshness header.

ETag and Last-Modified validators describe the selected file representation.
They are emitted for file responses independently of client TTL, and
`If-None-Match` is honored on cache hits and disk-streamed responses. The
matcher uses weak comparison and supports comma-separated lists and `*`, as
required for `If-None-Match`.

Do not gate validator generation on positive client TTL. A response may have
no freshness lifetime and still be perfectly valid to revalidate.

For mutable resources, prefer validators plus timestamp validation over long
client TTLs. Reserve `immutable` and year-long `max-age` policies for
content-addressed or versioned asset URLs where changing bytes means changing
the URL.

## Behavioral Cache Test

`tests/test_caching.sh` is the executable behavior check for the cache and
range contract. It creates one small file under `/assets` and one large file
under the host root, then exercises both explicit `statics:` policy and the
host-level `try_files` fallback.

The important distinction is visible in the expected `x-cache` headers:

- `/assets/*.js` is covered by configured static rules, so the second request
  may be a fast memory hit (`HIT+`).
- `/{file}.bin` is served by `try_files`, so it uses the conservative fallback
  policy. Large uncompressed files stream from disk and remain `MISS`, but
  still emit validators and honor `If-None-Match`.
- `304 Not Modified` is expected for both paths. Validator support is not tied
  to positive browser TTL.
- `206 Partial Content` and `416 Range Not Satisfiable` are expected on both
  cached-eligible and streamed responses, with response bytes checked against
  local file slices.

Run it against a local TLS host:

```sh
./tests/test_caching.sh
```

Representative successful output:

```text
🔶 creating test files
🔷 test files ready: small=4096B big=4194304B
🔷 test_has_miss_header_on_first_request → https://claviron.home/assets/script_...js
✅ x-cache = MISS
🔷 test_has_fast_hit_header_on_second_request → https://claviron.home/assets/script_...js
✅ x-cache = HIT+
🔷 test_has_miss_header_on_first_request → https://claviron.home/file_...bin
✅ x-cache = MISS
🔷 test_still_miss_on_second_request → https://claviron.home/file_...bin
✅ x-cache = MISS
✅ x-cache = MISS
🔷 Testing If-None-Match → 304 Not Modified for https://claviron.home/file_...bin
✅ Got 304 Not Modified with If-None-Match: "400000-..."
🔷 x-cache header: BYPASS
🔷 Testing If-None-Match → 304 Not Modified for https://claviron.home/assets/script_...js
✅ Got 304 Not Modified with If-None-Match: "1000-..."
🔷 x-cache header: HIT
🔷 Range 206 on SMALL: https://claviron.home/assets/script_...js
✅ status 206
✅ Content-Range = bytes 0-15/4096
✅ file size = 16
✅ range 0-15 ok
🔷 Range 206 on BIG: https://claviron.home/file_...bin
✅ status 206
✅ Content-Range = bytes 1024-2047/4194304
✅ file size = 1024
✅ range 1024-2047 ok
🔷 Range 416 on: https://claviron.home/assets/script_...js
✅ status 416
✅ Content-Range = bytes */4096
✅ invalid range correctly rejected
🔷 Testing Range 2048-3071 matches local file for https://claviron.home/file_...bin
✅ status 206
✅ Range 2048-3071 body matches local slice
🔷 Testing Range 64-127 matches local file for https://claviron.home/assets/script_...js
✅ status 206
✅ Range 64-127 body matches local slice
```

## Cache Tiers

Global static-file cache policy lives in `config/files.yaml`, owned by
`FileCacheProvider` as a Congif `Configurable`: the reader prepares the file —
variables from the application's frozen configuration environment — before
any provider boots, `configure` installs the policy and hands the arena its
boot policy, and an absent file leaves the defaults below standing. A manual
reload re-applies the prepared snapshot and empties the cache; there is no
source watch. The arena boot policy defaults to `preserve`; `reset` is available for a deployment that
deliberately treats every cold owner boot as invalidation. A reset is still
performed only after the SHM lifecycle proves that no process is using the
arena; safety is runtime behavior rather than part of the operator-facing
variant name.
Arena bytes, index slots, and maximum record size remain compile-time geometry
because every fleet process must map the same SHM layout. The runtime
`memory.max_body_size` may select a smaller admission threshold but cannot
exceed that compiled record geometry. Process-memory budget, disk location and
retention, and collector batch size are ordinary operator policy and therefore
live in the same typed file.

```yaml
arena:
  boot: preserve
memory:
  max_body_size: 1 MiB
  max_bytes: 256 MiB
disk:
  directory: storage/cache/http
  retention: 365d
gc:
  max_remove: 128
```

The small-file memory cache stores complete bodies below
`MAX_CACHE_BODY_BYTES`, with a process-wide byte budget. It may also keep one
encoded body when the policy enables compression.

The large-file cache path is only used for compressed large files. It stores
compressed bodies on disk and keeps metadata in memory. Uncompressed large
files stream from disk and rely on OS page cache.

Encoded representations are only served when the request's
`Accept-Encoding` permits the policy-selected encoding. If a client asks only
for an unsupported encoding such as `gzip`, Claviron serves the identity file
body instead.

Compressed large-file bodies are stored under a sharded content-hash path:
`storage/cache/http/{aa}/{bb}/{hash}.{br,zst}`. The first two hash byte pairs
are directory shards so a busy host does not accumulate every cached body in
one directory.

Large disk-backed responses are streamed through `ReaderStream` /
`StreamBody` into `RouteBody`; they are not read into a `Vec<u8>` before
building the response. This preserves backpressure and avoids allocating the
whole file in memory. Claviron does not currently expose a kernel `sendfile(2)`
path: TLS, HTTP/2, HTTP/3, compression, and the shared boxed `RouteBody`
boundary all mean the portable contract is streaming, not OS-level zero-copy.

Cache entries are representation metadata plus bytes. Timestamp validation
keeps entries tied to the current file contents when policy requires it.
The file cache owns its byte accounting in `TOTAL_CACHE_BYTES`; admission,
replacement, eviction, and garbage collection all update that single source.
When `metrics` is enabled, the service records the response cache outcome and
current `MEMORY_CACHE.len()` in the shared HTTP metrics family. Cache hits,
misses, memory-entry count, and the `TOTAL_CACHE_BYTES` budget value describe
one HTTP cache concern; the file crate does not create a separate metrics
provider or Orbit service for them.

## Body Boundary

All responses leave this crate as `claviron_http::RouteResponse` with boxed
`RouteBody`. This keeps file serving transport-agnostic: HTTP/1, HTTP/2, and
HTTP/3 adapters all see the same response/body shape.

Do not make the file pipeline generic over axum body types. Axum bindings are
feature-gated edge helpers; the core service is expressed in Claviron's shared
HTTP types.

## Native Host And Optional Axum Integration

Edge's host compiler prepares native Static and TryFiles steps, followed by
Embed/Upstream/Link and residual index/SPA fallback. Registered Rust
applications run ahead of those steps; they do not require the file service
to construct a router. A miss returns Unhandled with the original request.

Explicit Bypass handlers can compose ResourceAdapter or the public file
functions themselves. ResourceAdapter first applies the supplied FileSite,
then calls its GET-style Handler only for an unhandled request. UpstreamRouter
retains its own explicit file composition. No Asset enum variant or implicit
Bypass file pass exists.

The optional axum feature preserves routing::build_router, routing::mount,
routing::mount_selected and the body-converting middleware adapters. It also
selects claviron-http/axum for those conversions. Native file serving selects
neither these adapters nor the HTTP Axum bindings. Applications choose the
optional adapters explicitly; Edge's Axum application feature does not enable
them on their behalf.
