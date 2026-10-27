# claviron-files

Static file serving, representation caching, and resource fallback for Claviron.

This crate turns a `FileSite` view and shared HTTP request context into a
`RouteResponse`. It owns disk probing, static representations, conditional
responses, and streaming. Host selection, admission/authentication, transport
listeners, and dynamic application execution belong to the enclosing system.

Applications normally use `claviron::files` through the root crate's `files`
feature. [Edge](../edge/README.md) and [Runner](../runner/README.md) can use the
same core file service without making it depend on their host/application
configuration types.

## One file-serving view

`FileSite` supplies the root, directory indexes, explicit static mounts,
`try_files`, and optional SPA shell. The pure HTTP
`config::files::FilesConfig` implements that view; a custom site owner can
implement the trait instead.

For example, these are file-serving declarations supplied by an enclosing
site/route composition, not the contents of the cache-policy `files.yaml`:

```yaml
root: public
index: [index.html]
try_files: true
spa:
  entry: index.html
  reject_ajax: true
```

The composition anchors a relative root against the application working
directory. Static mount directories can be site-root relative or absolute.
The file service does not reinterpret those paths relative to a YAML document.

The general `try_handle_files` pipeline tries directory indexes, explicit
static mounts, opt-in `try_files`, then the SPA fallback. If none claims the
request, it returns `ServiceResult::Unhandled` so the caller can continue.
Explicit mounts may instead be installed ahead of application handlers by the
enclosing router.

## Mounts, fallthrough, and dynamic files

Mount prefixes match path segments rather than arbitrary string prefixes.
`StaticMount` dispatches a mount already selected by an external route table;
it does not scan every site mount again. `try_files_probe` is the smaller disk
probe for an owner that wants its own fallback after a miss.

URL path segments are decoded before traversal/hidden-file checks. Encoded
separators and NUL are refused. Dynamic extensions are not served as literal
static source files. General `try_files` requires an extension in the path;
extensionless application routes do not trigger that disk fallback.

Mount miss policy and the enclosing dispatch order remain explicit. The
primitive itself does not apply host ACLs, redirects, CSRF, or authentication.
Direct callers must establish those boundaries and use safely owned filesystem
roots; URL checks are not a replacement for deployment filesystem policy.

## HTTP representations

File responses support ETag/Last-Modified validators, conditional requests,
and single byte ranges with partial-content and unsatisfiable-range responses.
Validators are independent of positive browser freshness: mutable resources
can revalidate without being promised a long `max-age`.

Metadata ETags retain the available modification-time precision and are weak
validators, shared across disk, process-cache and fleet-cache paths. They do not
claim a content digest: replacing bytes while preserving both size and exact
modification time is not detectable by this policy. Use versioned URLs for that
case. Weak ETags support `If-None-Match` revalidation but cannot satisfy strong
`If-Match` or ETag-based `If-Range`; the latter falls back to a full response.
No additional file read or content hash is required to generate a validator.

List-valued conditional and encoding headers include every field occurrence;
singleton date and `If-Range` fields retain their own parsing.

Client TTL, server-side reuse, timestamp validation, compression, and immutable
policy are distinct choices. Use immutable caching only when the URL identifies
unchanging bytes. Conservative `try_files` policy permits server reuse with
timestamp validation and validators, without silently promising long browser
freshness for an unknown URL.

Brotli and Zstd representations are served only when the request permits the
selected encoding. Unsupported encoding-only requests fall back to the identity
file representation. Large disk-backed bodies stream under HTTP backpressure;
there is no advertised kernel `sendfile` or end-to-end zero-copy guarantee.

## SPA fallback is not an API response

The SPA declaration accepts a shell filename or typed `entry`, `reject_ajax`,
and optional `caching` policy. With `reject_ajax`, an XMLHttpRequest reaching
the shell fallback receives JSON 404 and `no-store`, rather than HTML for a
missing API endpoint. This occurs only after earlier file probes fail.

Only raw shell bytes belong in this cache. A consuming application owns any
session, CSRF, or user-specific response transformation; shared cached bytes
must not accidentally become personalized content for another request.

## Static cache policy and storage

`FileCacheProvider` owns cache policy from `files.yaml`: arena boot behavior,
memory body/byte limits, encoded disk-cache directory and retention, and GC
batch size. Congif prepares it before boot. An absent document keeps the typed
defaults. Explicit reload reapplies the policy and invalidates cached
representations; merely saving the file does not trigger a source observer.

Small complete representations can use the fleet arena when Orbit support and
an arena provider are available. Otherwise the process-memory tier serves them
within its byte budget. Compressed large representations use disk storage with
in-memory metadata; uncompressed large files stream from the source and rely
on the OS page cache.

`arena.boot: preserve` is the default. Opt-in `reset` still requires the SHM
lifecycle to prove quiescence before resetting retained state. Arena bytes,
slots, and maximum record size are compiled geometry that every attaching
process must share; runtime body limits cannot exceed that layout.

This is the file service's representation cache, not the named application
stores in [claviron-cache](../cache/README.md). The implementation's `cache`
feature is currently a composition marker, not a connection to `CacheProvider`.

## Resource handlers and optional integrations

`ResourceAdapter` runs the file probe before a user `Handler`. Only an
unhandled request reaches that fallback, which can generate, fetch, or look up
a resource. Saving generated bytes under the served root lets subsequent
requests use normal file caching and validators.

Edge consumers pass the selected `FileSite` to `handle_with_site`. Its prepared
`RouteMapping` pairs the disk directory with the URL prefix; static delivery,
`try_files_for_site` and generated-file placement use paths below that prefix.
The ordinary `handle` entry point retains axum-free use and selects a prepared
file route when the supplied site exposes one.

This handler receives a GET-style `RequestContext`, not a request body. Use an
application HTTP handler for body-aware POST/PUT behavior. The adapter itself
still accepts the shared `Request<RouteBody>` at the transport boundary.

The implementation has no default features. `axum` adds router/mount adapters,
`orbit` adds the fleet arena integration, `tasks` adds the registered cache-GC
task, and `metrics` records cache observations in the owning HTTP family.
There is no second file-specific metrics provider.

## Related crates

- [HTTP](../http/README.md): file declarations, shared context, bodies, and policy.
- [Edge](../edge/README.md): host admission and ordered static/application routing.
- [Runner](../runner/README.md): file serving beside a hosted application.
- [Orbit](../orbit/README.md): shared immutable body storage and lifecycle.
- [Tasks](../tasks/README.md): cache maintenance scheduling.

## Status and license

Part of Claviron's alpha development series; APIs and configuration may change.

Licensed under either [MIT](https://github.com/claviron/claviron/blob/main/LICENSE-MIT)
or [Apache-2.0](https://github.com/claviron/claviron/blob/main/LICENSE-APACHE), at your option.
