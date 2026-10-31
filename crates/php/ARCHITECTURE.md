# claviron-php Architecture

`php` is the shared PHP contract crate. It is used by both PHP
execution backends:

- `php-embed` (SAPI/embed workers)
- `php-fcgi` (FastCGI/FPM)

This crate does not execute PHP. It defines the shared primitives both
backends need to agree on.

The embed backend has two HTTP models: `runner: script` for request-scoped
script execution and `runner: application` for a named persistent application.
Application routes supply `application: <name>` from `php.yaml applications`,
not an `entry_script` or a new process/pool configuration. Both enum variants
are gated by `php_embed`; builds without embed reject `application` runners.
`PhpRunner` supports serialization as well as deserialization so single-application
launches can carry their resolved execution selection to replacement workers.
The route schema validates a nonempty name, rejects selectors on other runners,
and rejects conflicting script/FPM/try-files/streaming/body-limit settings.
Boot environment and capacity belong to the named application definition.
Application routes reject `environment` (aliases `env`, `environments`) and
`env_file` overrides during configuration validation. Environment sources belong
to the named application's boot definition, not to a site or request. Script
and FastCGI routes retain their existing environment settings.
`PhpRuntime.application` preserves the selected name through host composition;
the vhost router mounts the backend-owned prefix adapter. No PHP runtime types
or execution dependencies are imported into this shared contract crate.

## Shared Primitives

The crate owns:

- `PhpConfig` route/runtime configuration
- `PhpRuntime` derived runtime shape
- `PhpRunner` / `ExecMode` backend selection
- header deny-list behavior

This crate intentionally owns no generic PHP metrics family. FCGI and E-PHP
are separate execution engines with different health signals, so each backend
owns its own store, names, configuration, Orbit transport, and formatter.
There is no synthetic total that merges the two runtimes.

`PhpConfig::entry_extensions` is part of that shared contract:
extensionless paths and `.php` scripts follow the normal resolver,
while dotted non-`.php` paths only fall back to `entry_script` when
their extension is explicitly listed. This keeps asset misses on the
static/404 path by default while allowing rare route-shaped URLs such
as content-addressed avatars (`/users/avatar/<hash>.png`).

Keeping these in one crate prevents FCGI and SAPI from drifting into two
different configuration languages.

`PhpRuntime.mapping` carries HTTP's prepared `RouteMapping`. Edge supplies the
selected document directory and URL prefix once. Both backends remove that
prefix only while locating a URI-derived script or static candidate on disk.
They preserve the original HTTP URI, query, public script name and existing
PATH_INFO handling. A pinned `entry_script` is already relative to the document
directory and never has a URL prefix removed or an existence probe added.
An explicitly configured PHP `dir` retains its document-directory override.

Native mount preparation belongs to Edge's pipeline/embed/php.rs. This shared
contract crate has no Axum feature, dependency or routing builder. The host
prepares distinct Script, Application and FCGI call objects once; requests do
not match a runner enum or carry another runner's future state. Execution and
body delivery remain with the selected engine.

When neither backend feature is compiled, `PhpRunner::Unavailable` keeps the
shared configuration contract constructible. It is skipped by serde and is not
an executable runner.

## `EXECUTION_IO: OPCACHE_OWNS_FRESHNESS`

> **Where the entry is pinned, Claviron does not ask the disk what opcache
> already knows.**
>
> On a PHP execution path with a pinned entry — `runner: application`, or
> `entry_script` — Claviron performs no filesystem work for existence,
> freshness or parse state. That is opcache's job and the operator has already
> paid for it.
>
> The guarantee is scoped to that shape on purpose. A deployment that keeps
> `.php` files next to its assets in one folder has no pinned entry, and there
> the resolver genuinely has to ask the disk. That is not a violation of this
> rule; it is the shape the rule does not cover.

This is the normative text for the whole workspace. Other documents and code
sites point here and state only their own application of it; none of them
restates it.

**Why it is a rule and not a preference.** `opcache.validate_timestamps=0` is
the steady-state production setting: it tells PHP to stop stat-ing scripts and
serve them from shared memory. An operator who sets it has bought exactly one
thing — the syscall per include, gone. If Claviron stats the same script on
every request before handing it to PHP, it hands that syscall straight back and
the setting becomes decoration. The cost is not theoretical on a hot path;
`stat` on a network or container-overlay filesystem is the expensive part of
serving a request opcache would otherwise answer from memory.

### Where the guarantee is firm

**`runner: application`.** No filesystem resolution at all, per request or
otherwise. The entry is fixed when the application boots and the worker is
persistent, so there is nothing a per-request stat could learn. Adding one
here would be pure waste — a regression, not a hardening.

**`entry_script`.** The operator pinned the entry; Claviron takes them at their
word and never validates it. A missing file there is a configuration error, not
a request error, and it should fail loudly at boot rather than quietly per
request. `validate_script` explicitly does not apply to this path.

**`PhpEngine::run`, both backends.** No pre-flight `fs::metadata` on the script
file once it has been resolved. A genuinely missing script is PHP's own 404 to
emit. See `claviron-php-embed/src/engine/mod.rs`.

**Resolution by shape, not by probing.** The resolver maps a URI to a script
name from the string alone — direct `.php`, embedded `/foo.php/extra`,
`entry_script`, directory index. Deciding *which* script is never a disk
question.

### Where it is not, and must not be forced

A folder that mixes `.php` with assets and has no front controller is a first
class deployment, not a legacy one to be optimised away. There, whether
`/foo.php` exists, and whether `/report.pdf` is a static file or a route, are
questions only the disk can answer. Two stats serve that shape and both are
operator-controlled:

- **`validate_script` (default `true`).** One `fs::metadata` on a URI-derived
  `.php` candidate before PHP is invoked. Structural for the mixed-folder
  shape; it also buys a smaller blast radius, because without it a request for
  a missing script reaches libphp, which emits a fatal `Failed opening
  required` page with a 200 status and the full `DOCUMENT_ROOT` and
  `include_path` in the body. Set `false` when something upstream already
  guarantees existence — an atomic deploy with `validate_timestamps=0`, or an
  immutable container image — which is exactly the pinned-entry shape above.
- **`try_files` (opt-in, default off).** One `fs::metadata` per candidate.
  Never redundant: "does a real file win over the front controller" *is* a
  disk question and no cache can answer it.

Do not narrow either of these in the name of this rule. The rule says the
pinned-entry path owes no stat; it does not say every deployment must pin its
entry.

### The static file service in front of it

`files` decides a path is dynamic from the **name**, before any IO:
`is_dynamic_file_path` runs ahead of the cache lookup and ahead of
`symlink_metadata`, and `serve_index` skips dynamic index candidates without
probing for them. Do not move a `stat` in front of that decision.

Static files are the one place where stat *is* the job — validators, ranges and
cache freshness are disk questions. But a path already known to belong to PHP
is not a static file, and probing it before handing it over is work thrown
away twice: once here, once again in the resolver.

### The direction a change must not take

The failure mode is always the same shape: *add a stat to be safe*. A missing
file, a stale cache, a confusing error — and the smallest-looking fix is one
more metadata call on the request path. Each is individually cheap and
collectively the thing this rule exists to prevent.

So: a new stat on a pinned-entry path needs a stated reason opcache cannot
cover. If the honest answer is "so the error message is nicer", the error
message is the thing to fix. On the mixed-folder path, no justification is
owed — that stat is the feature.

## Runtime Boundary

`PhpRuntime` is created from a vhost/path configuration and then consumed
by the selected backend. The backend owns execution, process/thread
state, and error conversion.

Environment declarations retain `env_file` paths and inline `environment`
values separately in both `PhpConfig` and `PhpRuntime`. Composition performs no
environment-file I/O or merging. `env` and `environments` are deserialize aliases
for `environment`, inside the `php` declaration. They are not host-level route
directives; declaring more than one spelling is a duplicate-field error.
The route carries a shared, initially empty
execution-environment slot; an embedded backend prepares it once and pins a
snapshot for each request. Dropping the route releases the registration, and
cloning `PhpRuntime` shares it rather than duplicating watchers.
The execution backend owns evaluation through Dotenw. Embedded SAPI now uses
this route-owned capability; FastCGI remains a separate backend integration.

If a setting affects both SAPI and FCGI, it belongs here. If it affects
only one backend's execution machinery, keep it in that backend crate.

### HTTP request body limits

`PhpConfig.requests` is a route override, not a second PHP body-size limit.
`PhpRuntime` has no `post_max_size` field or numeric default. The fixed PHP
route schema rejects unknown fields, including the former `post_max_size` and
top-level `max_body_size` keys; use `requests.max_body_size` instead.

HTTP executors require the existing `HttpPolicy` request extension before
calling `PhpRuntime::policy`, which narrows that supplied policy with the PHP
route's request/response overrides. They never reread listener or host config.
A missing extension is an execution error, not implicit unlimited admission.
`None` inside a supplied effective body-size policy is valid explicit `off`.
The existing transport body guard remains the only guard wrapper. PHP's existing
body-read/collection paths validate the effective route override inline, before
buffer growth or forwarding input; they add no second guard, box or timer.
PHP/FPM's own INI limits remain independent. Console/task execution is not
HTTP admission.

## Header Policy

The denied-header helper exists so both backends treat dangerous or
transport-owned headers consistently. PHP applications can present
responses, but Claviron keeps authority over connection-level headers.

`headers::is_request_denied` additionally filters incoming `Forwarded` and
`X-Forwarded-Proto` during CGI projection, so a client cannot replace the
transport-owned scheme. This request-only rule does not broaden response
filtering. Explicit operator environment overrides remain supported.
