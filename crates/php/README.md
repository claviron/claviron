# claviron-php

Shared PHP route configuration and runtime contracts for Claviron.

This crate defines the PHP vocabulary used by the embedded and FastCGI
backends: `PhpConfig`, its prepared `PhpRuntime`, the `PhpRunner` selector, and
shared header rules. It does not link or execute PHP, manage an FPM connection
pool, or select an HTTP virtual host.

Applications normally use `claviron::php` through the root crate's `php` feature.
Select a backend explicitly with `php_embed` or `php_fcgi`; the contract crate
does not choose an engine on the application's behalf.

## Choose the execution model

| Route selector | Execution owner | PHP lifetime |
| --- | --- | --- |
| `runner: script` | [php-embed](../php-embed/README.md) request pool | A fresh PHP request scope for each HTTP request on a reusable OS/ZTS thread. |
| `runner: application` | php-embed named application pool | A persistent boot script using the explicit receive/respond protocol. |
| `runner: fcgi` | [php-fcgi](../php-fcgi/README.md) gateway and external PHP-FPM | FPM owns PHP execution; Claviron owns the FastCGI client boundary. |

Script and persistent application execution use the same SAPI engine but have
different application-state lifetimes. FastCGI is a different process boundary,
not another name for embedded PHP. Both backends can coexist in one server with
explicit route selection.

The backend enum variants depend on compiled capabilities. With no backend
selected, the contract has no executable runner. Prefer writing `runner`
explicitly when a composition supports more than one backend.

## Route declarations and prepared state

`PhpConfig` describes a PHP route. `PhpRuntime::from_config` combines it with
the caller's document root, indexes, and Claviron process context before request
dispatch. Edge and other composers supply these inputs; this crate does not
read their host configuration itself.

The principal fields are:

- Ownership: optional `path`, `runner`, named `application`, or `fpm` entries.
- Script selection: `dir`, `entry_script`, `entry_query`, `indexes`,
  `try_files`, `validate_script`, and `entry_extensions`.
- Execution environment declarations: `env_file` and inline `environment`.
- HTTP policy overrides: `requests` and `responses`.

A persistent application route selects an application already declared in the
embedded runtime. Its boot script, environment, and pool belong to that
definition, not to each route. Conflicting script/FPM settings and route-local
environment overrides are rejected for this model.

The PHP route's `dir` may override the caller's root: a relative value is joined
to that document root. Environment-file paths instead follow the execution
environment's process-working-directory rules. Do not assume every path is
relative to the YAML document.

## Script resolution and static fallthrough

A pinned `entry_script` names the front controller. On that path Claviron does
not repeatedly probe the script for existence or freshness; PHP and OPcache
remain responsible for executing the selected entry.

Mixed directories without a pinned entry have different needs.
`validate_script` controls checks on URI-derived PHP candidates; `try_files`
allows real files to win over application fallback. Extensionless routes can
use the front controller, while dotted non-PHP paths need explicit
`entry_extensions` when falling back after a static miss. Missing assets should
not become PHP pages accidentally.

The optional `routing` module, enabled by the implementation's `axum` feature,
composes resolved mounts with a caller-supplied backend builder. It constructs
routers during composition, not per request, and contains no execution engine.

## HTTP policy and trusted metadata

HTTP execution requires an effective `HttpPolicy` supplied by its ingress.
Each backend validates that boundary, then applies this route's overrides.
`requests.max_body_size` is the common HTTP limit; there is no second PHP-route
`post_max_size` setting. PHP/FPM's own INI limits remain independent.

Trusted scheme, peer, and TLS facts come from the accepted request context.
Shared header filtering prevents peer-supplied forwarding fields from replacing
that CGI projection and keeps transport-owned response headers under Claviron's
control. Framework request construction and domain authentication belong to
their respective adapters and application policy.

## Related integrations

- [Embedded PHP](../php-embed/README.md): SAPI, pools, native functions/classes,
  applications, tasks, and console execution.
- [FastCGI](../php-fcgi/README.md): external FPM entries and request forwarding.
- [Laravel](../laravel/README.md): framework drivers above embedded execution.
- [HTTP](../http/README.md): shared bodies, context, and policy.
- [Edge](../edge/README.md) and [Runner](../runner/README.md): host-aware versus
  single-application HTTP composition.

## Status and license

Part of Claviron's alpha development series; APIs may change. This README
describes the current source tree, not the placeholder package on crates.io.

Licensed under either [MIT](https://github.com/claviron/claviron/blob/main/LICENSE-MIT)
or [Apache-2.0](https://github.com/claviron/claviron/blob/main/LICENSE-APACHE), at your option.
