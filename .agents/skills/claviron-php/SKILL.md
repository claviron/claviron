---
name: claviron-php
description: Integrate or change embedded PHP, FastCGI, and Laravel or Symfony adapters on Claviron. Use for PHP execution modes, native registration, request identity, and host bridges rather than ordinary framework CRUD.
---

# Claviron PHP integration

Identify the execution path first. Keep framework request behavior separate from
Rust-owned admission, capacity, transport, and lifecycle.

## Execution modes

| Mode | Lifetime |
| --- | --- |
| Embedded Script | PHP request startup, script execution, and shutdown per request; a warm ZTS thread does not preserve request objects |
| Embedded Application | Named application boots once per instance and uses an explicit receive/respond loop; the framework adapter owns per-message cleanup |
| FastCGI | External PHP-FPM executes PHP; Claviron owns proxy dispatch, not Zend |

`claviron-php` defines shared contracts; `claviron-php-embed` and
`claviron-php-fcgi` implement execution. Select them through the public facade.
Laravel's Octane adapter and Symfony's persistent-kernel adapter use Application
mode; ordinary front controllers can remain in Script mode.

## Native execution and registration

Use the existing `PhpEmbedRuntime`, not another SAPI startup. Script and named
Application services own separate generations and drains. Preserve their owned
OS/ZTS execution threads; blocking PHP work must not run on a Tokio worker.

Register native functions, classes, and extensions before module startup, using
the existing registration path. Verify both process-wide MINIT and per-thread
ZTS setup when changing a native registration; thread reuse does not make Zend
handles transferable or request objects process-global.

Application routes select a declared application name. Its boot entry,
environment, and pool belong to that application, not a request or vhost override.
Preserve declared fixed/elastic capacity and separate instance execution budgets
from HTTP queue/body policy and service drain.

## Request and framework bridges

Keep native identity and authentication resolution request-scoped, including an
anonymous decision. Do not create a second PHP login/session merely to hydrate
Laravel's request user. Native session identity and Laravel's serialized session
payload are separate contracts; the opt-in middleware binds the latter without
making it a shared Rust/PHP map or exposing PHP flash automatically to Rust.

Use existing Laravel drivers for native cache, DB, broadcast, and logs. Check
both host registration and the installed Composer adapter; a Rust feature alone
does not install the PHP package. Preserve detached CLI/FPM behavior where the
adapter supports it. Do not replace a native bridge with a loopback HTTP call or
an external PHP command by convenience.

For pinned script entries and persistent applications, preserve OPcache's
freshness ownership: do not introduce per-request filesystem probes. Mixed
script/static directories retain their structural resolution rules.

## Source map

Read `crates/php/README.md` plus the selected backend's README. Laravel and
Symfony behavior lives in their respective `php/` Composer sources as well as
Rust integration. For native changes inspect the affected `php-embed` registration,
SAPI, or engine code and its architecture document when present. Do not assume
an unpublished architecture file or a maintainer's PHP installation is available.
