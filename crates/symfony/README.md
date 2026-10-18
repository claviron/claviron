# Symfony Composer adapter

PHP integration for Claviron's embedded PHP runtime.

The current working adapter lives in the companion [Composer package](php/README.md),
`claviron/symfony`. There is no Rust package, Symfony-specific native registration
function or framework request implementation in the Claviron workspace.
Installing and configuring the PHP adapter is separate from selecting its engine.

Select the general `php_embed` capability; there is no facade `symfony` feature
or `claviron::symfony` re-export.
The application uses the shared embedded-PHP engine and its application lifecycle,
not a second Symfony-specific HTTP listener.

## Persistent kernel adapter

Install the companion Composer package, copy its
[application entry stub](php/stubs/application.php) into the application's root,
and configure the embedded PHP application's `script_path` to select that entry.
The stub loads the application's Composer autoloader, constructs `App\Kernel`,
and calls `Claviron\Symfony\ApplicationRunner::run()`.

The supplied kernel must implement both `KernelInterface` and
`TerminableInterface`. The runner boots that kernel once, receives native request
envelopes, builds ordinary HttpFoundation requests, and calls the kernel's
handler. It sends the buffered response before invoking `terminate`; the kernel
is shut down when the receive loop ends, including exceptional exit.

Symfony owns its request/service reset behavior. Claviron owns native execution,
admission, cancellation, and execution budgets. The adapter does not start a shell
server, add a polling loop, or call `set_time_limit()` to invent another budget.
Persistent application code must respect its framework's reset contract; a
long-lived kernel does not make arbitrary application globals request-local.

## Buffered request and response contract

The request envelope supplies URI, method, raw body, headers, and optional server
values. The adapter constructs a Symfony request from those values; it does not
emulate all PHP superglobals or parse multipart uploads.

Responses must provide buffered content. Streamed/file responses and unsupported
non-HTTP messages are rejected explicitly. This is the adapter's current contract,
not a claim that the underlying Claviron HTTP body types cannot stream.

Ordinary script/SAPI and FastCGI deployments use Symfony's usual HTTP entry point.
They do not keep this persistent receive loop alive between requests or acquire
application Link presence merely by installing the package.

## Optional Link events

`ApplicationRunner::run($kernel, links: true)` opts into the Link bridge. It uses
the kernel's `event_dispatcher` and translates native local presence notifications
into typed Symfony events:

- `EdgeConnected`
- `ApplicationConnected`
- `EdgeLeft`
- `ApplicationLeft`

These classes live under `Claviron\Symfony\Link\Events`. Application subscribers
can listen to them; a supplied logging subscriber can also be registered in
Symfony's service configuration. See the [PHP integration guide](php/README.md).

Presence describes directly observed opposite-side Link lanes, not a fleet-wide
discovery query. Departure reports the final lane for a name. An unknown PID
remains null rather than becoming a guessed process identity. The bridge uses
the existing application receive loop, not an extra worker or poller.

## Requirements and package ownership

The Composer manifest declares PHP `^8.2` and Symfony HttpFoundation/HttpKernel
`^7.0 || ^8.0`, alongside its SAPI and event/logging dependencies. These are
declared constraints, not verification of every version/platform combination.
Native execution also requires the matching
[embedded PHP toolchain](../php-embed/README.md#requirements-and-optional-capabilities).

Keep application kernels, routes, user models, and deployment settings in the
application. The Rust package's dependency list is not evidence of implemented
Symfony-specific auth, cache, or broadcast adapters.

## Related crates

- [PHP](../php/README.md): shared PHP declarations and runtime contract.
- [Embedded PHP](../php-embed/README.md): engine and persistent application execution.
- [App](../app/README.md): application ownership and management.
- [Runner](../runner/README.md): a shell around a single application owner.

## Status and license

Part of Claviron's alpha development series; APIs may change. This README
describes the current source tree, not the placeholder package on crates.io.

The Rust crate is licensed under either
[MIT](https://github.com/claviron/claviron/blob/main/LICENSE-MIT) or
[Apache-2.0](https://github.com/claviron/claviron/blob/main/LICENSE-APACHE), at your option.
The companion Composer package declares MIT in [its manifest](php/composer.json).
