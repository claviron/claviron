# claviron-module

Native modules: compiled libraries loaded into a Claviron process, which register themselves
as ordinary services and applications. Write one in C, C++ or anything with a C ABI. It does
not link against Claviron, and it needs no Rust.

From outside, from the management API, a module's service or application is like any other.

```yaml
# modules.yaml, beside the server's other configuration
modules:
  - lib/libshop.so

applications:
  shop:
    instances: 8
    max_queued: 256
```

```yaml
# a site's route selects an application by name
shop.example.test:
  root: public
  module: shop
```

```c
#include <claviron_module.h>

static const claviron_host *host;

static uint32_t on_request(claviron_request *request, void *context) {
    host->response_begin(request, 200);
    host->response_write(request, (claviron_slice){(const uint8_t *)"hello\n", 6});
    return host->response_finish(request);
}

uint32_t claviron_module_abi_version(void) { return CLAVIRON_MODULE_ABI_VERSION; }

uint32_t claviron_module_register(const claviron_host *h, claviron_registrar *registrar) {
    host = h;
    claviron_http_handler http = {sizeof http, on_request, NULL, NULL, NULL};
    claviron_application application = {sizeof application, NULL, NULL, NULL, &http, NULL};
    claviron_slice name = {(const uint8_t *)"shop", 4};
    return host->register_application(registrar, name, &application);
}
```

A Rust runner takes the same directive on a route of the `site:` in its `runner.yaml`.

Enable the facade's `module` feature. Without a `modules.yaml`, nothing is installed.
[`include/claviron_module.h`](include/claviron_module.h) is the contract.

## What a library can register

| It registers | Reached by |
| --- | --- |
| An application: HTTP and WebSocket | Routes, and the application management API |
| A service: `run` on a thread of its own | The service management API |
| An operation | An invocation by name, from any language |
| An event subscription | Any publisher of that application event topic |

A library may also have one lifecycle, `boot`, `reload` and `finalize`. It can invoke
operations, publish events and log through the host table it is handed.

## Who decides what

| What | Whose | Where |
| --- | --- | --- |
| Name, kind, handlers | The library | Its register function |
| Its own settings | The library | A file it reads in `boot` |
| `instances`, `max_queued`, `scope`, `dispatch`, `handlers`, `events` | The operator | `modules.yaml` |
| `requests`, `responses`, ACLs and everything else in front of an application | The operator | Listener, host and route |

Nothing in `modules.yaml` has a default. A name no library registered, one a library registered
that the file does not declare, a name registered twice or an unknown field stops boot. A change
to the file takes a server restart.

## Errors

| What happens | The client gets |
| --- | --- |
| A handler returns an error | 500 |
| The application is stopped | 503 |
| No room to wait (`max_queued`), or `requests.queue_timeout` passes | 503 |
| A memory fault | The process dies with the code that would have answered. Its connections drop with no status. |

A service whose `run` returns an error reads failed, and the server carries on. Nothing restarts
it implicitly.

A subscriber that falls behind its `events.capacity` loses the oldest events, and each loss is logged
as an error with the number lost. A library that fails to load or register, or whose `boot` fails,
stops the server's boot with the reason.

## Limits

A module runs in the process, with its privileges and access to its keys and shared memory: load
only what you trust. A memory fault takes every other library with it. Native code cannot be
interrupted: a call that hangs takes one instance out of use, and after a stop signal the process stays
until its service manager kills it. Handlers run concurrently and share the library's
globals; with `instances` above one they must be thread-safe. A library is never unloaded. Native
WebTransport and raw HTTP/2 CONNECT are not offered to a module.

For a library in its own process, where a crash leaves the server answering 503, use a binding over
Orbit instead.

## Status and license

Part of Claviron's alpha development series; APIs and configuration may change.
