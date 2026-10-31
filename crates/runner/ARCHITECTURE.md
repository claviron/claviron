# Runner architecture

`claviron-runner` is the shell every language runner shares: one application,
served by one process kind, with no virtual hosts. A runner — PHP/Laravel,
Python/ASGI, and whatever comes next — consumes `claviron-runner` directly
and supplies one thing of its own: the **owner**, the service that
answers what nothing in front of it claimed.

Status: implemented. The Python and the PHP runner are built on it; what
each keeps is its owner and its language's own pieces.

## Why it exists

Every runner has grown the same skeleton:

| piece | what it is |
|---|---|
| options | application name, fleet name, TCP / Unix socket / Orbit ingress, request and response policy |
| socket ingress | accept, HTTP/1 and HTTP/2, graceful drain |
| Orbit ingress | a lane in the configured fleet, advertised under the application name, a reactor-native receiver, drain |
| TLS | one certificate, optional client CA |
| proxy trust | which peers' forwarding headers count |
| admission | concurrent requests, queue and head deadlines |
| static mounts | explicit `statics` in front of the application |
| broadcast | the Pusher protocol endpoints and the local publisher |
| workers | master and workers under the supervisor, rolling restart |
| source watching | native notifications and a restart on change |
| routing | the order of all of the above, and the owner last |

Only the owner and what "restart" means differ by language. The rest is
copied, and a third runner would copy it again. It is not the edge's either:
the edge routes many hosts, a runner serves one application, and nothing here
may make a runner boot the edge.

## Engines and frameworks

The application introduction separates runtime, framework tags, runner and request
mode from its ingress. `RunnerOptions.frameworks` (repeatable `--framework`) is an
optional application declaration; omission lets the language adapter discover
its list, while an explicit empty list suppresses discovery. Scalar `framework`
config remains accepted as a one-element list. `icon` / `--icon` declares a
symbolic admin-client icon code; clients own the catalog and default fallback.
The execution owner prepares the other
facts. Gufi reports Python / Gufi / ASGI, and Lumenize reports PHP / Lumenize /
application or script. Framework labels are free-form informational tags.
The same description travels in the
existing Application snapshot over local fleet observation or Link; no HTTP
request discovers it. Link does not imply a particular request mode, and
ordinary upstream routing remains separate from linked application control.

A runner exists per language **engine**: the owner is the engine's service
(embedded PHP, the ASGI engine, the JavaScript runtime). A framework on an
engine that already has a runner is a feature or preset of it, not a new
runner: default watch paths and `files` mounts, the framework's log door, its
integrations (a broadcast driver, a channel layer). A product name on a
preset is marketing; underneath it is the engine's runner.

A new language costs a new engine. The shell saves the rest, not that.

Small per-language pieces may still live here, behind a feature named for the
language (`php`, `asgi`), when they need no engine to build: the PHP owner's
adapter over embedded PHP, which Claviron already carries, or turning a
request's `Origin` into an ASGI scope. An interpreter never does — Claviron
builds and tests without one — and a feature here is a few conversions, not
a second architecture.

## Boundary

- Claviron still knows no language. This crate holds no interpreter, no PHP,
  no ASGI; a language runner depends on this crate directly. Lumenize follows
  this boundary; Gufi still uses the legacy facade adapter pending migration.
- It composes lower crates — `http`, `files`, `tls`, `broadcast`, `orbit`,
  `runtime` — and none of `edge`, vhosts or the hosting store.
- The lifecycle stays Claviron's: `Server`, the registry, runnable
  generations, drain and finalize. The runner adds providers, not a sequence.
- Optional pieces are this crate's own features: `files`, `broadcast`,
  `supervisor`, `tls`, `cli` and `watch`. A language runner forwards its
  features directly to this dependency. Its listener TLS does not enable
  the facade's certificate-provider bootstrap. DNS, hosting and upstream
  services are not part of this shell; an application composes them separately.

## The owner

The owner is what the application *is*: Laravel, a Django project, an ASGI
app. Everything else is an exception placed in front of it.

A runner hands the shell:

- **the owner service** — a `tower` service from request to response, booted
  by the runner's own provider before any ingress runs;
- **its transport runtime name** — what the Orbit ingress writes into its lane
  (`php`, `asgi`), so an edge knows what answers;
- **its restart** — what a source change does (below);
- **its default watch paths** — `app/`, `routes/` for Laravel; every directory
  but the virtual environment and build output for Python.

**Whoever is the fallback is the owner.** `register_router` makes the owner
the Rust router's fallback and its method-not-allowed fallback: Rust routes
only intercept, and the application stays the authority on what exists.
`register_router_with` lets the application place the owner itself; one that
places it nowhere has made its Rust service the owner, and boot says so rather
than leaving `routing.fallback` to read otherwise.

## Portable Rust code

Rust written for one runner compiles unchanged under another, and under an
edge binary: only the owner behind it differs. Nothing new is needed for
that, only that runners use what Claviron already shares instead of their
own copies:

- **Registration** is a provider that builds a `Router<AppState>` and knows
  no host, listener or process shape — `claviron::http::routing::Routable`.
  `register_router(state, build)` stays as the short form and registers one.
  The edge's own `Routable` is the one in use today: host selection
  (`match_host`) and the same router (`routes`, whose host arguments its
  implementations do not read) in one trait. The two keep the same name in
  different crates and stay different: a runner has no host key and no edge,
  and never sees either. Moving a routable to an edge is adding its host —
  `match_host` and the host arguments — and nothing of the edge's reaches a
  runner.
- **Handlers** see the facade's axum (`claviron::axum`) and its body, and
  reach services through `AppState` and `claviron::http::ext` extractors
  (`Provide`, `Inject`, `Env`, `RealIp`).
- **Connection facts** are the edge's types. An `Arc<ConnCtx>` describes
  only a connection this process accepted (none for a Unix socket, which has
  no addresses); `RealIpExt` is the client after trust; `ReceivedAt` when it
  arrived. What a trusted hop reported — a proxy's forwarding headers, or the
  edge over Orbit, where this process accepted nothing — is
  `claviron::http::trust::Forwarded`, never folded into `ConnCtx`. The owner
  reads both through `Origin::of`: the hop's word first, the connection's
  otherwise, and `None` for what neither knows. A runner-specific peer or
  scheme extension is exactly what makes a handler unportable, so none
  exists.
- **Bodies** inside the shell are claviron-http's `RouteBody`, which the edge
  and embedded PHP already speak; the owner plugs into that one type, and a
  handler never sees it.

## Routing

The runner's `axum` feature selects HTTP's opt-in `routing` feature for
the shared route-capability registry. It does not depend on Edge.

One order, the same for every ingress, outermost first:

1. **Guards** — framing, declared length, request policy, admission.
2. **Broadcast** — its own exact paths (REST publication, app-key WebSocket
   upgrades). Nothing else.
3. **Files** — GET and HEAD under an explicit `statics` mount. Any other
   method goes on, and so does a dynamic file (`.php` and its kind), whose
   source is never served. A missing file is the mount's 404, unless the mount
   says `none_found: true` — assets and the owner's paths sharing a
   directory — and then it goes on to the owner. `assets` expands relative
   directories into the same mount type. With opt-in `try_files` (default
   false), unmatched requests also probe public files before going on.
4. **Rust routes** — what the application registered.
5. **The owner.**

A front layer answers only what it claimed and passes the rest on unchanged.
The order is fixed here, not by which feature happened to wrap last.

## Configuration

Each runner keeps its own file (`lumenize.yaml`, `gufi.yaml`), flat. The keys
every runner shares are `RunnerOptions`; the rest are the runner's own (a
virtual environment, PHP application sections). `options::split` hands
each key to its side and each side refuses a key it does not know, so a typo
is an error rather than a silent default — serde's `flatten` cannot refuse
unknown keys, which is why the file is divided by hand.

```yaml
name: shop                     # application / instance address
fleet_name: shop               # independent process-family namespace
orbit: true
tcp: 127.0.0.1:8000
tcp_backlog: 1024              # pending TCP connections per socket; OS may cap it
uds: /run/shop.sock
tls_cert: ...                  # with tls
tls_key: ...
trusted: [10.0.0.0/8]          # peers whose forwarding headers count
max_in_flight: 128             # Orbit exchanges taken and not answered
max_concurrent_requests: 128   # a count or off
requests:  { max_body_size: 64 MiB, body_read_timeout: 30s, head_timeout: 60s }
responses: { body_idle_timeout: 60s }
try_files: false                # with files; general public-file probe
assets: [build, images]         # matching URL mounts under public
files:                         # with files
  root: public
  statics:
    - { path: /build, dir: build }
watch: false                   # with watch
watch_paths: []                # empty: the owner's defaults
workers: 4                     # with supervisor
supervisor: { ... }            # the supervisor's own section
```

`assets` and `files.statics` share one file site. Without a `files` section its
root is `public`; otherwise assets are relative to `files.root`. Explicit mounts
take precedence at the same prefix and keep their cache rules. `--asset` replaces
the written list; `--try-files` enables probing. Neither form enables directory
indexes, and the shared file service still rejects private paths and dynamic
source files.

`tcp_backlog` is an optional positive count through 2147483647, also available
as `--tcp-backlog`. Omission uses HTTP's declared `TcpConfig.backlog` default
(1024); an explicit command-line value overrides the file. Worker launch
serialization retains the declaration. `SocketIngress::tcp_backlog` carries
the resolved setting to `sys::net::create_tcp_listener` for each TCP bind; UDS
does not use it. Sys has no implicit queue value. Socket opening is the only
execution point, with no request-path lookup or check.

Request execution bounds remain opt-in: an unwritten one bounds nothing.
Everything serializes, because a master hands its resolved launch to its
workers; what Claviron's own types only read (the policy, a mount) travels as
written and is parsed where it is used. With `cli` the same fields are
flags, the command line standing over the file wherever it said anything.

`fleet_name` is independent of the application address and travels in the
worker envelope. `--fleet` (visible alias `--fleet-name`) overrides the file
for every language runner. The Rust field and YAML key remain `fleet_name`.
If omitted, the name is
used only when it is a valid `FleetName`; a longer application name requires
an explicit fleet name instead of truncation. Fleet validation is fallible
before server construction, not a panic from converting an unchecked string.

## Ingress

- **Socket** — TCP and Unix sockets, all bound before any is served;
  HTTP/1 and HTTP/2 on one handler; TLS on TCP with ALPN from the protocols
  served; upgrades the owner claims (an ASGI WebSocket) handed to it with the
  connection's upgrade handle, others refused; forwarded headers applied only
  from trusted peers (a Unix socket is trusted, as opening it already
  required the filesystem's permission). Workers share a TCP port the way
  Claviron's own do. A Unix socket is one process's: a stale file is removed
  only when nothing answers on it, and several workers on one are refused at
  launch until the master binds it for them. Shutdown stops accepting, asks
  connections to finish and joins them and their sessions, with no deadline
  of its own.
- **Orbit** — the lane in the configured `fleet_name` under the owner's runtime
  name, read by one Tokio task through `InboxReceiver`; SHM holds the frame
  and a named descriptor bell wakes the reactor directly, with no poll and no
  thread/channel handoff. `max_in_flight` bounds what is taken and not
  answered. A WebSocket there is spoken by the edge: the owner's Orbit session
  handler gets the request, the slot and the handshake answer, and exchanges
  whole messages. Shutdown releases the lane first, cancels the receiver,
  serves what it already took and completes what was accepted. The common
  transport protocol, never a per-language wire format.

Link message sessions retain the incoming HTTP version and method. H1/H2
WebSockets and H3 WebTransport reliable bidi streams use the same slot record
format and internal 101 acknowledgment; Edge owns the public handshake.
Each WebTransport bidi stream is an independent application message session,
not a shared HTTP request body. No QUIC listener or H3 dependency is needed in
the runner. Datagram, uni and server-initiated streams are outside this API.

Each message request carries `orbit::SessionShutdown`, a read-only view of the
ingress generation's cancellation. Owners use it to begin their cooperative
protocol close while the ingress retains and joins accepted work. Closing lane
admission alone is not a shutdown signal to an idle owner. The capability adds
no deadline or authority to cancel guest tasks or another service's generation.

The Broadcast adapter applies header/session admission to linked WebSockets
and its existing first-record authentication to linked WebTransport streams.
A broadcast acceptance that cannot enter the caller's bounded inbox resets
the response direction instead of starting an unreachable session.

Both call the same routed service, so an application never learns which door
a request came through.

Ingress dependencies are bound by the lifecycle registry, not resolved inside
the serving loop. The routed application is required before a serving generation
starts, before any socket or Orbit lane opens. The master registers these ingress
components but serves nothing; its unavailable generations bind no dependencies.
Socket proxy trust keeps its empty default when absent. Orbit's owner session
adapter and worker incarnation remain optional, retaining the refusal for an
unsupported WebSocket and the standalone first-incarnation value.

An edge reaches Orbit ingress by declaring the same `fleet_name` and the
runner's application `name`. The destination fleet is independent of the
edge's own fleet. No raw SHM path or cross-user selector is required or
supported. Transport addressing does not change runtime process-family
ownership or permit independent masters to share their coordination state.

Orbit Link management addresses this runner by `fleet_name` and its advertised
Link name, then selects a hosted application by runtime and application name.
`launch::register_doors` installs the common management bridge for Orbit
launches. Custom ingress compositions may register it once explicitly. It
uses the existing application manager:
standalone applies the command locally, while a supervised runner publishes it
to its own worker fleet. Start, Stop, and Restart change the hosted application,
not the Rust process or Link lane. The runner reports complete application
snapshots when it joins Link, when Edge asks for status, and after application
lifecycle events. Edge keeps Link connectivity separate from application state.

The Link lane is withdrawn before accepted exchanges drain, but its kernel
hold lasts through that drain. Worker death resets only that PID/incarnation's
stream sides. A streamed response waits for either its next body frame or the
peer's confirmed death on the existing stream readiness path, so a quiet SSE
does not wait for another application message to notice its owner is gone.
Before the reply head, both `Application::call` and collection of an exact-size
buffered body also wait on the response writer's reader-departure signal.
Caller cancellation drops that exchange's pending future, releasing its admission
place and its `max_in_flight` entry. An already cancelled caller does not enter
the handler. Request-body FIN is not cancellation. This is separate from ingress
shutdown, which continues to drain accepted work; it adds no execution deadline
or authority to terminate a foreign execution thread.

If an HTTP reply head cannot enter the caller's bounded inbox, the runner
resets that exchange's response direction, even if its buffered body already
finished. Edge observes RESET while waiting for the head and completes the
request as a gateway failure instead of retaining an unanswered correlation.
This signals the existing exchange; it adds no retry queue or timeout.

The ingress also owns `LinkJoined`, `EdgeConnected`, `EdgeDisconnected` and
`EdgeShutdownInitiated` observations. Language runners subscribe to these
existing local events when needed; they do not copy directory watching or
define a language-specific connectivity lifecycle. Execution engines implement
`ApplicationController` and emit `ApplicationStarted`, `ApplicationStopped`
and `ApplicationFailed` only for real native transitions. This boundary does
not require Python, PHP or a future embedded JavaScript engine to share how
they boot or drain.

A received shutdown notice also becomes the local application event
`claviron.edge.shutdown.initiated.v1` with `edge`, `fleet` and `application`.
Like a departure, it is delivered only in the receiving process, without
republishing it into the fleet ring. This observation does not change the
runner's `edge_shutdown_policy`: `continue` is the default; `shutdown` opts
into stopping the runner. Guest subscriptions provide logging or their own
application behavior, separately from this runtime policy.

## Workers

With `supervisor`, the shell runs the application as a master and `workers`
workers, the way Claviron's own server does: the master serves nothing and
boots no owner, each worker boots its own and joins the same fleet under the
same name. Without it — or with `workers` unset — one standalone process
serves. The supervisor's restart, backoff and liveness policy is its own
section, passed through unchanged; the shell adds none.

An owner states whether it can run in several processes. One that cannot
refuses `workers` at launch, rather than the shell deciding for it.

## Source watching

Native notifications only: one pending wake, no polling, no debounce timer.
The watcher reports *sources changed*; what that does is the owner's:

- a runner whose owner lives once per process (Python) asks the master for a
  rolling worker restart, and so needs two or more workers;
- a runner whose owner restarts in place (PHP's application manager) restarts
  that application, which drains its own generation.

Development only; a production build carries no watcher.

## What stays out

- **Logging.** Claviron's bootstrap already registers the pipeline from
  `logging.yaml`; a runner adds only its language's door (a Monolog handler,
  a `logging.Handler`), which is language code.
- **Language surface.** The `claviron` Python module, PHP functions,
  framework integrations.

## Order of work

1. This crate from the more settled pieces of each: socket ingress, TLS,
   admission and upgrades from the PHP runner; proxy trust (into `http`,
   where the edge's weaker resolver can converge on it), workers and the
   rolling-restart watch from the Python runner; Orbit ingress from both.
2. The Python runner moves onto it.
3. The PHP runner moves onto it.

Each step leaves the runner's configuration file readable as before, or says
in its own changelog what moved.
