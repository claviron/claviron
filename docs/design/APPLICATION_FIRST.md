# Applications define what is served

Design identifier: `APPLICATION_FIRST`.

Claviron is an application server. An application defines its work, services
and dependencies; hosting binds entry points to that application. A hostname
describes where and how parts of it are served.

The application can combine native Rust handlers, embedded or linked runtimes,
shared providers and background work. These parts keep their own execution and
lifecycle contracts. The runtime's managed `Application` type describes a
particular workload; the principle here concerns the wider application being
assembled and served.

## One application, several presentations

One application can use several hostnames with a different composition at each:

| Entry point | Composition |
| --- | --- |
| `api.example.test` | Selected API routes and their application owner. |
| `admin.example.test` | Management routes with their own access policy. |
| `assets.example.test` | Static files and application-provided resources. |

These bindings can use the same application services and data. Each host still
has its own routing and admission policy. Conversely, one host can compose
several application components: native handlers for some paths, a language
runtime for others and resource serving alongside them.

The application chooses the components and shared providers. The host binding
selects their exposure. Adding another hostname does not inherently require
another implementation of the application or another copy of its state.

[HOST_PIPELINE](HOST_PIPELINE.md) describes the serving choices: a native
host pipeline, an application-owned router or direct bypass dispatch. Those are ways
to present application behavior through the edge.

## Routable contributions and interception

`Routable` lets a registered application provider contribute Rust routes to
selected hosts. Host matching happens while preparing the serving snapshot;
handlers can retain the provider's shared dependencies. One provider can
contribute to several hosts, each with its own composition.

Interception lets those contributions handle selected requests while the
configured application owner continues serving the remaining paths. A
`Routable` can also supply route extensions and explicitly opt into an Axum
application layer with `layer_routes`. Returning `None` installs no adapter.
The prepared downstream handle delegates to the selected native owner without
re-entering the Rust routes or selecting the prefix again.

This supports applications built from several cooperating parts, including
gradual replacement of individual handlers. Route ownership and delegation are
explicit. Standalone and bypass bindings select their own dispatch composition;
router-specific middleware runs where that composition installs it.

## Compose before serving

Application providers supply prepared dependencies and handlers. Host
composition binds them to configured routes and publishes the serving snapshot.
Requests use those bindings rather than rebuilding the application.

A host can have its own prepared router while retaining shared service handles.
Sharing application state is an explicit composition choice; matching hostnames
or loading the same source file is not a sharing mechanism.

[LIFETIME_POLICY](LIFETIME_POLICY.md) defines when preparation and publication
happen. Application work retains its own graceful lifetime under
[GRACEFUL_OWNERSHIP](GRACEFUL_OWNERSHIP.md).

## Identity, session and credential

The application's identity and session policies are selected independently of
the hostname that exposes an endpoint. Several entry points can intentionally
use the same user provider and fleet session state while applying different
admission policies.

For fleet-session authentication, the token carries a binding to a session,
including its ID. The live session state is held in the fleet table. The
credential's validity and the current session state are both checked; revoking
the session prevents its token from authorizing later work.

For example, an application can expose login through a session service at
`session.example.test`, while its API and other application functions are served
through separate entry points or processes:

1. The session service authenticates the user, creates a fleet session and
   issues a token bound to that session.
2. A client presents the token to an application server. That server verifies
   it against its configured policy and the shared live session state.
3. The application server uses the verified session and resolves the user when
   needed. Login stays with the session service; application work stays with
   the serving application.

Processes using the same fleet authentication state can perform this validation
directly, without calling the login service for every request. This sharing has
the [same-OS fleet boundary](FLEETAWARE.md); the token itself does not replicate
session state to another machine. Session creation and session consumption are
separate responsibilities, even when deployed in the same process.

The application chooses the accepted audience, purpose, capabilities and
credential carrier. Sharing a session table does not make every token valid
at every endpoint. Browser cookie scope and credential delivery remain part of
the entry-point policy.

The Auth adapter connects verified sessions to the application's `UserProvider`.
Role checks, permission checks and management scope remain application policy.
The [Auth API](../../crates/auth/README.md) describes how applications supply
these identity and management providers.

## Serving policy binds the client

Routing, virtual hosts and certificates define how an application is presented.
HTTP policies, origin checks and CORS define the permitted interactions with
that presentation. These bindings let the application expose the same services
through different entry points while controlling each entry point's use.

The binding is a contract the client must satisfy, enforced by the serving
runtime. It does not require application code and shared state to be duplicated
for every domain. A client supplies a hostname, origin or credential; those
inputs are checked against the selected entry point's policy. They do not grant
authority to replace an accepted binding or relax its requirements.

Application composition preserves accepted transport identities. A TLS
connection retains its selected host and application binding, and requests must
still pass the applicable host, protocol and access checks.
[HOST_PIPELINE](HOST_PIPELINE.md#connection-binding) defines that connection contract. An alias or
another hostname serving the same application does not authorize switching
hosts on an established TLS connection.

Certificates establish transport identities and any required client
authentication. A certificate covering several names grants no cross-host
routing authority. For browser clients, configured origin/CORS handling also
controls cross-origin interaction and response access. Origin and CORS are
distinct from credential authentication; a native client can supply its own
Origin header.

Configured edge exchange policy applies at shared host dispatch, including
bypass. Security implemented only in a particular router or application guard
remains part of that selected composition. Sharing an application does not
silently remove either boundary.

The useful separation is application behavior, its presentation through host
bindings, and the policy that admits each operation. Change the presentation
without implicitly changing the owners of application work or shared state.

## Follow the implementation

- [Provider route and host bindings](../../crates/edge/src/pipeline/routable.rs).
- [Native host compilation](../../crates/edge/src/pipeline/host/compile.rs).
- [Optional Rust application adapters](../../crates/edge/src/pipeline/app.rs).
- [Prepared host applications](../../crates/edge/src/pipeline/apps.rs).
- [Shared host admission](../../crates/edge/src/service/host.rs).
- [Configured exchange policies](../../crates/edge/src/service/exchange.rs) and
  [CORS handling](../../crates/edge/src/service/cors.rs).
- [Runner application composition](../../crates/runner/src/routing.rs).
- [Session handles](../../crates/auth/src/session.rs) and
  [token authentication](../../crates/auth/src/session/token.rs).

Use `APPLICATION_FIRST` for changes to the relationship between application
ownership, hosting composition and shared identity/session state.
