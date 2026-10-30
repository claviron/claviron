# An application-shaped pipeline

Design identifier: `BYPASS_PIPELINE`.

This is the serving-pipeline application of
[APPLICATION_FIRST](APPLICATION_FIRST.md).

If your repository has both `app/*.php` and `src/main.rs`, routing should help
those parts work together. It should not force you to put another server between
them, or rewrite the application before Rust can handle its first request.

Claviron is an edge, but its pipeline is built around the application it serves.
TLS, host admission and HTTP transport belong to the edge. Which application
owns a path, and which parts you choose to implement in Rust, need not be fixed
for the whole host or for the lifetime of the project.

## Start with what already runs

You can keep an existing application as the main request owner and add native
Rust routes where they are useful. `Routable` describes application routing;
interceptors let selected paths be handled before the configured fallback.
Unmigrated paths can continue to reach the existing application.

This makes a gradual Rust migration possible, but migration is not required.
Keeping PHP for presentation and Rust for a particular service can be the
finished design. An application may also run in a separate process reached
through [Orbit Link](ORBIT_LINK.md) rather than being embedded.

These are explicit bindings, not automatic discovery of every file in your
repository. The application and its host configuration declare what is served.

## Bypass is a choice of dispatch, not an escape from admission

Not every host needs the merged router. The application registry supports
different composition shapes:

| Shape | What you choose |
| --- | --- |
| Configured / merged routing | Compose configured owners, native routes and the applicable router layers. |
| An application-owned router | Supply the router yourself without the merged Claviron router stack. |
| Bypass | Dispatch directly to a native `BypassHandler` or a built-in backend without an Axum router. |

The choice is per host, not a server-wide mode. A native resource service and a
mixed application can live behind the same edge without taking the same route
through it.

Bypass remains behind shared host admission: accepted authority and transport,
deny/access rules and the narrowed HTTP policy still apply. Request admission,
head timing and response-body guards are not Axum-only facilities. Skipping the
router must not make an application invisible to exchange accounting either.

## Keep the boundary useful

Native transports adapt their request bodies to `RouteBody`; bypass handlers
return `RouteResponse`. The bypass dispatch chain does not require Axum types.
That lets a backend participate without pretending to be a router middleware.

On native TCP, the host-dispatch future stays inline for bypass requests. The
handler returns its erased `RouteFuture`; the transport does not add a second
future box. Axum erases its own selected dispatch future separately, so its
concrete state cannot enlarge bypass dispatch. Admission and head timing retain
the same handler future until completion or cancellation, while response bodies
keep their independent streaming lifetime.

Conversely, middleware attached only to a router does not run merely because a
bypass handler serves the same host. Framework-specific validation, CSRF and
response formatting belong to the application/framework composition, not an
implicit promise that every dispatch shape executes every layer.

The goal is a pipeline suited to your application, not a claim that bypass is
always faster or should replace routing everywhere.

## Follow the implementation

- [Edge routing and composition guide](../../crates/edge/README.md).
- [Routable bindings](../../crates/edge/src/pipeline/routable.rs).
- [Host application registry](../../crates/edge/src/pipeline/apps.rs).
- [Native bypass dispatch](../../crates/edge/src/pipeline/bypass.rs).
- [Shared host admission and dispatch](../../crates/edge/src/service/host.rs).

Use `BYPASS_PIPELINE` as the search key for this application-first composition.
