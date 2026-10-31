---
name: claviron-routing
description: Compose Claviron Edge or Runner routes, Routable services, route directives, and incremental Rust interception. Use for request ownership and HTTP policy changes, not wire-protocol driver internals.
---

# Claviron routing

Establish the current response owner and serving composition before editing a
route. Verify the consuming application's facade features and actual registration;
a route implementation alone does not mean the route is exposed.

## Pick the composition

| Need | Contract |
| --- | --- |
| Multiple vhosts and configured application owners | Edge |
| One application behind HTTP, a socket upstream, or Orbit Link | Runner |
| Service contributes routes to selected Edge hosts | `claviron::edge::Routable` and `register_routable` |
| Host-independent application routes | `claviron::http::routing::Routable` |

For Edge, `register_app` and matched routables merge Rust contributions with the
configured pipeline. `register_router` and `register_other` install exclusive
owners; they are not interchangeable ways to add an intercept.

## Intercept without changing ownership accidentally

- Select canonical vhost keys at boot/reload. Public hostname aliases are resolved
  by Edge, not rescanned by a provider on every request.
- Build routes from the registered shared service. Keep path preparation and
  configuration parsing out of dispatch.
- Preserve the original method, URI, query, and body when delegating. Do not
  consume a body and then assume the original owner can read it again.
- Use the composition's unhandled/delegation contract. A native handler's 404 or
  failure is not permission to replay an accepted request into another owner.
- In a host-wide Edge layer, the prepared `RouteLayerContext.downstream` calls
  the selected configured owner directly, avoiding recursion through intercepts.
  In Runner, use its registered `Owner` and routing helpers.

Keep native paths explicit so an existing application can remain the fallback.
Do not add a second router match or silently enable a PHP/upstream fallback to
hide a broken native handler.

## Preserve policy

Host admission applies to configured plans and both RustRouter variants. Authentication, CSRF, and application
permissions must be composed for intercepted routes; an external framework's
middleware is not inherited automatically.

Use `RouteBody`, `RouteResponse`, and trusted request extensions at shared
boundaries. Native connection facts are not client-supplied headers. H1 upgrade,
H2 CONNECT, and WebTransport need their declared transport permission and owner.

Custom route directives belong in `Routable::route_extensions`; compile them with
the host snapshot rather than interpreting configuration on each request.

## Source map

Start with `crates/edge/README.md`, `crates/runner/README.md`, and
`crates/http/README.md`. For exact contracts inspect
`crates/edge/src/pipeline/routable.rs` and the relevant configured/Other or Runner
implementation. Use local architecture documents when present; public usage
must also be understandable from the exported API and README.
