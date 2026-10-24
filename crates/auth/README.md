# claviron::auth

`claviron::auth` defines shared identity and management contracts for Claviron
integrations. Your application implements them using its own user model,
storage and policy. Consumers work through those contracts rather than depend
on your application's concrete types.

The crate also supplies ready-made fleet-session authentication and CSRF
mechanisms. You can use these alongside your own providers.

Application, host and session ownership follow
[APPLICATION_FIRST](../../docs/design/APPLICATION_FIRST.md).

## Features

Select capabilities through the `claviron` facade:

| Feature | What it provides |
| --- | --- |
| `auth` | User identity, user lookup, authentication events and CSRF tokens. |
| `users` | `auth::users::UserManager` for managing user records. Enables `auth`. |
| `authorization` | `auth::authorization::RolePermissionManager` for managing roles, permissions and assignments. Enables `auth`. |
| `auth` + `orbit` | `auth::session` for authenticating tokens against an existing fleet session table. |

`users` and `authorization` can be selected independently.

## Common contracts

`auth::role::{Role, RoleDefinition}` and
`auth::permission::{Permission, PermissionDefinition}` provide shared name
values. Define marker types in the application module that uses them and
convert their values with `Into`. Parsed names and typed definitions compare
by the same exact name, without a central enum. Membership and access decisions
remain the application's policy.

| Contract | What consumers receive | What your application implements or supplies |
| --- | --- | --- |
| `Authenticated` | An accepted identity with a stable subject and login/display identifiers. | The identity's representation and its mapping to application data. |
| `UserProvider` | Asynchronous identity lookup through `AnyAuthenticated`. | Subject lookup and any supported identifier/local-ID lookups. |
| `users::UserManager` | User records, schemas and management operations. | Record scope, caller authorization, validation and persistence. |
| `authorization::RolePermissionManager` | Roles, permissions and direct assignments. | Management of that data within the caller's allowed scope. |
| `session::token::TokenAuthenticator` | Token validation and optional user resolution. | The supplied `OrbitAuth` adapter for fleet sessions. |

The shared record, form, revision and error types describe the values exchanged
through these interfaces. They let integrations use an application provider
without prescribing its database schema. The managers can also be called
directly, independently of gRPC or a GUI.

### Identity and lookup

Implement `Authenticated` for the user identity your application exposes to
Claviron. `auth_subject()` supplies its stable public identifier;
`auth_identifier()` supplies its login or display identifier. Optional methods
provide a display name, a local numeric ID and application claims.
`AnyAuthenticated` is an `Arc<dyn Authenticated>` shared with consumers.

Implement `UserProvider` to find that identity. `find_by_subject()` is required;
lookup by login identifier or local numeric ID is optional. The provider owns
its database queries and caching. This lookup contract lets a consumer resolve
a user without depending on the application's user model.

With Claviron's runtime registry, register the application's shared provider and
bind it as `dyn UserProvider`. The traits can also be called directly.

### Management records and forms

Both managers describe, list and read records, with optional create, update and
delete operations. `RolePermissionManager` also reads and replaces user-role,
user-permission and role-permission assignments. Each call receives the trusted
authenticated caller.

The current contracts exchange `Record` values with a public reference, a
revision and JSON fields. The provider describes those fields through JSON
Schema and supplies separate create/update forms with optional JSON Forms UI
metadata. It validates submitted values when applying a change. Optional write
methods return `Unsupported` until implemented.

Updates carry the record and schema revisions. Only submitted fields change;
explicit null remains a submitted value. Assignment replacement supplies the
complete editable set, with an empty set clearing it.

These managers administer authorization data. Runtime checks such as
`has_role` and `has_permission` belong to the application's guard or policy.

## Downstream responsibilities

Your application supplies the user identity and provider, then implements the
management capabilities it exposes. It owns login and credential verification,
user storage and caching, runtime role/permission checks, and the authority to
create or change records. Manager implementations enforce validation and revision
checks when committing a mutation; a schema describes the input but does not
authorize it.

The integration selects its authentication policy, extracts credentials and
passes the accepted identity to consumers. Providers can be supplied directly
or bound in the runtime registry. Selecting a feature makes its contracts
available; the application still supplies and connects their implementations.

## Provided adapters and mechanisms

### Fleet-session authentication

`session::token::OrbitAuth` implements `TokenAuthenticator` using Orbitive's
existing authority and fleet-session state together with your `UserProvider`.

The session lives in the fleet's session table. Its token carries a session
binding, including the session ID, together with the user's subject, audience,
purpose and validity times. An application passes the token received from its
client to this adapter. Authentication checks both the credential and the
current state of the referenced session.

`auth::session` connects that check to Claviron's identity contracts. The
application supplies its `Authority`, validation policy,
`FleetAuth`, `UserProvider` and validation-cache duration to `auth::session::token::OrbitAuth::new()`.

- `validate(token, now)` returns a `VerifiedSession` after checking the token and
  its live session state.
- `authenticate(token, now)` additionally finds the user and returns an
  `AuthenticatedSession` containing the user and verified session.
- `VerifiedSession::check(now)` checks expiry and current fleet session state
  again, without another user lookup or token decode.

Use the check at later admission boundaries, such as accepting another message
on a long-lived connection. Supply trusted current Unix time. Login, token
issuance, session renewal and revocation use the application's Orbitive handles;
this adapter connects their result to Claviron consumers.

### CSRF tokens

`auth::csrf::Csrf` issues and verifies signed double-submit tokens. Supply a
dedicated 32-byte key and the session scope used by your application. Use the
same scope when issuing and verifying a token. `None` represents a guest scope.
The request handler owns reading the cookie/header and handling a failed check.

## Authentication events

`events::AuthEvent` represents successful login, logout and registration. Emit
it after the application completes that operation. Listeners can use the guard,
subject and optional login context for audit or application notifications.

## Further reading

[ARCHITECTURE.md](ARCHITECTURE.md) maps these contracts to their implementation
and describes the rules for changing this crate.

Licensed under [MIT](https://github.com/claviron/claviron/blob/main/LICENSE-MIT)
or [Apache-2.0](https://github.com/claviron/claviron/blob/main/LICENSE-APACHE).
