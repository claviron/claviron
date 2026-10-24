# claviron-auth

Schema-independent identity and authentication contracts for Claviron.

Applications supply their own user model and lookup policy. This crate gives
native services a common identity interface, successful-auth lifecycle events,
an optional CSRF primitive, and an optional adapter for Orbit's live sessions.
It does not own an application's users table, login controller, password
hashing, role/permission storage, or framework guard configuration.

Applications normally use `claviron::auth` through the root crate's `auth`
feature. That facade selection includes CSRF; composing it with `orbit` also
enables the Orbit adapter. The implementation crate itself has no default
features.

Application, host and session ownership follow
[APPLICATION_FIRST](../../docs/design/APPLICATION_FIRST.md).

## Identity, not a second user model

`Authenticated` describes an application's authenticated identity:

- `auth_subject`: its stable, externally usable subject.
- `auth_identifier`: its username or other application identifier.
- `auth_display_name`: a presentation label, defaulting to the identifier.
- `auth_local_id`: an optional numeric database identity.
- `extra_claims`: application-supplied structured claims.
- `as_any`: typed downcasting when an integration needs the concrete model.

`AnyAuthenticated` is `Arc<dyn Authenticated>`. Adapting an existing model to
this interface does not authenticate it again or create another session. A
local database ID is not a substitute for the public subject.

`UserProvider` resolves these identities. `find_by_subject` is required;
identifier and local-ID lookups are optional and return no match by default.
Consumers can use the provider without depending on the host's model:

```rust
use claviron::auth::{AnyAuthenticated, UserProvider, UserProviderError};

async fn find_user(
    users: &dyn UserProvider,
    subject: &str,
) -> Result<Option<AnyAuthenticated>, UserProviderError> {
    users.find_by_subject(subject).await
}
```

The host registers its shared concrete provider and binds `dyn UserProvider`
in Claviron's [runtime registry](../runtime/README.md). The ordinary binding is
one provider per registry, not a separate provider for every guard. Applications
needing distinct identity realms must model that distinction explicitly rather
than repeatedly replacing the same binding.

## Service admission

`access::{PeerIdentity, AccessTarget, AccessPolicy}` is available independently
of the `users` and `authorization` features. An adapter supplies peer certificate
facts only after transport validation and names the actual service and operation.
The host implements policy and binds it in the application registry; no default
policy, database lookup or role model is installed.

The policy returns `Ok(None)` for peer-only access, `Ok(Some(actor))` for access
with an application subject, or a safe `AccessError`. Consumers requiring a user
reject peer-only grants. A connection adapter may admit `connect` once and keep
the actor for that connection. This service admission does not authorize every
operation: user-management scope and current record-level authorization remain
the manager's responsibility.

CNs and URI SANs are identity candidates, not permissions or automatic bindings.
The host owns trust-domain restrictions and any mapping to a user. The issuer DN
is a display name, not proof of a particular trust anchor. Credential DER is
public certificate material, provided for host-owned pinning; private keys are
never carried by this contract. Parsing peer facts alone does not authenticate.

## User management contracts

The opt-in capabilities are independent, including through the Claviron facade:

- `users` exposes `users::UserManager` for user CRUD, without role/permission
  or assignment methods.
- `authorization` exposes `authorization::AuthorizationManager` for role and
  permission CRUD and direct assignment management. Subject references are
  opaque; no user CRUD provider is required.

Select either feature or both. Neither implies the other, and neither installs
a management server. `UserProvider` remains the separate identity lookup contract.

Applications implement the selected management capabilities against their
existing data. Records have opaque public references, revision tokens, and
application-defined JSON objects: no email, username, user type, administrator
flag, database key format, or table layout is required. Managed records need
not represent authenticated users.

The implementation provides JSON Schema 2020-12, independent create/update
forms, optional declarative UI metadata, supported operations, and safe field
validation errors. Schemas may be prepared during build or boot. They describe
the exposed management view, not database columns discovered by reflection.
A transport can carry them to native or web clients without owning field
semantics. This module supplies neither a renderer nor a schema validator.

The host supplies the trusted caller identity. Every method must check current
authorization and the implementation's management scope, even for direct record
references and related role/permission changes. Capabilities and hidden UI
controls do not enforce access. Optional writes fail with `Unsupported` unless
implemented. Scoped user management does not grant authority to modify shared
roles that affect users outside that scope.

Updates carry record and schema revisions with sparse top-level replacements:
omission leaves a field unchanged; explicit null is validated as a value;
arrays and objects replace the whole field. The application validates and
checks revisions at the mutation boundary, rejects noneditable or unknown
fields, and never returns stored credentials. Direct assignment sets have
independent revisions and can be atomically replaced; a provider must not
silently drop hidden assignments from a supposedly complete set.

## Successful authentication events

`AuthEvent` carries `Login`, `Logout`, and `Registered` facts. The host emits
them only after the corresponding state transition succeeds. Login/logout
events identify the guard and subject; optional context records already-known
username, display name, timestamp, peer IP, and browser/device identity.

These are snapshots, not a live authorization decision. Credentials, raw
cookies, and ORM models do not cross this event boundary. A consumer owns its
mail, audit, queue, or framework-event behavior. Implementing `Authenticated`
does not emit events automatically.

## CSRF primitive

The `csrf` feature exposes `Csrf`: HMAC-signed double-submit tokens bound to a
caller-supplied scope. The caller provides a dedicated, domain-separated
32-byte key. `issue(scope)` creates a token; `verify(token, scope)` checks its
signature for that exact scope. `None` represents the guest scope without
requiring a persistent guest session.

The conventional names are `Csrf::COOKIE` (`XSRF-TOKEN`) and `Csrf::HEADER`
(`x-xsrf-token`). This primitive does not write cookies, choose their attributes,
compare the request's cookie and submitted value, or decide which routes need
CSRF protection. Those are ingress/session responsibilities. Scope changes at
login or logout must be reflected by that owner.

## Orbit-backed live sessions

The `orbit` feature adapts [Orbitive](https://crates.io/crates/orbitive) auth
primitives through `OrbitAuth<P>` and `TokenAuthenticator`. The host supplies
the authority, validation policy, prepared `FleetAuth` state, shared
`UserProvider`, and validation-cache duration. This crate neither joins a fleet
nor invents a session storage policy on the application's behalf.

`validate` verifies a token and resolves its live state into `VerifiedSession`.
`authenticate` also resolves the application user and checks that its subject
matches the validated principal. Refresh-purpose tokens are not accepted as
ordinary authenticated sessions.

`VerifiedSession::check(now)` rechecks live state and expiry using a trusted
Unix timestamp. A validation-cache hit does not bypass these checks. After an
asynchronous user lookup, admission owners must recheck before admitting work;
long-lived sockets should retain the verified session and recheck at their
own admission/delivery boundaries. This is not a promise of idle push
revocation without an integration that observes it.

Multiple validators may share the same users and fleet state while enforcing
different audience, purpose, or capability policies. Sharing storage does not
make one validator's decision valid under another policy.

## Related crates

- [Runtime](../runtime/README.md): typed service bindings and local events.
- [Orbit](../orbit/README.md): Claviron's same-host fleet integration.
- [Laravel](https://github.com/claviron/lumenize): opt-in framework auth/session integration.
- [HTTP](../http/README.md): request context; application ingress owns admission.

## Status and license

Part of Claviron's alpha development series; APIs and configuration may change.

Licensed under either [MIT](https://github.com/claviron/claviron/blob/main/LICENSE-MIT)
or [Apache-2.0](https://github.com/claviron/claviron/blob/main/LICENSE-APACHE), at your option.
