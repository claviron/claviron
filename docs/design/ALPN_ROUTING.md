# ALPN Routing

Status: the profile registry and handshake-only profiles are implemented
(`AlpnProfile`, `AlpnProfiles`, `acme-tls/1` on a listener). Session profiles,
enabled per host, are a proposal.

A TLS client names the protocol it wants to speak in its ClientHello. That
choice selects a handshake before any request exists, which makes it the one
place where a connection can be given terms different from its host's
without a second host or a second listener. This note says where that branch
sits, who owns it, and what it may and may not change.

ALPN negotiates; it never authenticates. A client chooses the protocol it
offers, so a protocol id selects which handshake and which services a
connection gets, never whom the server trusts.

## Where it sits

```mermaid
flowchart TD
    Hello[ClientHello: SNI and offered ALPN] --> Listener{Listener's handshake-only profile answers?}
    Listener -->|yes| Only[Profile's ServerConfig] --> Close[Handshake completes, connection ends]
    Listener -->|no, or profile declines| Host[Select host by SNI]
    Host --> Session{Host enables a session profile for an offered id?}
    Session -->|yes| SessionConfig[Profile ServerConfig, created for this host]
    Session -->|no| HostConfig[Host ServerConfig]
    SessionConfig --> Negotiated[Handshake; negotiated id in ConnCtx]
    HostConfig --> Negotiated
    Negotiated --> Plan{Pipeline for listener, host and profile}
    Plan -->|session profile| ProfilePlan[Profile's prepared pipeline: its services only]
    Plan -->|ordinary protocol| HostPlan[Host's prepared pipeline]
```

Everything after the handshake is the ordinary connection driver and
[Hot Pipeline](HOT_PIPELINE.md). What ALPN adds is which `ServerConfig` the
handshake uses and which prepared pipeline the connection is given. Both are
chosen once, at acceptance; no request checks a protocol id.

## Three shapes

| Shape | After the handshake | Example | Pipeline |
| --- | --- | --- | --- |
| Handshake only | The connection ends | `acme-tls/1` (RFC 8737) | None |
| Session | HTTP on the same host | A registration service | The profile's prepared pipeline |
| Opaque protocol | A non-HTTP byte stream | None yet | None; needs its own contract |

A handshake-only profile owns the whole handshake, with its own certificate,
and ends the connection. It is independent of virtual hosts, consulted
before host selection, and is enabled on a listener. It narrows itself: it
answers only names its own state holds, such as an ACME order's active
challenge, and declines every other hello. A session profile speaks HTTP to
services registered for it, under the host's certificate, and is enabled per
host, since nothing in the profile says which host should offer it. An
opaque protocol would bypass the pipeline entirely and needs a contract of
its own, documented apart from it; nothing needs one today.

## Ownership

Code registers a profile; configuration enables it. Claviron's TLS and Edge
crates name no protocol id: they provide the registry and the selection.

- The package that speaks a protocol registers its id, the handshake it
  requires and, for a session profile, the services the connection may reach.
  One id has one profile in a process; a second registration is refused at
  boot.
- A handshake-only profile is enabled in code, for the listener its clients
  reach. An application enables ACME's `acme-tls/1` once with
  `register_local(state, listener)`, naming the listener the CA connects to;
  first issuance and every renewal are new orders on that listener, and no
  host or configuration file lists the id.
- A host enables a session profile by its id. An id no profile registered is
  a configuration error, not a no-op, and a host that does not list an id
  never selects its profile. Which hosts list it is setup, in the host
  declaration, not the registering package's configuration.
- Neither id is known to the TLS or Edge crates.

```yaml
grpc.example.com:
  tls:
    client_auth: required          # ordinary h2: client certificates
    alpn: [example-registration/1] # a session profile registered by its package
```

A profile can relax what the handshake demands, such as a client
certificate, only together with narrowing what the connection reaches.
Relaxing it from configuration alone would let a connection without a
certificate reach every service of its host, so a session profile's handshake
terms are the profile's, and enabling it is the host's.

## Server configs

A session profile's `ServerConfig` is created for each host that enables it,
not derived by overriding the host's own. Its inputs are the host's
certificate source and the profile's handshake requirements; its ALPN list is
the profile's ids alone. Configs are created when the host is prepared, at
boot or reload, and kept with the host's TLS view like its ordinary config.
Certificate rotation reaches both through the same certificate resolver.

## Pipelines

Each listener and host already has its own prepared pipeline. A session
profile adds one per listener, host and profile, built from the services the
profile registered and the host's admission policy. A connection that
negotiated the profile is dispatched to that pipeline and can reach nothing
else on the host; an ordinary connection cannot reach the profile's services.
The services authenticate their callers themselves; for registration that is
the application's `RegistrationGuard`.

## Open questions

- How a session profile declares its services: a prepared pipeline of its own
  from a backend the package registers, or host routes the profile selects.
- QUIC: HTTP/3 negotiates `h3`; a session profile over QUIC would need its own
  id there and is not proposed.
- Clients: a session profile is for clients that set ALPN themselves, such as
  native tools. Browsers cannot offer a custom id.
