# Early arrival is not application permission

Design identifier: `EARLY_DATA_POLICY`.

0-RTT can let a resumed QUIC client send work before the handshake completes.
That is useful only when the application can tolerate the replay implications.
Transport support and permission to execute a particular request are therefore
two different decisions in Claviron.

## Two gates, not one switch

The QUIC listener's `enable_early_data` enables transport acceptance; it is off
by default. A host's `transport.h3.early_data` then decides which early requests
may reach its application. Setting host policy does not enable a listener or
turn on HTTP/3 by itself.

The policy values are:

| Policy | Early requests admitted |
| --- | --- |
| `allow_idempotent` (alias `idempotent`, default) | GET, HEAD and OPTIONS only. |
| `deny_all` (alias `deny`) | None. |
| `allow_all` (alias `allow`) | Any method, subject to the other admission checks. |

`allow_idempotent` is a historical name for that exact allow-list. It does not
mean all methods classified as idempotent by HTTP, and it does not prove that
your GET handler is free of side effects.

## Arrival belongs to the transport

The native transport supplies `RequestArrival`; an application header is not
evidence that a request arrived in 0-RTT. Shared host admission first validates
the request's authority and transport capability. A wrong host remains a 421
even if early-data policy would also refuse the method.

A disallowed early request receives **425 Too Early**. Marking an H1 or H2
request as early does not make it an admitted QUIC request. Established
requests are not filtered by this early-arrival method policy.

Client-authenticated QUIC uses its own TLS configuration with early data
disabled. This policy is not a way around completing client authentication.

## The application still owns replay safety

Claviron can refuse early work; it cannot infer the effects of a route from
its method. Enabling `allow_all` is an application decision, not a promise of
deduplication or exactly-once execution. Idempotency keys and durable operation
state remain the application's concern where required.

The setting governs ordinary HTTP request admission. It is not blanket
permission to establish every kind of native session in early data.

## Follow the implementation

- [Host transport policy](../../crates/http/src/config/transport.rs).
- [QUIC listener configuration](../../crates/http/src/config/yaml/quic.rs).
- [Transport arrival and host admission](../../crates/edge/src/service/host.rs).
- [QUIC TLS configurations](../../crates/edge/src/quic/tls.rs).
- [Authority-before-early-data regression checks](../../crates/edge/tests/service/host.rs).
- [Host binding](HOST_BINDING.md).
