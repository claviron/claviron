# claviron-transport

Socket acceptance and HTTP connection driving, without host selection.

A transport accepts a connection, drives its protocol and hands each request
to a service. What that service is belongs to how the listener is composed:
`tcp::plain::Single`, `tcp::secure::Single` and `quic::Single` give every
request to one owner with no host table, a plain handler or a site compiled by
`claviron-pipeline`; with the `edge` feature the crate borrows
`claviron-edge` and serves its virtual hosts through the same drivers.
`claviron-runner` composes them with its own application.

See [ARCHITECTURE.md](ARCHITECTURE.md) for the contract a host selector
implements and what lives where.

## Features

| Feature | Enables |
| --- | --- |
| `edge` | Borrow `claviron-edge` and serve its virtual hosts: `HttpServer`, `HttpsServer`, `H3Server`. |
| `tcp` | TCP bind groups, accept recovery, socket I/O, HTTP/1.1 and cleartext HTTP/2 by prior knowledge; Unix sockets. |
| `tls` | TLS accept, ALPN selection and HTTP/2 over TCP. |
| `quic` | HTTP/3 over QUIC: listener, request and response framing, WebTransport session routing. |
| `webtransport` | With `edge`, sessions for the handlers Edge binds. |
| `websocket` | HTTP/1.1 connection upgrades. |
| `geoip` | A connection's country from its peer address. |
| `metrics` | Accepted-connection counters. |

No feature is on by default, and none opens a socket by being selected.
