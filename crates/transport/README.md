# claviron-transport

Socket acceptance and HTTP connection driving, without host selection.

A transport accepts a connection, drives its protocol and hands each request
to a service. What that service is belongs to how the listener is composed:
`tcp::plain::Single` and `tcp::secure::Single` give every request to one
handler with no host table; with the `edge` feature the crate borrows
`claviron-edge` and serves its virtual hosts through the same drivers.

HTTP/1.1 and HTTP/2 over TCP are host-free here, cleartext and TLS. HTTP/3 is
served for Edge's hosts; its host-free form follows. See
[ARCHITECTURE.md](ARCHITECTURE.md) for what is here today.

## Features

| Feature | Enables |
| --- | --- |
| `edge` | Borrow `claviron-edge` and serve its virtual hosts: `HttpServer`, `HttpsServer`, `H3Server`. |
| `tcp` | TCP bind groups, accept recovery, socket I/O and Hyper HTTP/1.1 settings. |
| `tls` | TLS accept, ALPN selection and HTTP/2 over TCP. |
| `quic` | HTTP/3 connection-closure classification, stream drain and session stream routing. |
| `webtransport` | WebTransport session stream routing; with `edge`, sessions for the handlers Edge binds. |
| `websocket` | HTTP/1.1 connection upgrades. |
| `geoip` | A connection's country from its peer address. |
| `metrics` | Accepted-connection counters. |

No feature is on by default, and none opens a socket by being selected.
