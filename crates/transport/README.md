# claviron-transport

Socket acceptance and HTTP connection driving, without host selection.

A transport accepts a connection, drives its protocol and hands each request
to a service. It knows no virtual host, route or backend: whoever supplies the
service decides which host a connection belongs to and what happens to a
request. `claviron-edge` supplies one by resolving a virtual host; a single
application can supply itself.

The crate is being separated from `claviron-edge`. HTTP/1.1 and HTTP/2
over TCP are complete here, cleartext and TLS: `tcp::plain::Single` and
`tcp::secure::Single` serve a listener with one handler and no host table.
HTTP/3 follows as its host selection moves behind a contract this crate
declares. See
[ARCHITECTURE.md](ARCHITECTURE.md) for what is here today.

## Features

| Feature | Enables |
| --- | --- |
| `tcp` | TCP bind groups, accept recovery, socket I/O and Hyper HTTP/1.1 settings. |
| `tls` | TLS accept, ALPN selection and HTTP/2 over TCP. |
| `quic` | HTTP/3 connection-closure classification, stream drain and session stream routing. |
| `webtransport` | Application-facing WebTransport sessions over the Quinn driver. |
| `websocket` | HTTP/1.1 connection upgrades. |
| `geoip` | A connection's country from its peer address. |
| `metrics` | Accepted-connection counters. |

No feature is on by default, and none opens a socket by being selected.
