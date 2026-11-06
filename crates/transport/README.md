# claviron-transport

Socket acceptance and HTTP connection driving, without host selection.

A transport accepts a connection, drives its protocol and hands each request
to a service. It knows no virtual host, route or backend: whoever supplies the
service decides which host a connection belongs to and what happens to a
request. `claviron-edge` supplies one by resolving a virtual host; a single
application can supply itself.

The crate is being separated from `claviron-edge`. It holds the parts that
never needed a host; the listener owners and connection handlers follow as
their host selection moves behind a contract this crate declares. See
[ARCHITECTURE.md](ARCHITECTURE.md) for what is here today.

## Features

| Feature | Enables |
| --- | --- |
| `tcp` | TCP bind groups, accept recovery, socket I/O and Hyper HTTP/1.1 settings. |
| `quic` | HTTP/3 connection-closure classification, stream drain and session stream routing. |
| `webtransport` | Application-facing WebTransport sessions over the Quinn driver. |
| `geoip` | A connection's country from its peer address. |
| `metrics` | Accepted-connection counters. |

No feature is on by default, and none opens a socket by being selected.
