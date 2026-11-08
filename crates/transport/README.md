# claviron-transport

Socket acceptance and HTTP connection driving, without host selection.

Transport owns the named `Listener` Runnable, socket acceptance, protocol
execution and drain. Edge and runner provide adapters to it; this crate has no
Edge dependency. `tcp::plain::Single`, `tcp::secure::Single` and `quic::Single`
serve one owner. `single::serve` composes named socket declarations for that
owner, while `orbit::OrbitDoor` serves it over Orbit Link.

TCP, Unix and QUIC share HTTP's listener declaration API. Under a supervisor,
the master binds Unix sockets once and workers inherit duplicate descriptors.
Worker restart retains that socket; recovering its FD from surviving workers
after master replacement is not implemented.

See [ARCHITECTURE.md](ARCHITECTURE.md) for the contract a host selector
implements and what lives where.

## Features

| Feature | Enables |
| --- | --- |
| `pipeline` | A site compiled by `claviron-pipeline` as a listener's owner. |
| `tcp` | TCP bind groups, accept recovery, socket I/O, HTTP/1.1 and cleartext HTTP/2 by prior knowledge; Unix sockets. |
| `tls` | TLS accept, ALPN selection and HTTP/2 over TCP. |
| `quic` | HTTP/3 over QUIC: listener, request and response framing, WebTransport session routing. |
| `webtransport` | WebTransport session driving for the supplied owner. |
| `orbit` | The door with no socket: a lane an edge reaches over Orbit shared memory. |
| `websocket` | HTTP/1.1 connection upgrades. |
| `websocket-session` | An owner's WebSocket spoken on the sockets, with whole messages like Link's. |
| `geoip` | A connection's country from its peer address. |
| `metrics` | Accepted-connection counters. |

No feature is on by default, and none opens a socket by being selected.

Shared descriptor declarations are sealed at the first worker spawn. Adding a
Unix bind or changing its socket settings requires a process-family restart;
policy reload on an existing bind reuses its descriptor. Retiring a listener
releases retained copies without closing active generation copies or unlinking
another process's path.
