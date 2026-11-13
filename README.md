<p align="center">
  <img src="assets/logo.svg" width="280" alt="Claviron logo">
</p>

# Claviron

**Retire middlemen. Let the others go!**

Claviron is an application-first toolkit for building and running services.
Run Rust alongside your existing applications. Keep them together as a modular
monolith or deploy them as independent services.

Build an Edge that accepts public traffic directly. It handles TLS, routing,
static files, and application dispatch in one process, so an Nginx proxy hop
is optional. Your application controls each layer, from listeners and access
rules to handlers and response policies. Enable what you need. An omitted layer
adds no work per request.

## Move to Rust without a full rewrite

Keep your application behind Claviron and add a **Rust intercept** for the first
route you want to move. Claviron runs the Rust handler first. Requests it leaves
unhandled continue to the existing application. You can move one API, a hot path,
or a security check at a time while keeping the public URL and host policies.

Start with a route that benefits from Rust's type and memory safety, or one
where you want to reduce latency or CPU use. Claviron can run the new Rust code
alongside your PHP, Python, JavaScript, Go, Java, or C/C++ application. Each part
can keep its existing runtime and move on its own schedule.

## Build your own Edge

Add `claviron` with the `edge` feature and Tokio with `macros` and
`rt-multi-thread`. Define your listeners and hosts in `config/`.

```rust
use claviron::{Result, Server, Workers};

#[tokio::main]
async fn main() -> Result<()> {
    Server::new()
        .config_dir("config")
        .workers(Workers::Available)
        .serve()
        .await
}
```

Start with the [Edge example](https://github.com/claviron/examples/tree/main/edge-server)
or the [Edge configuration guide](crates/edge/README.md).

## Bring your application

- **Rust** — Serve an Axum or Warp application with [Claviron Runner](https://github.com/claviron/runner), or add [reusable backends and layers](docs/design/EDGE_BACKENDS.md) to your Edge.
- **PHP** — [Embed PHP or use FastCGI](https://github.com/claviron/php). Run Laravel with [Lumenize](https://github.com/claviron/lumenize) or Symfony with [Skene](https://github.com/claviron/skene).
- **Python** — Run FastAPI and other ASGI applications with [Gufi](https://github.com/claviron/gufi).
- **JavaScript, Go, Java, and C/C++** — Use the native bindings to connect an application to Claviron over [Orbit Link](docs/design/ORBIT_LINK.md). [Browse runnable examples](https://github.com/claviron/examples).

## Put boundaries where they belong

Let your Edge apply CORS and verify CSRF before a request reaches PHP.
Claviron includes CORS policy. Register a route extension for CSRF and give
it the application's session and token verifier. The Edge can then enforce
the check on the routes you select.

```yaml
routes:
  /account:
    csrf: true
```

[Register a route extension](crates/edge/README.md#route-extensions)

## Performance

In this Linux HTTP/1.1 plaintext benchmark, Claviron delivered **2.74× as
many responses per second** using **66% less CPU per response** than Caddy.
Both met the same SLA with 128 loopback connections and 1,280 target
outstanding requests.

| Server | Responses/s | Server CPU/response |
| --- | ---: | ---: |
| Caddy 2.11.8 | 287,280 | 75.36 µs |
| Claviron with Axum | 788,552 | 25.49 µs |

[Benchmark setup and full results](https://git.claviron.dev/claviron/bench-http/issues/5)

## Run and manage your services

Start with one process and add supervised workers as traffic grows. Serve
HTTP/1.1, HTTP/2, and HTTP/3, and host services with WebSockets or WebTransport.
Use [Claviron for Apple](https://github.com/claviron/apple) to inspect running
applications, workers, and tasks.

<p><a href="https://github.com/claviron/apple"><img src="assets/gui-overview.png" width="560" alt="Claviron management app showing a fleet overview"></a></p>

[Crates.io](https://crates.io/crates/claviron) · [API documentation](https://docs.rs/claviron) · [Contributing](CONTRIBUTING.md) · [Security](SECURITY.md)

Licensed under [MIT](LICENSE-MIT) or [Apache 2.0](LICENSE-APACHE).
