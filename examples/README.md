# Examples

Application examples built on Claviron.

| Example | What it demonstrates | Connection |
| --- | --- | --- |
| [Edge](edge-server/) | HTTP ingress, host routing and static assets | Public HTTP listener |
| [FastAPI](fastapi-app/) | Python/ASGI through Gufi, with WebSockets | Separate process connected to Edge through Orbit Link |
| [Octane](octane-app/README.md) | Laravel through Lumenize, with SQLite and native broadcasting | Separate process connected to Edge through Orbit Link |
| [Orbit](orbit-app/README.md) | React/Vite and Hono on Deno, Node or Bun | Separate process connected to Edge through Orbit Link |
| [Lua](lua-app/README.md) | Lua request handling and streaming | Embedded runtime |
| [Bench](bench-app/README.md) | Native Rust transport benchmarks | Orbit Link, pooled UDS/h2c and TCP/h2c to the same runner |

## Run

These are independent application projects, not Cargo example targets;
`cargo run --example` does not launch them. Each Rust application has its own
workspace. Use the scripts below.

Use [examples.sh](scripts/examples.sh) from the repository root to manage all
three applications together. Edge is always included:

```sh
examples/scripts/examples.sh start all
examples/scripts/examples.sh status all
examples/scripts/examples.sh stop all
```

To select one application, use `fastapi`, `octane` or `orbit`; names with `-app`
are also accepted. An action and target are required. Extra launcher options
are forwarded:

```sh
examples/scripts/examples.sh start fastapi --no-build
```

Open [examples.code-workspace](examples.code-workspace) for the application examples,
or open an application folder on its own.


