# Laravel application over Orbit

A Laravel application with its own Rust binary, hosted by
[Lumenize](https://crates.io/crates/lumenize) with its `runner` feature in persistent PHP Application mode.
A separate Claviron edge sends HTTP and Pusher WebSocket traffic to application
`octane-app` in fleet `octane` over [Orbit Link](https://crates.io/crates/orbitive).
The edge does not embed PHP; this application does not open a TCP or UDS listener.

## Prepare

Keep the `lumenize` checkout beside Claviron. Cargo consumes it through a local
path. Composer's `claviron/sapi` and `claviron/laravel` paths point to this
Claviron checkout. The optional benchmark development package currently requires
a sibling `bench` checkout at `../../../bench/packages/php`.

From this directory:

```sh
cp .env.example .env
# Replace the example PostgreSQL values with a dedicated local database.
"$PHP" /path/to/composer.phar install
"$PHP" artisan key:generate
npm install
npm run build
```

Use the workspace ZTS embed PHP build, not Homebrew PHP. Set `PHP` and
`PHP_CONFIG` to that installation, and `PHP_DIR` to its root for the ini paths
in `lumenize.yaml`. Keep generated keys, credentials and `.env` private.
Dependencies and generated assets are not copied from the original application.

Database examples use the `octane` schema. Before exercising Todo/database routes,
review and run the application's migrations against that dedicated database:

```sh
"$PHP" artisan migrate
```

This setup is explicit: the launch script does not install dependencies, create
databases or run migrations.

## Run behind the edge

From the Claviron root:

```sh
liberum/scripts/octane-app.sh
# In another terminal, unless the example edge is already running:
liberum/scripts/edge.sh
```

The application script supports `--release`, `--no-build` and an explicit
`WORKERS` override. Ctrl-C follows the runner's normal shutdown path.
`liberum/edge-server/config/vhost.yaml` supplies the `octane.test` route and
permits HTTP/1 WebSocket upgrades. It serves `/build` assets directly and sends
application requests, including Pusher sockets, over Orbit.

Resolve `octane.test` to loopback for browser use. The sample `.env` assumes an
edge listener on port 8080; keep `APP_URL` and `PUSHER_PORT` aligned with your
actual listener. The browser uses its page hostname and HTTP/HTTPS scheme for
the socket, so no separate broadcast hostname or TLS certificate is required.

```sh
curl --resolve octane.test:8080:127.0.0.1 http://octane.test:8080/up
curl --resolve octane.test:8080:127.0.0.1 http://octane.test:8080/_lumenize/rust
```

The front page demonstrates native broadcasts, Laravel notifications and client
whispers. Open two tabs to see delivery. `/_lumenize/php` and
`/_lumenize/rust` exercise PHP and Rust shared-cell access; `/session` shows
ordinary Laravel session behavior. Undeclared Rust paths continue to Laravel
through `application.php`.

These are local diagnostics, not a public deployment: the demo channel permits
subscription without a user login, and `/runtime` reports PHP server data. Some
invocation examples require providers not registered in this sample; they are
not a claim that every Claviron capability is active.

## Optional shared benchmarks

`liberum/scripts/octane-app.sh --bench --release` sets `LIBERUM_BENCH=1`.
The local `liberum/bench` development package supplies `/bench/plain`,
`/bench/json` and `/bench/echo` through the existing Laravel application, not a
native interception shortcut. Ordinary launches leave those routes disabled.
Keep results separate from transport or PHP lifecycle correctness checks.

The Rust package is an independent workspace with `publish = false`. The entire
`liberum/` tree is excluded from the root Claviron crate's registry package.
