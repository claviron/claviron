# Laravel application over Orbit

A Laravel application with its own Rust binary, hosted by
[Lumenize](https://crates.io/crates/lumenize) with its `runner` feature in persistent PHP Application mode.
A separate Claviron edge sends HTTP and Pusher WebSocket traffic to application
`octane-app` in fleet `octane` over [Orbit Link](https://crates.io/crates/orbitive).
The edge does not embed PHP; this application does not open a TCP or UDS listener.

## Prepare

Cargo consumes Lumenize `0.2.0-alpha.18` from crates.io with `runner` and `cli`;
imports use `lumenize::{Options, Runner}`. Composer consumes `claviron/laravel`
`0.1.2` and `claviron/sapi` `0.1.1` from Packagist. These releases must exist before
installation; no sibling integration checkout is selected automatically.
PHP imports use `Lumenize\\` for
Laravel integration; native engine classes retain `Claviron\\`.
The optional benchmark development package is `claviron/bench` `0.1.0` from
Packagist; it does not use a sibling checkout. That release must be available
before updating the Composer lockfile.

From this directory:

```sh
# The checked-in .env and ../database/demo.sqlite contain public demo values.
"$PHP" /path/to/composer.phar install
npm install
npm run build
```

Use the workspace ZTS embed PHP build, not Homebrew PHP. Set `PHP` and
`PHP_CONFIG` to that installation, and `PHP_DIR` to its root for the ini paths
in `lumenize.yaml`. The example .env and app key are intentionally public;
never reuse them for a deployment or add real credentials to this file.
Dependencies and generated assets are not copied from the original application.

Todo/database routes use the pre-migrated shared [SQLite demo](../database/README.md).
PHP, Python and JavaScript processes open the same file independently; no native
PostgreSQL pool is required and Edge does not own their connections. The seeded
user is `demo@example.test` / `password`. For later schema changes, from here:

```sh
"$PHP" artisan migrate --database=sqlite --force
"$PHP" artisan db:seed --database=sqlite --force
```

This setup is explicit: the launch script does not install dependencies, create
databases or run migrations.

## Run behind the edge

From the Claviron root:

```sh
examples/scripts/octane-app.sh
# In another terminal, unless the example edge is already running:
examples/scripts/edge.sh
```

The application script supports `--release`, `--no-build` and an explicit
`WORKERS` override. Ctrl-C follows the runner's normal shutdown path.
`..` supplies the `octane.test` route and
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

`examples/scripts/octane-app.sh --bench --release` sets `EXAMPLES_BENCH=1`.
The local `..` development package supplies `/bench/plain`,
`/bench/json` and `/bench/echo` through the existing Laravel application, not a
native interception shortcut. Ordinary launches leave those routes disabled.
Keep results separate from transport or PHP lifecycle correctness checks.

The Rust package is an independent workspace with `publish = false`. The entire
`..` tree is excluded from the root Claviron crate's registry package.
