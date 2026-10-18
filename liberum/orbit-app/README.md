# Orbit JavaScript application

A React/Vite frontend with a separate Hono application, reached through Claviron
over [Orbit Link](https://crates.io/crates/orbitive). Deno, Node and Bun use the
same native [`@claviron/orbit`](https://www.npmjs.com/package/@claviron/orbit)
addon, pinned to npm release `0.1.0`. Publish that release with a native addon
for the target OS/CPU before installation. The example does not select the
`orbit-js` source checkout or compile an addon during installation.

## Prepare and run

From this directory:

```sh
npm install
npm run build
# The checked-in .env contains only public demo settings.
npm run serve       # Deno
# npm run serve:node
# npm run serve:bun
```

Alternatively, use `liberum/scripts/orbit-app.sh [deno|node|bun]` from the
Claviron root. The script starts the application; it does not install or build.
The default identity is application `orbit-app` in fleet `orj`. `ORBIT_APP` and
`ORBIT_FLEET` can override it; update the edge's route to match if changed.

## Through the example edge

Start `liberum/scripts/edge.sh` from the Claviron root. Its `orbit.test` vhost
serves `/assets` from this application's generated `public/` directory and
routes other requests to the named application over SHM, not TCP or UDS.
Resolve `orbit.test` to loopback for browser use. The commands below assume the
edge listener is configured on loopback port 8080.

```sh
curl --resolve orbit.test:8080:127.0.0.1 http://orbit.test:8080/api/status
curl --resolve orbit.test:8080:127.0.0.1 http://orbit.test:8080/api/echo \
  --data 'Hello over Orbit'
```

The page shows the runtime, process and lane, echoes a request body, and can
publish `example.message.v1`. Open two tabs to observe events through
`/api/events/stream` (Server-Sent Events). The stream is event-driven, not polled;
the in-memory recent-event list is bounded and not durable history. Multiple
application processes can join the same named fleet.

It also subscribes to `demo.shared.v1`, the Octane example's `SharedEvent` topic.
After preparing that example's database and seeding its demo user, its
`/events/shared` route can publish the PHP event for subscribed linked consumers.

`/api/db` reads the same pre-migrated [SQLite file](../database/README.md) as
Octane and FastAPI. It reports shared user/todo/record counts without opening a
remote DB connection. Node 24 and Deno use built-in `node:sqlite`; Bun uses
`bun:sqlite`, adapted to Kysely's SQLite interface. No new native addon needs
compilation for database access. Node currently marks its SQLite API experimental.
The database is opened read-only and must already exist; startup does not create
or migrate it. `LIBERUM_DATABASE` can explicitly select another local file.

No Revolution installation or business event topics are required. The .env
contains public demonstration values only; do not add credentials. These
unauthenticated diagnostics and event publishing routes are for a local
demonstration, not a public deployment.
