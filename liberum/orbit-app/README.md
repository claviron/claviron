# Orbit JavaScript application

A React/Vite frontend with a separate Hono application, reached through Claviron
over [Orbit Link](https://crates.io/crates/orbitive). Deno, Node and Bun use the
same native [`@claviron/orbit`](../../../orbit-js) addon. Its dependency is a local
path until the package is published; keep `orbit-js` beside the Claviron checkout
and build its native addon using that project's instructions.

## Prepare and run

From this directory:

```sh
npm install
npm run build
cp .env.example .env
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

PostgreSQL is optional. Set a private `DATABASE_URL` to enable `/api/db`; without
it only that diagnostic returns 503. No Revolution installation or business
event topics are required. These unauthenticated diagnostics and event publishing
routes are for a local demonstration, not a public deployment.
