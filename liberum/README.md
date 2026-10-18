# liberum

An open playground where applications built on Claviron come to life.
Independent applications share one edge through Orbit Link:

- `edge-server`: HTTP ingress, host admission and static assets.
- `fastapi-app`: Python/ASGI through Gufi, including WebSockets.
- [orbit-app](orbit-app/README.md): React/Vite and Hono on Deno, Node or Bun,
  using the local `orbit-js` addon, with streaming demo events.
- [octane-app](octane-app/README.md): Laravel through Lumenize, with native
  broadcasting, notifications, Pusher WebSockets and Rust interception.

Start applications independently, then use `scripts/edge.sh` from this directory.
The `scripts/` directory contains example launchers, not library install hooks.
Each Rust application has its own workspace and build output; it is not a member
of the library workspace. JavaScript and PHP dependencies are installed in their
respective application directories.

The edge declares `orbit.test` and `octane.test` alongside the existing FastAPI
transport-comparison hosts. Configure a local listener and resolve those names
to loopback for browser use. Launchers do not modify host resolution or restart
an already-running edge. Demo routes are unauthenticated and belong on a local
listener, not a public interface.


