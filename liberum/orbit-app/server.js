// The dynamic half of orbit-app: joins fleet `orj` under `orbit-app` over Orbit
// Link, where the example edge routes what is not a static file. Hono answers
// requests; Kysely optionally reads Postgres; demo events arrive over the Link
// and are pushed to browsers as Server-Sent Events.
//
//   npm run serve        (Deno)
//   npm run serve:node
//   npm run serve:bun
//
// DATABASE_URL (in .env, not in git) points at the Postgres database.

import { readFile } from "node:fs/promises";
import process from "node:process";
import { join } from "@claviron/orbit";
import { Hono } from "hono";
import { streamSSE } from "hono/streaming";
import { Kysely, PostgresDialect, sql } from "kysely";
import pg from "pg";

const fleet = process.env.ORBIT_FLEET ?? "orj";
const name = process.env.ORBIT_APP ?? "orbit-app";
const runtime =
  typeof Deno !== "undefined" ? "deno" : typeof Bun !== "undefined" ? "bun" : "node";
const started = new Date();

// A public demo topic, independent of any application's business events.
const TOPICS = ["example.message.v1", "demo.shared.v1"];

const database = process.env.DATABASE_URL
  ? new Kysely({
      dialect: new PostgresDialect({
        pool: new pg.Pool({ connectionString: process.env.DATABASE_URL }),
      }),
    })
  : null;

const link = await join({ fleet, name, icon: "orbit" });

// Recent events for a page that opens later, and the open event streams.
const recent = [];
const streams = new Set();
for (const topic of TOPICS) {
  link.on(topic, (payload, event) => {
    const entry = { id: event.id, topic, payload, at: new Date().toISOString() };
    console.log(`orbit-app: ${topic}`, JSON.stringify(payload));
    recent.unshift(entry);
    recent.length = Math.min(recent.length, 50);
    for (const send of streams) {
      send(entry);
    }
  });
}

const app = new Hono();

app.get("/api/status", (c) =>
  c.json({
    application: name,
    fleet,
    runtime,
    pid: process.pid,
    lane: link.lane,
    started: started.toISOString(),
    now: new Date().toISOString(),
    database: database !== null,
    topics: TOPICS,
  }));

app.post("/api/echo", async (c) => {
  const text = await c.req.text();
  return c.json({ received: text, length: text.length, runtime });
});

app.get("/api/db", async (c) => {
  if (database === null) {
    return c.json({ error: "DATABASE_URL is not set" }, 503);
  }
  const result = await sql`select current_database() as database, now() as now`.execute(database);
  return c.json(result.rows[0]);
});

app.get("/api/events", (c) => c.json(recent));

app.post("/api/events", async (c) => {
  const message = await c.req.text();
  const id = link.publish(TOPICS[0], { message, at: new Date().toISOString() });
  return c.json({ published: id });
});

// Each event is pushed as it arrives; nothing polls on either side.
app.get("/api/events/stream", (c) =>
  streamSSE(c, async (stream) => {
    const send = (entry) => stream.writeSSE({ data: JSON.stringify(entry), id: entry.id });
    streams.add(send);
    await new Promise((resolve) => stream.onAbort(resolve));
    streams.delete(send);
  }));

app.all("/api/*", (c) => c.json({ error: "not found" }, 404));

// A route the client renders itself: the single-page entry.
app.get("*", async (c) => {
  const page = await readFile(new URL("./public/index.html", import.meta.url), "utf8");
  return c.html(page);
});

const stop = async () => {
  await link.close();
  await database?.destroy();
};
process.on("SIGINT", stop);
process.on("SIGTERM", stop);

console.log(`orbit-app: serving '${name}' in fleet '${fleet}' on lane ${link.lane} (${runtime}, pid ${process.pid})`);
await link.serve(app.fetch);
console.log(`orbit-app: released '${name}'`);
