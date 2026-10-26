// The dynamic half of orbit-app: joins fleet `orj` under `orbit-app` over Orbit
// Link, where the example edge routes what is not a static file. Hono answers
// requests; Kysely reads the shared demo SQLite file; events arrive over the Link
// and are pushed to browsers as Server-Sent Events.
//
//   npm run serve        (Deno)
//   npm run serve:node
//   npm run serve:bun
//
// The public .env names ../database/demo.sqlite; no remote DB credentials.

import { readFile } from "node:fs/promises";
import process from "node:process";
import { join } from "@claviron/orbit";
import { Hono } from "hono";
import { streamSSE } from "hono/streaming";
import { Kysely, SqliteDialect, sql } from "kysely";

const fleet = process.env.ORBIT_FLEET ?? "orj";
const name = process.env.ORBIT_APP ?? "orbit-app";
const runtime =
  typeof Deno !== "undefined" ? "deno" : typeof Bun !== "undefined" ? "bun" : "node";
const started = new Date();

// A public demo topic, independent of any application's business events.
const TOPICS = ["example.message.v1", "demo.shared.v1"];

const benchmark = process.env.EXAMPLES_BENCH === "1";
const database = benchmark ? null : new Kysely({
  dialect: new SqliteDialect({ database: (await import("./database.js")).openDemoDatabase() }),
});

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

if (benchmark) {
  app.get("/bench/javascript/plain", (c) => {
    c.header("x-orbit-process-id", String(process.pid));
    c.header("x-orbit-lane", String(link.lane));
    c.header("x-orbit-runtime", runtime);
    return c.text("ok\n");
  });
  app.get("/bench/javascript/json", (c) => {
    c.header("x-orbit-process-id", String(process.pid));
    c.header("x-orbit-lane", String(link.lane));
    c.header("x-orbit-runtime", runtime);
    return c.json({ message: "ok" });
  });
}

app.get("/api/status", (c) =>
  c.json({
    application: name,
    fleet,
    runtime,
    pid: process.pid,
    lane: link.lane,
    started: started.toISOString(),
    now: new Date().toISOString(),
    database: "sqlite",
    topics: TOPICS,
  }));

app.post("/api/echo", async (c) => {
  const text = await c.req.text();
  return c.json({ received: text, length: text.length, runtime });
});

app.get("/api/db", async (c) => {
  if (!database) return c.json({ error: "database disabled for benchmarks" }, 503);
  const result = await sql`select sqlite_version() as version, current_timestamp as now,
    (select count(*) from users) as users,
    (select count(*) from todos) as todos,
    (select count(*) from demo_records) as records`.execute(database);
  return c.json({ database: "demo.sqlite", ...result.rows[0] });
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
