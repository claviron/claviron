// SQLite is local to this process; Orbit Link never transports this handle.
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import process from "node:process";

const isBun = typeof Bun !== "undefined";
const Database = isBun
  ? (await import("bun:sqlite")).Database
  : (await import("node:sqlite")).DatabaseSync;

/** The better-sqlite3-shaped interface consumed by Kysely's SqliteDialect. */
export function openDemoDatabase(path = process.env.LIBERUM_DATABASE ?? "../database/demo.sqlite") {
  const file = resolve(dirname(fileURLToPath(import.meta.url)), path);
  // Refuse a missing database instead of creating an empty, unmigrated file.
  const database = new Database(file, isBun
    ? { readonly: true, create: false }
    : { readOnly: true });
  return {
    close() {
      database.close();
    },
    prepare(sql) {
      const statement = database.prepare(sql);
      return {
        reader: (isBun ? statement.columnNames : statement.columns()).length > 0,
        all(parameters) { return statement.all(...parameters); },
        run(parameters) { return statement.run(...parameters); },
        iterate(parameters) { return statement.iterate(...parameters); },
      };
    },
  };
}
