// Own only these child processes. No restart loop, polling or kill deadline.
import { spawn } from "node:child_process";
import process from "node:process";

const [runtime, root, countText] = process.argv.slice(2);
const count = Number(countText);
if (!["node", "bun", "deno"].includes(runtime) || !Number.isSafeInteger(count) || count < 1) {
  throw new Error("Expected a runtime, application root and positive worker count");
}
const benchmark = process.env.EXAMPLES_BENCH === "1";
const args = runtime === "deno"
  ? ["run", "--allow-ffi", "--allow-read", "--allow-env", "--allow-net", "--allow-sys", ...(!benchmark ? ["--env-file=.env"] : []), "server.js"]
  : runtime === "node"
    ? [...(!benchmark ? ["--env-file-if-exists=.env"] : []), "server.js"]
    : ["server.js"];
const children = new Set();
let stopping = false;
let failed = false;
const stop = () => {
  stopping = true;
  for (const child of children) child.kill("SIGTERM");
};
process.on("SIGINT", stop);
process.on("SIGTERM", stop);
await Promise.all(Array.from({ length: count }, () => new Promise((resolve) => {
  const child = spawn(runtime, args, { cwd: root, env: process.env, stdio: "inherit" });
  children.add(child);
  child.once("error", (error) => {
    console.error(error);
    failed = true;
    children.delete(child);
    stop();
    resolve();
  });
  child.once("exit", (code, signal) => {
    children.delete(child);
    if (!stopping) {
      console.error(`${runtime} worker ${child.pid} exited unexpectedly: ${signal ?? code}`);
      failed = true;
      stop();
    }
    resolve();
  });
})));
process.exitCode = failed ? 1 : 0;
