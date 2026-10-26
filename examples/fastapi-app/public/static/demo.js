import { DemoConnection } from "./socket.js";

for (const button of document.querySelectorAll("[data-request]")) {
  button.addEventListener("click", async () => {
    const output = button.closest("article").querySelector("pre");
    button.disabled = true; output.hidden = false; output.textContent = "Loading…";
    try {
      const response = await fetch(button.dataset.request);
      const body = await response.text();
      let value;
      try { value = JSON.stringify(JSON.parse(body), null, 2); } catch { value = body; }
      output.textContent = `${response.status} ${response.statusText}\n${value}`;
    } catch (error) { output.textContent = error.message; }
    finally { button.disabled = false; }
  });
}

let events = null;
function stopEvents() {
  events?.close(); events = null;
  document.querySelector("#events-start").disabled = false;
  document.querySelector("#events-stop").disabled = true;
}
document.querySelector("#events-stop").addEventListener("click", stopEvents);
document.querySelector("#events-form").addEventListener("submit", (event) => {
  event.preventDefault(); stopEvents();
  const count = Number(document.querySelector("#event-count").value);
  const every = Number(document.querySelector("#event-spacing").value);
  const output = document.querySelector("#events-log");
  output.hidden = false; output.textContent = "";
  let received = 0;
  events = new EventSource(`/events?count=${count}&every=${every}`);
  document.querySelector("#events-start").disabled = true;
  document.querySelector("#events-stop").disabled = false;
  events.onmessage = ({ data }) => {
    output.textContent += `data: ${data}\n`;
    if (++received >= count) { stopEvents(); output.textContent += "— Complete\n"; }
  };
  events.onerror = () => { stopEvents(); output.textContent += "— Stream closed\n"; };
});

const connect = document.querySelector("#connect"), disconnect = document.querySelector("#disconnect");
const transport = document.querySelector("#transport"), input = document.querySelector("#message");
const send = document.querySelector("#send"), probe = document.querySelector("#stream-test");
const output = document.querySelector("#socket-log"), details = document.querySelector("#connection-details");
let connection, connected = false, isWT = false, testing = false;
function append(line) {
  output.textContent = [...output.textContent.split("\n"), line].slice(-100).join("\n");
  output.scrollTop = output.scrollHeight;
}
function state(value) {
  connected = value === "connected";
  document.querySelector("#connection-state").textContent = value;
  connect.disabled = value !== "disconnected";
  disconnect.disabled = value === "disconnected";
  transport.disabled = value !== "disconnected";
  input.disabled = send.disabled = !connected;
  probe.disabled = !connected || !isWT || testing;
}
connect.addEventListener("click", () => {
  connection?.disconnect(); isWT = false;
  details.textContent = "Waiting for the application's transport details…";
  connection = new DemoConnection(new URL("/ws", location.origin).href, {
    onState: state,
    onLog: (line) => append(`— ${line}`),
    onMessage: (data) => {
      if (typeof data !== "string") { append(`< binary: ${data.byteLength} bytes`); return; }
      append(`< ${data}`);
      try {
        const message = JSON.parse(data);
        if (message.event === "connected") {
          isWT = message.transport === "webtransport";
          details.textContent = `HTTP/${message.http_version} · ${message.transport} · Python PID ${message.pid}`;
          probe.disabled = !connected || !isWT || testing;
          if (!isWT) append("— Concurrent stream test unavailable: H3 WebTransport was not selected.");
        }
      } catch { /* Plain echo text is already shown above. */ }
    },
  });
  void connection.connect(transport.value).catch((error) => { append(`— ${error.message}`); connection.disconnect(); });
});
disconnect.addEventListener("click", () => connection?.disconnect());
document.querySelector("#message-form").addEventListener("submit", async (event) => {
  event.preventDefault(); send.disabled = true;
  const value = input.value;
  try { await connection.send(value); append(`> ${value}`); input.value = ""; }
  catch (error) { append(`— ${error.message}`); }
  finally { send.disabled = !connected; }
});
probe.addEventListener("click", async () => {
  testing = true; probe.disabled = true;
  try { await connection.testStreams(); }
  catch (error) { append(`— Concurrent stream test: FAILED — ${error.message}`); }
  finally { testing = false; probe.disabled = !connected || !isWT; }
});
document.querySelector("#clear-log").addEventListener("click", () => { output.textContent = ""; });
window.addEventListener("pagehide", () => { stopEvents(); connection?.disconnect(); });
