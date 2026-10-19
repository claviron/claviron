import { useCallback, useEffect, useState } from "react";

/** Static files come from the edge; `/api` goes over Orbit Link to server.js. */
export default function App() {
  const [status, setStatus] = useState(null);
  const [statusError, setStatusError] = useState(null);
  const [database, setDatabase] = useState(null);
  const [events, setEvents] = useState([]);
  const [text, setText] = useState("");
  const [reply, setReply] = useState(null);

  const refresh = useCallback(async () => {
    try {
      const response = await fetch("/api/status");
      setStatus(await response.json());
      setStatusError(null);
    } catch (error) {
      setStatusError(String(error));
    }
  }, []);

  useEffect(() => {
    refresh();
  }, [refresh]);

  // Orbit events are pushed as they happen; the recent ones first.
  useEffect(() => {
    fetch("/api/events").then((r) => r.json()).then(setEvents).catch(() => {});
    const source = new EventSource("/api/events/stream");
    source.onmessage = (message) => {
      const entry = JSON.parse(message.data);
      setEvents((previous) => [entry, ...previous.filter((e) => e.id !== entry.id)].slice(0, 50));
    };
    return () => source.close();
  }, []);

  async function checkDatabase() {
    const response = await fetch("/api/db");
    setDatabase(await response.json());
  }

  async function send(event) {
    event.preventDefault();
    const response = await fetch("/api/echo", { method: "POST", body: text });
    setReply(await response.json());
  }

  async function publish() {
    const response = await fetch("/api/events", { method: "POST", body: text });
    setReply(await response.json());
  }

  return (
    <main>
      <h1>Orbit App</h1>
      <p className="lede">
        Static files from the edge; <code>/api</code> over Orbit Link.
      </p>

      <section>
        <h2>Server</h2>
        <pre>
          {statusError
            ? `unreachable: ${statusError}`
            : status
              ? JSON.stringify(status, null, 2)
              : "…"}
        </pre>
        <button onClick={refresh}>Refresh</button>
      </section>

      <section>
        <h2>Orbit events</h2>
        {events.length === 0 ? (
          <p className="lede">Publish a demo event below; another tab receives it live.</p>
        ) : (
          <ul className="events">
            {events.map((entry) => (
              <li key={entry.id}>
                <time>{new Date(entry.at).toLocaleTimeString()}</time>{" "}
                <strong>{entry.topic}</strong>
                <pre>{JSON.stringify(entry.payload, null, 2)}</pre>
              </li>
            ))}
          </ul>
        )}
      </section>

      <section>
        <h2>Database</h2>
        <button onClick={checkDatabase}>Check</button>
        {database && <pre>{JSON.stringify(database, null, 2)}</pre>}
      </section>

      <section>
        <h2>Echo</h2>
        <form onSubmit={send}>
          <input
            value={text}
            onChange={(event) => setText(event.target.value)}
            placeholder="Something to send"
            autoComplete="off"
          />
          <button>Echo</button>
          <button type="button" onClick={publish}>Publish event</button>
        </form>
        {reply && <pre>{JSON.stringify(reply, null, 2)}</pre>}
      </section>
    </main>
  );
}
