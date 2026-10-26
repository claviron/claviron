// WebTransport reliable bidi streams carry the same Text/Binary/Close records
// as Orbit Link. A WebSocket lets the browser own those message boundaries.
const encoder = new TextEncoder();
const text = new TextDecoder("utf-8", { fatal: true });
export const socketPolicy = Object.freeze({ maxMessageBytes: 64 * 1024, pendingBytes: 256 * 1024 });

export function encodeRecord(kind, bytes) {
  const record = new Uint8Array(5 + bytes.length);
  record[0] = kind;
  new DataView(record.buffer).setUint32(1, bytes.length, true);
  record.set(bytes, 5);
  return record;
}

export class RecordDecoder {
  pending = new Uint8Array();
  push(bytes) {
    const joined = new Uint8Array(this.pending.length + bytes.length);
    joined.set(this.pending); joined.set(bytes, this.pending.length);
    this.pending = joined;
    const records = [];
    let offset = 0;
    while (this.pending.length - offset >= 5) {
      const kind = this.pending[offset];
      const length = new DataView(this.pending.buffer).getUint32(offset + 1, true);
      if (length > socketPolicy.maxMessageBytes || ![1, 2, 8].includes(kind) || (kind === 8 && length < 2)) {
        throw new Error("Invalid WebTransport message record");
      }
      if (this.pending.length - offset < 5 + length) break;
      records.push({ kind, bytes: this.pending.slice(offset + 5, offset + 5 + length) });
      offset += 5 + length;
    }
    this.pending = this.pending.slice(offset);
    return records;
  }
  finish() { if (this.pending.length) throw new Error("Truncated WebTransport record"); }
}

export class DemoConnection {
  generation = 0;
  active = null;
  writing = false;
  constructor(url, { onMessage, onState, onLog }) { Object.assign(this, { url, onMessage, onState, onLog }); }

  async connect(mode, initialMessage = null) {
    this.disconnect();
    const generation = this.generation;
    this.onState("connecting");
    if (mode !== "websocket") {
      try {
        if (!globalThis.WebTransport) throw new Error("WebTransport is unavailable in this browser");
        await this.openWT(generation, initialMessage);
        return;
      } catch (error) {
        if (generation !== this.generation) return;
        this.release();
        if (mode === "webtransport") { this.ended(generation, error.message); return; }
        this.onLog(`WebTransport unavailable: ${error.message}; trying WebSocket`);
      }
    }
    if (generation === this.generation) this.openWS(generation, initialMessage);
  }

  async openWT(generation, initialMessage) {
    const transport = new WebTransport(this.url);
    const closed = transport.closed.then(() => null, (error) => error);
    this.active = { close: () => transport.close() };
    await transport.ready;
    if (generation !== this.generation) return;
    const stream = await transport.createBidirectionalStream();
    const reader = stream.readable.getReader(), writer = stream.writable.getWriter();
    const close = () => {
      transport.close();
      void reader.cancel().catch(() => {});
      void writer.abort().catch(() => {});
    };
    if (generation !== this.generation) { close(); reader.releaseLock(); writer.releaseLock(); return; }
    this.active = { transport, close, send: (value) => writer.write(encodeRecord(1, encoder.encode(value))) };
    try {
      // QUIC may not expose an opened stream until it carries data.
      await writer.write(encodeRecord(1, encoder.encode(initialMessage ?? "/hello")));
    } catch (error) { close(); reader.releaseLock(); writer.releaseLock(); throw error; }
    if (generation !== this.generation) { reader.releaseLock(); writer.releaseLock(); return; }
    this.onState("connected");
    void closed.then((error) => this.ended(generation, error?.message ?? "WebTransport closed"));
    void (async () => {
      const decoder = new RecordDecoder();
      try {
        for (;;) {
          const { value, done } = await reader.read();
          if (generation !== this.generation) return;
          if (done) { decoder.finish(); break; }
          for (const record of decoder.push(value)) {
            if (record.kind === 8) {
              this.ended(generation, `Closed ${new DataView(record.bytes.buffer).getUint16(0, true)} ${text.decode(record.bytes.subarray(2))}`);
              return;
            }
            this.onMessage(record.kind === 1 ? text.decode(record.bytes) : record.bytes);
          }
        }
        this.ended(generation, "WebTransport stream ended");
      } catch (error) { this.ended(generation, error.message); }
      finally { reader.releaseLock(); writer.releaseLock(); }
    })();
  }

  openWS(generation, initialMessage) {
    let socket;
    try { socket = new WebSocket(this.url.replace(/^http/, "ws")); }
    catch (error) { this.ended(generation, error.message); return; }
    socket.binaryType = "arraybuffer";
    this.active = {
      close: () => socket.close(1000, "bye"),
      send: (value) => {
        if (socket.bufferedAmount + encoder.encode(value).length > socketPolicy.pendingBytes) {
          throw new Error("WebSocket is backpressured; wait before sending again");
        }
        socket.send(value);
      },
    };
    socket.onopen = () => {
      if (generation !== this.generation) return;
      if (initialMessage !== null) socket.send(initialMessage);
      this.onState("connected");
    };
    socket.onmessage = (event) => {
      if (generation === this.generation) this.onMessage(event.data);
    };
    socket.onerror = () => this.ended(generation, "WebSocket connection failed");
    socket.onclose = (event) => this.ended(generation, `Closed ${event.code} ${event.reason}`);
  }

  async send(value) {
    if (!this.active?.send) throw new Error("Not connected");
    if (this.writing) throw new Error("A send is already in progress");
    if (encoder.encode(value).length > socketPolicy.maxMessageBytes) throw new Error("Message is too large");
    this.writing = true;
    try { await this.active.send(value); }
    finally { this.writing = false; }
  }
  async testStreams() {
    const transport = this.active?.transport;
    if (!transport) throw new Error("Stream test requires WebTransport");
    const generation = this.generation;
    const peers = [];
    const open = async () => {
      const stream = await transport.createBidirectionalStream();
      const peer = { reader: stream.readable.getReader(), writer: stream.writable.getWriter(), decoder: new RecordDecoder(), pending: [] };
      peers.push(peer);
      return peer;
    };
    const next = async (peer, event) => {
      for (;;) {
        if (generation !== this.generation) throw new Error("Connection closed");
        while (peer.pending.length) {
          const record = peer.pending.shift();
          if (record.kind !== 1) throw new Error("Expected a text record");
          const value = JSON.parse(text.decode(record.bytes));
          if (value.event === event) return value;
        }
        const { value, done } = await peer.reader.read();
        if (done) throw new Error("Probe stream ended early");
        peer.pending.push(...peer.decoder.push(value));
      }
    };
    this.onLog("Concurrent stream test started");
    try {
      const a = await open();
      const request = encodeRecord(1, encoder.encode("/concurrent-stream-test A"));
      await a.writer.write(request.subarray(0, 5));
      const helloA = await next(a, "connected");
      this.onLog("Stream A: waiting for remaining request bytes");
      const b = await open();
      await b.writer.write(encodeRecord(1, encoder.encode("/concurrent-stream-test B")));
      const helloB = await next(b, "connected");
      const answerB = await next(b, "concurrent_stream_test");
      if (answerB.stream !== "B" || answerB.socket_id !== helloB.socket_id || helloA.socket_id === helloB.socket_id) {
        throw new Error("Probe stream identity mismatch");
      }
      this.onLog("Stream B: reply received while A is waiting ✓");
      await a.writer.write(request.subarray(5));
      const answerA = await next(a, "concurrent_stream_test");
      if (answerA.stream !== "A" || answerA.socket_id !== helloA.socket_id) throw new Error("Probe stream identity mismatch");
      await Promise.all(peers.map((peer) => peer.writer.close()));
      this.onLog("Stream A: released and completed");
      this.onLog("Concurrent stream test: PASS");
    } finally {
      await Promise.allSettled(peers.flatMap((peer) => [peer.reader.cancel(), peer.writer.abort()]));
      for (const peer of peers) { peer.reader.releaseLock(); peer.writer.releaseLock(); }
    }
  }
  release() { const active = this.active; this.active = null; try { active?.close(); } catch { /* Already closed. */ } }
  disconnect() { ++this.generation; this.release(); this.onState("disconnected"); }
  ended(generation, reason) {
    if (generation !== this.generation) return;
    ++this.generation; this.release(); this.onLog(reason); this.onState("disconnected");
  }
}
