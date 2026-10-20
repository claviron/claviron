//! Lua callbacks over one reliable WebTransport bidirectional stream.
//!
//! The native H3 driver owns acceptance and drain. Records use the existing
//! Text/Binary/Close codec, not WebSocket frames or unreliable datagrams.
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use claviron_orbit::transport::{Message, MessageReader, write_message};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc;

use crate::app::LuaApplication;
use crate::serve::LuaApplications;
use crate::socket::{
    Event, LuaSocket, NEXT_ID, Outbound, Transport, handle_events,
};

/// Resolve the application generation before accepting the native session.
pub fn prepare(
    service: &LuaApplications,
    name: &str,
) -> mlua::Result<Arc<LuaApplication>> {
    let application = service
        .get(name)
        .ok_or_else(|| mlua::Error::runtime("Lua application is not loaded"))?;
    if application.socket_handlers(Transport::WebTransport)?.is_none() {
        return Err(mlua::Error::runtime(
            "Lua application serves no WebTransport",
        ));
    }
    Ok(application)
}

/// Serve a stream owned by the accepting WebTransport session.
pub async fn serve<S>(
    mut stream: S,
    url: String,
    headers: Vec<(String, String)>,
    application: Arc<LuaApplication>,
    shutdown: impl Future<Output = ()> + Send,
) where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    let config = application.socket_config(Transport::WebTransport);
    let (outbound, mut queued) = mpsc::channel(config.send_queue.max(1));
    let socket = LuaSocket {
        id: NEXT_ID.fetch_add(1, Ordering::Relaxed),
        url,
        headers,
        outbound,
        transport: Transport::WebTransport,
        http_version: "HTTP/3.0",
    };
    // One pending event overlaps the callback; further reads wait for it.
    let (events, happened) = mpsc::channel(1);
    let handling = handle_events(Arc::clone(&application), socket, happened);
    let stopping = application.execution().stopping().clone();
    let driving = async {
        tokio::pin!(shutdown);
        let mut records = MessageReader::with_limit(config.max_message_size);
        let mut pending = None;
        let close = loop {
            tokio::select! {
                biased;
                () = stopping.cancelled() => break (1001, "application stopping".to_owned(), false),
                () = &mut shutdown => break (1001, "server stopping".to_owned(), false),
                out = queued.recv() => {
                    let Some(out) = out else { break (1011, "handler ended".to_owned(), false) };
                    let message = match out {
                        Outbound::Text(text) => Message::Text(text),
                        Outbound::Binary(bytes) => Message::Binary(bytes),
                        Outbound::Close(code, reason) => break (code, reason, true),
                    };
                    if write_message(&mut stream, &message).await.is_err()
                        || stream.flush().await.is_err() {
                        break (1006, String::new(), false);
                    }
                },
                permit = events.reserve(), if pending.is_some() => {
                    match permit {
                        Ok(permit) => permit.send(pending.take().expect("pending event")),
                        Err(_) => break (1011, "handler ended".to_owned(), false),
                    }
                },
                record = records.read(&mut stream), if pending.is_none() => {
                    match record {
                        Ok(Some(Message::Text(text))) => pending = Some(Event::Message(text.into_bytes(), false)),
                        Ok(Some(Message::Binary(bytes))) => pending = Some(Event::Message(bytes, true)),
                        Ok(Some(Message::Close { code, reason })) => break (code, reason, true),
                        Ok(None) => break (1000, String::new(), true),
                        Err(error) => {
                            tracing::debug!(%error, "Lua WebTransport stream ended");
                            break (1006, String::new(), false);
                        }
                    }
                },
            }
        };
        // Stop sends before awaiting callbacks, so a full outbound queue cannot
        // strand a handler after IO ends. Both futures belong to this session.
        queued.close();
        let (code, reason, clean) = close;
        if code != 1006 {
            let _ = write_message(
                &mut stream,
                &Message::Close { code, reason: reason.clone() },
            )
            .await;
            let _ = stream.shutdown().await;
        }
        if let Some(event) = pending {
            let _ = events.send(event).await;
        }
        let _ = events.send(Event::Close(code, reason, clean)).await;
        drop(events);
    };
    tokio::join!(handling, driving);
}
