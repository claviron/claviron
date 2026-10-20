//! Lua callbacks over one reliable WebTransport bidirectional stream.
//!
//! The native H3 driver owns acceptance and drain. Records use the existing
//! Text/Binary/Close codec, not WebSocket frames or unreliable datagrams.
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use claviron_orbit::transport::{Message, MessageReader, write_message};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot};

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
    stream: S,
    url: String,
    headers: Vec<(String, String)>,
    application: Arc<LuaApplication>,
    shutdown: impl Future<Output = ()> + Send,
) where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    let config = application.socket_config(Transport::WebTransport);
    let (outbound, queued) = mpsc::channel(config.send_queue.max(1));
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
    let shutdown = async {
        tokio::pin!(shutdown);
        tokio::select! {
            biased;
            () = stopping.cancelled() => Close(1001, "application stopping".to_owned(), false),
            () = &mut shutdown => Close(1001, "server stopping".to_owned(), false),
        }
    };
    let driving =
        drive(stream, queued, events, config.max_message_size, shutdown);
    tokio::join!(handling, driving);
}

#[derive(Clone)]
struct Close(u16, String, bool);

async fn drive<S>(
    stream: S,
    queued: mpsc::Receiver<Outbound>,
    events: mpsc::Sender<Event>,
    max_message_size: usize,
    shutdown: impl Future<Output = Close>,
) where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let (mut reader, writer) = tokio::io::split(stream);
    let (finish, closing) = oneshot::channel();
    let writing = write_loop(writer, queued, closing);
    tokio::pin!(writing, shutdown);
    let mut writer_done = false;
    let mut records = MessageReader::with_limit(max_message_size);
    let mut pending = None;
    let close = loop {
        tokio::select! {
            biased;
            close = &mut shutdown => break close,
            close = &mut writing => {
                writer_done = true;
                break close;
            },
            permit = events.reserve(), if pending.is_some() => {
                match permit {
                    Ok(permit) => permit.send(pending.take().expect("pending event")),
                    Err(_) => break Close(1011, "handler ended".to_owned(), false),
                }
            },
            record = records.read(&mut reader), if pending.is_none() => {
                match record {
                    Ok(Some(Message::Text(text))) => pending = Some(Event::Message(text.into_bytes(), false)),
                    Ok(Some(Message::Binary(bytes))) => pending = Some(Event::Message(bytes, true)),
                    Ok(Some(Message::Close { code, reason })) => break Close(code, reason, true),
                    Ok(None) => break Close(1000, String::new(), true),
                    Err(error) => {
                        tracing::debug!(%error, "Lua WebTransport stream ended");
                        break Close(1006, String::new(), false);
                    }
                }
            },
        }
    };
    let _ = finish.send(close.clone());
    let notifying = async move {
        if let Some(event) = pending {
            let _ = events.send(event).await;
        }
        let Close(code, reason, clean) = close;
        let _ = events.send(Event::Close(code, reason, clean)).await;
    };
    // Keep polling the writer while callbacks finish: closing its receiver
    // wakes producers blocked on a full queue, even if the peer cannot read.
    let draining = async {
        if !writer_done {
            writing.await;
        }
    };
    tokio::join!(notifying, draining);
}

async fn write_loop<W: AsyncWrite + Unpin>(
    mut writer: W,
    mut queued: mpsc::Receiver<Outbound>,
    mut closing: oneshot::Receiver<Close>,
) -> Close {
    let mut close = None;
    loop {
        let out = tokio::select! {
            biased;
            Ok(reason) = &mut closing, if close.is_none() => {
                queued.close();
                close = Some(reason);
                continue;
            }
            out = queued.recv() => out,
        };
        let message = match out {
            Some(Outbound::Text(text)) => Message::Text(text),
            Some(Outbound::Binary(bytes)) => Message::Binary(bytes),
            Some(Outbound::Close(code, reason)) => {
                close.get_or_insert(Close(code, reason, true));
                break;
            }
            None => break,
        };
        let sending = async {
            write_message(&mut writer, &message).await?;
            writer.flush().await
        };
        tokio::pin!(sending);
        let result = tokio::select! {
            biased;
            Ok(reason) = &mut closing, if close.is_none() => {
                queued.close();
                close = Some(reason);
                // Never cancel a partially written record and start another.
                sending.await
            }
            result = &mut sending => result,
        };
        if result.is_err() {
            return Close(1006, String::new(), false);
        }
    }
    queued.close();
    let close =
        close.unwrap_or_else(|| Close(1011, "handler ended".to_owned(), false));
    if close.0 != 1006 {
        let message = Message::Close { code: close.0, reason: close.1.clone() };
        if write_message(&mut writer, &message).await.is_err()
            || writer.shutdown().await.is_err()
        {
            return Close(1006, String::new(), false);
        }
    }
    close
}

#[cfg(test)]
#[path = "../tests/webtransport.rs"]
mod tests;
