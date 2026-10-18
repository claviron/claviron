//! WebSockets for Lua applications, the way JavaScript applications have
//! them: the edge holds the socket — handshake, frames, ping, close — and the
//! application sees three events.
//!
//! ```lua
//! router:websocket({
//!   open    = function(socket) socket:send("hello") end,
//!   message = function(socket, data, binary) socket:send(data) end,
//!   close   = function(socket, code, reason, clean) end,
//! })
//! ```
//!
//! A socket's events run one after another, each in its own coroutine and
//! each holding one of the application's `concurrency` permits only while it
//! runs, so a socket that is open and quiet costs no permit. `socket:send`
//! queues for the client and waits while the queue is full. The same session
//! serves an HTTP/1.1 `Upgrade` and an HTTP/2 extended CONNECT: both hand an
//! upgraded transport to [`serve_websocket`].

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use axum::body::Body;
use claviron_http::upgrade::{
    H1UpgradeAccept, H1UpgradeContext, H1UpgradeHandler, H1UpgradeReject,
    H1UpgradeResult, H1UpgradeState, is_h1_header_upgrade,
    websocket_response_headers,
};
use claviron_runtime::AppState;
use claviron_runtime::registry::{
    Error as RegistryError, Result as RegistryResult,
};
use fastwebsockets::{
    FragmentCollector, Frame, OpCode, Payload, Role, WebSocket,
};
use http::Request;
use hyper::upgrade::Upgraded;
use hyper_util::rt::TokioIo;
use mlua::{Function, LuaString, Table, UserData, UserDataMethods, Value};
use tokio::sync::mpsc;
use tracing::{debug, warn};

use crate::app::LuaApplication;
use crate::serve::LuaApplications;

/// Why a socket cannot be opened for an application.
#[derive(Debug, thiserror::Error)]
pub enum Refused {
    #[error("Lua application '{0}' is not loaded")]
    NotLoaded(String),
    #[error("Lua application '{0}' serves no WebSocket")]
    NoHandlers(String),
}

/// The application a socket is for, checked before the handshake is
/// answered: loaded, and with handlers to hand its events to.
pub fn prepare(
    service: &LuaApplications,
    application: &str,
) -> Result<Arc<LuaApplication>, Refused> {
    let loaded = service
        .get(application)
        .ok_or_else(|| Refused::NotLoaded(application.to_owned()))?;
    match loaded.websocket_handlers() {
        Ok(Some(_)) => Ok(loaded),
        _ => Err(Refused::NoHandlers(application.to_owned())),
    }
}

/// One exact HTTP/1.1 WebSocket route of a Lua application.
pub struct LuaWebSocketUpgradeHandler {
    service: Arc<LuaApplications>,
    application: Arc<str>,
}

impl LuaWebSocketUpgradeHandler {
    pub fn new(
        service: Arc<LuaApplications>,
        application: impl Into<Arc<str>>,
    ) -> Self {
        Self { service, application: application.into() }
    }
}

#[async_trait]
impl H1UpgradeHandler for LuaWebSocketUpgradeHandler {
    fn name(&self) -> &'static str {
        "lua_websocket"
    }

    fn is_upgrade(
        &self,
        req: &Request<Body>,
    ) -> bool {
        is_h1_header_upgrade(req, "websocket")
    }

    async fn accept(
        &self,
        _state: &AppState,
        ctx: &H1UpgradeContext,
    ) -> H1UpgradeResult {
        let headers = websocket_response_headers(ctx, None)
            .map_err(|error| H1UpgradeReject::bad_request(error.to_string()))?;
        let application =
            prepare(&self.service, &self.application).map_err(|error| {
                H1UpgradeReject::service_unavailable(error.to_string())
            })?;
        Ok(H1UpgradeAccept::with_state(headers, application))
    }

    async fn handle_upgraded(
        &self,
        _state: AppState,
        ctx: H1UpgradeContext,
        accepted: Option<H1UpgradeState>,
        upgraded: Upgraded,
    ) -> RegistryResult<()> {
        let application = accepted
            .ok_or_else(|| {
                RegistryError::msg("Lua WebSocket application missing")
            })?
            .take::<Arc<LuaApplication>>()?;
        let host = ctx
            .headers
            .get(http::header::HOST)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned)
            .unwrap_or_else(|| ctx.host.to_string());
        let url = format!("wss://{host}{}", ctx.uri);
        serve_websocket(upgraded, url, header_pairs(&ctx.headers), application)
            .await;
        Ok(())
    }
}

/// The request headers as a socket shows them.
pub fn header_pairs(headers: &http::HeaderMap) -> Vec<(String, String)> {
    headers
        .iter()
        .map(|(name, value)| {
            (
                name.as_str().to_owned(),
                String::from_utf8_lossy(value.as_bytes()).into_owned(),
            )
        })
        .collect()
}

/// What the application writes to a socket.
enum Outbound {
    Text(String),
    Binary(Vec<u8>),
    Close(u16, String),
}

/// What a socket's events are made of, in the order they happened.
enum Event {
    Message(Vec<u8>, bool),
    Close(u16, String, bool),
}

/// Ids tell sockets apart within this worker process.
static NEXT_ID: AtomicU64 = AtomicU64::new(1);

/// One socket as a handler holds it.
struct LuaSocket {
    id: u64,
    url: String,
    headers: Vec<(String, String)>,
    outbound: mpsc::Sender<Outbound>,
}

fn failure(error: impl std::fmt::Display) -> mlua::Error {
    mlua::Error::runtime(format!("socket: {error}"))
}

impl LuaSocket {
    async fn queue(
        &self,
        message: Outbound,
    ) -> mlua::Result<()> {
        self.outbound
            .send(message)
            .await
            .map_err(|_| failure("the socket is closed"))
    }
}

impl UserData for LuaSocket {
    fn add_fields<F: mlua::UserDataFields<Self>>(fields: &mut F) {
        fields.add_field_method_get("id", |_, this| Ok(this.id));
        fields.add_field_method_get("url", |_, this| Ok(this.url.clone()));
        fields.add_field_method_get("headers", |lua, this| {
            let table = lua.create_table()?;
            for (name, value) in &this.headers {
                table.set(name.as_str(), value.as_str())?;
            }
            Ok(table)
        });
    }

    fn add_methods<M: UserDataMethods<Self>>(methods: &mut M) {
        // A text message: the string must be UTF-8, as the protocol says.
        methods.add_async_method(
            "send",
            |_, this: mlua::UserDataRef<LuaSocket>, text: LuaString| async move {
                let text = text
                    .to_str()
                    .map_err(|_| failure("text is UTF-8; send bytes with send_binary"))?
                    .to_string();
                this.queue(Outbound::Text(text)).await
            }
        );
        methods.add_async_method(
            "send_binary",
            |_, this: mlua::UserDataRef<LuaSocket>, bytes: LuaString| async move {
                this.queue(Outbound::Binary(bytes.as_bytes().to_vec())).await
            }
        );
        methods.add_async_method(
            "close",
            |_,
             this: mlua::UserDataRef<LuaSocket>,
             (code, reason): (Option<u16>, Option<String>)| async move {
                this.queue(Outbound::Close(
                    code.unwrap_or(1000),
                    reason.unwrap_or_default(),
                ))
                .await
            },
        );
    }
}

/// Serve one socket over an upgraded transport until either side closes it
/// or the application stops. HTTP/1.1 `Upgrade` and HTTP/2 extended CONNECT
/// both end here.
pub async fn serve_websocket(
    upgraded: Upgraded,
    url: String,
    headers: Vec<(String, String)>,
    application: Arc<LuaApplication>,
) {
    let config = application.socket_config();
    let (outbound, mut queued) = mpsc::channel(config.send_queue.max(1));
    let socket = LuaSocket {
        id: NEXT_ID.fetch_add(1, Ordering::Relaxed),
        url,
        headers,
        outbound,
    };
    let id = socket.id;
    let (events, happened) = mpsc::channel::<Event>(1);
    let handling =
        tokio::spawn(handle_events(Arc::clone(&application), socket, happened));

    let mut raw =
        WebSocket::after_handshake(TokioIo::new(upgraded), Role::Server);
    raw.set_auto_close(true);
    raw.set_auto_pong(true);
    raw.set_max_message_size(config.max_message_size);
    let mut frames = FragmentCollector::new(raw);

    // One event waits here for the handlers to take it; frames are not read
    // meanwhile, so a client that sends faster than the application reads
    // is held back by its own connection. What the application sends keeps
    // flowing out whatever the handlers are doing.
    let mut pending: Option<Event> = None;
    let stopping = application.execution().stopping().clone();
    loop {
        tokio::select! {
            out = queued.recv() => match out {
                Some(Outbound::Text(text)) => {
                    if frames.write_frame(Frame::text(Payload::Owned(text.into_bytes()))).await.is_err() {
                        pending = Some(Event::Close(1006, String::new(), false));
                        break;
                    }
                }
                Some(Outbound::Binary(bytes)) => {
                    if frames.write_frame(Frame::binary(Payload::Owned(bytes))).await.is_err() {
                        pending = Some(Event::Close(1006, String::new(), false));
                        break;
                    }
                }
                Some(Outbound::Close(code, reason)) => {
                    let _ = frames.write_frame(Frame::close(code, reason.as_bytes())).await;
                    pending = Some(Event::Close(code, reason, true));
                    break;
                }
                // The handlers are gone: an error closed the socket.
                None => break
            },
            permit = events.reserve(), if pending.is_some() => match permit {
                Ok(permit) => permit.send(pending.take().expect("guarded")),
                Err(_) => break
            },
            frame = frames.read_frame(), if pending.is_none() => {
                let frame = match frame {
                    Ok(frame) => frame,
                    Err(error) => {
                        debug!(socket = id, %error, "Lua WebSocket transport ended");
                        pending = Some(Event::Close(1006, String::new(), false));
                        break;
                    }
                };
                if frame.payload.len() > config.max_message_size {
                    let _ = frames.write_frame(Frame::close(1009, b"message too large")).await;
                    pending = Some(Event::Close(1009, "message too large".to_owned(), false));
                    break;
                }
                match frame.opcode {
                    OpCode::Text => {
                        if std::str::from_utf8(&frame.payload).is_err() {
                            let _ = frames.write_frame(Frame::close(1007, b"text is not UTF-8")).await;
                            pending = Some(Event::Close(1007, "text is not UTF-8".to_owned(), false));
                            break;
                        }
                        pending = Some(Event::Message(Vec::from(frame.payload), false));
                    }
                    OpCode::Binary => pending = Some(Event::Message(Vec::from(frame.payload), true)),
                    OpCode::Close => {
                        let (code, reason) = close_details(&frame.payload);
                        pending = Some(Event::Close(code, reason, true));
                        break;
                    }
                    _ => {}
                }
            },
            () = stopping.cancelled() => {
                let _ = frames.write_frame(Frame::close(1001, b"application stopping")).await;
                break;
            }
        }
    }
    // The close event, when there is one, is the handlers' last; they are
    // waited for, so a close handler finishes before the session ends.
    if let Some(event) = pending.take()
        && let Ok(permit) = events.reserve().await
    {
        permit.send(event);
    }
    drop(events);
    let _ = handling.await;
}

/// Run a socket's handlers: `open`, then each event in order.
async fn handle_events(
    application: Arc<LuaApplication>,
    socket: LuaSocket,
    mut happened: mpsc::Receiver<Event>,
) {
    let outbound = socket.outbound.clone();
    let id = socket.id;
    let run = async {
        let lua = application.lua();
        let handlers = application.websocket_handlers()?.ok_or_else(|| {
            failure("the application no longer serves WebSockets")
        })?;
        let socket = lua.create_userdata(socket)?;
        call(&application, &handlers, "open", (socket.clone(),)).await?;
        while let Some(event) = happened.recv().await {
            match event {
                Event::Message(data, binary) => {
                    let data = lua.create_string(data)?;
                    call(
                        &application,
                        &handlers,
                        "message",
                        (socket.clone(), data, binary),
                    )
                    .await?;
                }
                Event::Close(code, reason, clean) => {
                    call(
                        &application,
                        &handlers,
                        "close",
                        (socket.clone(), code, reason, clean),
                    )
                    .await?;
                    break;
                }
            }
        }
        Ok::<(), mlua::Error>(())
    };
    if let Err(error) = run.await {
        warn!(application = %application.name(), socket = id, "Lua WebSocket handler failed: {error}");
        let _ = outbound
            .send(Outbound::Close(1011, "application error".to_owned()))
            .await;
    }
}

/// Call one handler, when the application has it, holding a permit while it
/// runs. A handler waits for a permit as long as it takes: a socket's events
/// are not refused, they are only ordered behind the application's work.
async fn call(
    application: &Arc<LuaApplication>,
    handlers: &Table,
    name: &str,
    arguments: impl mlua::IntoLuaMulti,
) -> mlua::Result<()> {
    let Some(handler) = handlers.get::<Option<Function>>(name)? else {
        return Ok(());
    };
    let _permit = tokio::select! {
        permit = Arc::clone(application.semaphore()).acquire_owned() => {
            permit.map_err(|_| failure("the application stopped"))?
        }
        () = application.execution().stopping().cancelled() => {
            return Err(failure("the application is stopping"));
        }
    };
    tokio::select! {
        done = handler.call_async::<Value>(arguments) => done.map(|_| ()),
        () = application.execution().stopping().cancelled() => {
            Err(failure("the application is stopping"))
        }
    }
}

fn close_details(payload: &[u8]) -> (u16, String) {
    if payload.len() < 2 {
        return (1005, String::new());
    }
    let code = u16::from_be_bytes([payload[0], payload[1]]);
    (code, String::from_utf8_lossy(&payload[2..]).into_owned())
}
