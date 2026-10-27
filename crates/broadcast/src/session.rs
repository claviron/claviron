//! One admitted session: the Pusher conversation with one client, over
//! whichever [`MessageStream`] carries it.

use std::collections::HashSet;
use std::future::Future;
use std::sync::Arc;

use claviron_pusher::{
    ClientMessage, PresenceMember, PusherAppId, PusherAppKey, PusherAppSecret,
    ServerMessage, TriggerEvent, subscription, unsubscribe,
    verify_channel_authorization,
};
use claviron_runtime::Permit;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::channels::ChannelPeer;
use crate::fleet::FleetFanout;
use crate::service::Generation;
use crate::stream::{Message, MessageStream};

/// An admitted session, retaining its Broadcast generation until the message loop ends.
pub struct BroadcastSession {
    pub(crate) application: Arc<str>,
    pub(crate) app_id: PusherAppId,
    pub(crate) key: PusherAppKey,
    pub(crate) secret: Option<PusherAppSecret>,
    pub(crate) client_events: bool,
    pub(crate) socket_id: Arc<str>,
    pub(crate) cancellation: CancellationToken,
    pub(crate) generation: Arc<Generation>,
    pub(crate) outbound: mpsc::Receiver<String>,
    pub(crate) sender: mpsc::Sender<String>,
    pub(crate) _permit: Permit,
    pub(crate) authenticator: Option<Arc<dyn crate::SessionAuthenticator>>,
    pub(crate) authenticated: bool,
}

impl BroadcastSession {
    /// Run under the caller's task ownership; no session work is detached.
    pub async fn run<S: MessageStream>(
        self,
        socket: S,
    ) -> crate::Result<()> {
        self.run_with_shutdown(socket, std::future::pending()).await
    }

    /// Observe service stop while the native transport is still waiting for IO.
    pub async fn wait_shutdown(&self) {
        self.cancellation.cancelled().await;
    }

    /// Share the same protocol loop with the ingress owner's graceful signal.
    /// Subscription cleanup runs before returning; no task or timer is added.
    pub async fn run_with_shutdown<S: MessageStream>(
        self,
        mut socket: S,
        shutdown: impl Future<Output = ()> + Send,
    ) -> crate::Result<()> {
        run_messages(&mut socket, self, shutdown).await
    }
}

async fn run_messages<S: MessageStream>(
    socket: &mut S,
    mut session: BroadcastSession,
    shutdown: impl Future<Output = ()> + Send,
) -> crate::Result<()> {
    if session.authenticator.is_some() && !session.authenticated {
        return Err(crate::Error::SessionRequired);
    }
    tokio::pin!(shutdown);
    let mut subscriptions = HashSet::<Arc<str>>::new();

    let result: crate::Result<()> = async {
        if !write_text(socket, ServerMessage::connection_established(&session.socket_id)?, &session, &mut shutdown).await? {
            return Ok(());
        }
        debug!(application = %session.application, socket_id = %session.socket_id, "Pusher connection established");
        loop {
            tokio::select! {
                _ = &mut shutdown => {
                    let _ = socket.send(Message::Close { code: 1001, reason: "listener stopped".to_owned() }).await;
                    break Ok(());
                }
                _ = session.cancellation.cancelled() => {
                    let _ = socket.send(Message::Close { code: 1001, reason: "service restart".to_owned() }).await;
                    break Ok(());
                }
                outbound = session.outbound.recv() => {
                    let Some(outbound) = outbound else {
                        break Ok(());
                    };
                    if !write_text(socket, outbound, &session, &mut shutdown).await? { break Ok(()) }
                }
                frame = socket.receive() => {
                    match frame? {
                        Some(Message::Text(text)) => {
                            let message: ClientMessage = serde_json::from_str(&text)?;
                            if message.event == "pusher:ping" {
                                if !write_text(socket, ServerMessage::pong()?, &session, &mut shutdown).await? { break Ok(()) }
                            } else if let Some(request) = subscription(&message) {
                                let response = if request.channel.starts_with("presence-") {
                                    presence_subscription(
                                        &session,
                                        &mut subscriptions,
                                        &request
                                    )?
                                } else if request.channel.starts_with("private-") {
                                    let authorized = session.secret.as_ref().is_some_and(|secret| {
                                        verify_channel_authorization(
                                            request.auth.as_deref(),
                                            &session.socket_id,
                                            &request.channel,
                                            request.channel_data.as_deref(),
                                            &session.key,
                                            secret
                                        ).is_ok()
                                    });
                                    if !authorized {
                                        ServerMessage::subscription_error(
                                            &request.channel,
                                            "invalid channel authorization"
                                        )?
                                    } else {
                                        subscribe_session(
                                            &session,
                                            &mut subscriptions,
                                            &request.channel,
                                            None
                                        );
                                        ServerMessage::subscription_succeeded(&request.channel)?
                                    }
                                } else {
                                    subscribe_session(
                                        &session,
                                        &mut subscriptions,
                                        &request.channel,
                                        None
                                    );
                                    ServerMessage::subscription_succeeded(&request.channel)?
                                };
                                if !write_text(socket, response, &session, &mut shutdown).await? { break Ok(()) }
                            } else if let Some(request) = unsubscribe(&message) {
                                if subscriptions.remove(request.channel.as_str()) {
                                    unsubscribe_session(&session, &request.channel);
                                }
                            } else if message.event.starts_with("client-") {
                                publish_client_event(&session, &subscriptions, message)?;
                            }
                        }
                        Some(message @ Message::Close { .. }) => {
                            let _ = socket.send(message).await;
                            break Ok(());
                        }
                        None => break Ok(()),
                        Some(Message::Binary(_)) => {}
                    }
                }
            }
        }
    }
    .await;
    for channel in subscriptions {
        unsubscribe_session(&session, &channel);
    }
    debug!(application = %session.application, socket_id = %session.socket_id, "Pusher connection closed");
    result
}

/// The same membership release for a wire leave and final socket cleanup.
fn unsubscribe_session(
    session: &BroadcastSession,
    channel: &str,
) {
    if session
        .generation
        .channels
        .leave_presence(&session.application, channel, &session.socket_id)
        .unwrap_or(false)
        && let Some(fleet) = &session.generation.fleet
        && let Err(error) = fleet.publish_presence_left(
            &session.application,
            channel,
            &session.socket_id,
        )
    {
        tracing::warn!(%error, "Pusher presence leave could not reach the fleet");
    }
    session.generation.channels.unsubscribe(
        &session.application,
        channel,
        &session.socket_id,
    );
    if let Some(fleet) = &session.generation.fleet
        && let Err(error) = fleet.publish_subscription_left(
            session.app_id.as_str(),
            channel,
            &session.socket_id,
        )
    {
        tracing::warn!(%error, "Pusher subscription leave could not reach the occupancy owner");
    }
}

fn presence_subscription(
    session: &BroadcastSession,
    subscriptions: &mut HashSet<Arc<str>>,
    request: &claviron_pusher::SubscriptionRequest,
) -> crate::Result<String> {
    let Some(secret) = session.secret.as_ref() else {
        return Ok(ServerMessage::subscription_error(
            &request.channel,
            "presence authorization is not configured",
        )?);
    };
    if verify_channel_authorization(
        request.auth.as_deref(),
        &session.socket_id,
        &request.channel,
        request.channel_data.as_deref(),
        &session.key,
        secret,
    )
    .is_err()
    {
        return Ok(ServerMessage::subscription_error(
            &request.channel,
            "invalid channel authorization",
        )?);
    }
    let member = PresenceMember::parse(
        request.channel_data.as_deref().unwrap_or_default(),
    )?;
    if let Some(fleet) = &session.generation.fleet {
        fleet.publish_presence_joined(
            &session.application,
            &request.channel,
            &session.socket_id,
            &member,
        )?;
    }
    let snapshot = session.generation.channels.join_presence(
        Arc::clone(&session.application),
        Arc::from(request.channel.as_str()),
        Arc::clone(&session.socket_id),
        member.clone(),
        session.generation.fleet.as_ref().map_or(0, FleetFanout::node_id),
    )?;
    subscribe_session(session, subscriptions, &request.channel, Some(member));
    Ok(ServerMessage::presence_subscription_succeeded(
        &request.channel,
        snapshot,
    )?)
}

fn publish_client_event(
    session: &BroadcastSession,
    subscriptions: &HashSet<Arc<str>>,
    message: ClientMessage,
) -> crate::Result<()> {
    let Some(channel) = message.channel else {
        return Ok(());
    };
    if !session.client_events
        || !(channel.starts_with("private-")
            || channel.starts_with("presence-"))
        || !subscriptions.contains(channel.as_str())
    {
        return Ok(());
    }
    let event = TriggerEvent {
        name: message.event,
        data: message.data,
        channels: Vec::new(),
        channel: Some(channel),
        socket_id: Some(session.socket_id.to_string()),
        info: None,
    }
    .validate()?;
    if let Some(fleet) = &session.generation.fleet {
        fleet.publish(&session.application, &event)?;
    }
    session.generation.channels.publish(&session.application, &event)?;
    Ok(())
}

fn subscribe_session(
    session: &BroadcastSession,
    subscriptions: &mut HashSet<Arc<str>>,
    channel: &str,
    member: Option<PresenceMember>,
) {
    let channel = Arc::<str>::from(channel);
    if !subscriptions.insert(Arc::clone(&channel)) {
        return;
    }
    session.generation.channels.subscribe(
        Arc::clone(&session.application),
        Arc::clone(&channel),
        Arc::clone(&session.socket_id),
        ChannelPeer {
            sender: session.sender.clone(),
            cancellation: session.cancellation.clone(),
            app_id: Arc::from(session.app_id.as_str()),
            member: member.clone(),
        },
    );
    if let Some(fleet) = &session.generation.fleet
        && let Err(error) = fleet.publish_subscription_joined(
            session.app_id.as_str(),
            &channel,
            &session.socket_id,
            member.as_ref(),
        )
    {
        tracing::warn!(%error, "Pusher subscription join could not reach the occupancy owner");
    }
}

async fn write_text<S: MessageStream>(
    socket: &mut S,
    text: String,
    session: &BroadcastSession,
    shutdown: &mut (impl Future<Output = ()> + Unpin + Send),
) -> crate::Result<bool> {
    tokio::select! {
        biased;
        _ = session.cancellation.cancelled() => Ok(false),
        _ = shutdown => Ok(false),
        sent = socket.send(Message::Text(text)) => sent.map(|()| true),
    }
}

#[cfg(test)]
#[path = "../tests/session.rs"]
mod tests;
