use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

use claviron_pusher::{
    PresenceMember, PresenceSnapshot, PublishedEvent, ServerMessage,
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub(crate) struct ChannelPeer {
    pub sender: mpsc::Sender<String>,
    pub cancellation: CancellationToken,
    pub app_id: Arc<str>,
    pub member: Option<PresenceMember>,
}

impl ChannelPeer {
    #[cfg(test)]
    fn test(
        sender: mpsc::Sender<String>,
        cancellation: CancellationToken,
    ) -> Self {
        Self { sender, cancellation, app_id: Arc::from("1"), member: None }
    }
}

type Subscribers = HashMap<Arc<str>, ChannelPeer>;
type Channels = HashMap<Arc<str>, Subscribers>;
#[derive(Clone)]
struct PresenceRecord {
    member: PresenceMember,
    origin_node: u16,
}

type PresenceSockets = HashMap<Arc<str>, PresenceRecord>;
type PresenceChannels = HashMap<Arc<str>, PresenceSockets>;
pub(crate) type LocalPresenceEntry =
    (Arc<str>, Arc<str>, Arc<str>, PresenceMember);
pub(crate) type LocalSubscriptionEntry =
    (Arc<str>, Arc<str>, Arc<str>, Option<PresenceMember>);

#[derive(Default)]
pub(crate) struct ChannelRegistry {
    applications: RwLock<HashMap<Arc<str>, Channels>>,
    presence: RwLock<HashMap<Arc<str>, PresenceChannels>>,
}

impl ChannelRegistry {
    pub fn subscribe(
        &self,
        application: Arc<str>,
        channel: Arc<str>,
        socket_id: Arc<str>,
        peer: ChannelPeer,
    ) {
        self.applications
            .write()
            .expect("Broadcast channel registry lock poisoned")
            .entry(application)
            .or_default()
            .entry(channel)
            .or_default()
            .insert(socket_id, peer);
    }

    pub fn unsubscribe(
        &self,
        application: &str,
        channel: &str,
        socket_id: &str,
    ) {
        let mut applications = self
            .applications
            .write()
            .expect("Broadcast channel registry lock poisoned");
        let Some(channels) = applications.get_mut(application) else {
            return;
        };
        let Some(subscribers) = channels.get_mut(channel) else {
            return;
        };
        subscribers.remove(socket_id);
        if subscribers.is_empty() {
            channels.remove(channel);
        }
        if channels.is_empty() {
            applications.remove(application);
        }
    }

    pub fn join_presence(
        &self,
        application: Arc<str>,
        channel: Arc<str>,
        socket_id: Arc<str>,
        member: PresenceMember,
        origin_node: u16,
    ) -> crate::Result<PresenceSnapshot> {
        let (snapshot, first_for_user) = {
            let mut applications = self
                .presence
                .write()
                .expect("Broadcast presence registry lock poisoned");
            let members = applications
                .entry(Arc::clone(&application))
                .or_default()
                .entry(Arc::clone(&channel))
                .or_default();
            let first_for_user = !members
                .values()
                .any(|known| known.member.user_id == member.user_id);
            members.insert(
                socket_id,
                PresenceRecord { member: member.clone(), origin_node },
            );
            (presence_snapshot(members), first_for_user)
        };
        if first_for_user {
            self.broadcast_message(
                &application,
                &channel,
                &ServerMessage::presence_member_added(&channel, &member)?,
                None,
            );
        }
        Ok(snapshot)
    }

    pub fn leave_presence(
        &self,
        application: &str,
        channel: &str,
        socket_id: &str,
    ) -> crate::Result<bool> {
        let removed = {
            let mut applications = self
                .presence
                .write()
                .expect("Broadcast presence registry lock poisoned");
            let Some(channels) = applications.get_mut(application) else {
                return Ok(false);
            };
            let Some(members) = channels.get_mut(channel) else {
                return Ok(false);
            };
            let Some(member) = members.remove(socket_id) else {
                return Ok(false);
            };
            let last_for_user = !members
                .values()
                .any(|known| known.member.user_id == member.member.user_id);
            if members.is_empty() {
                channels.remove(channel);
            }
            if channels.is_empty() {
                applications.remove(application);
            }
            last_for_user.then_some(member)
        };
        if let Some(member) = removed {
            self.broadcast_message(
                application,
                channel,
                &ServerMessage::presence_member_removed(
                    channel,
                    &member.member.user_id,
                )?,
                Some(socket_id),
            );
        }
        Ok(true)
    }

    pub fn reset_presence_node(
        &self,
        origin_node: u16,
    ) -> crate::Result<()> {
        let mut removed = Vec::<(Arc<str>, Arc<str>, String)>::new();
        {
            let mut applications = self
                .presence
                .write()
                .expect("Broadcast presence registry lock poisoned");
            for (application, channels) in applications.iter_mut() {
                for (channel, members) in channels.iter_mut() {
                    let candidate_users = members
                        .values()
                        .filter(|record| record.origin_node == origin_node)
                        .map(|record| record.member.user_id.clone())
                        .collect::<HashSet<_>>();
                    members
                        .retain(|_, record| record.origin_node != origin_node);
                    for user_id in candidate_users {
                        if !members
                            .values()
                            .any(|record| record.member.user_id == user_id)
                        {
                            removed.push((
                                Arc::clone(application),
                                Arc::clone(channel),
                                user_id,
                            ));
                        }
                    }
                }
                channels.retain(|_, members| !members.is_empty());
            }
            applications.retain(|_, channels| !channels.is_empty());
        }
        for (application, channel, user_id) in removed {
            self.broadcast_message(
                &application,
                &channel,
                &ServerMessage::presence_member_removed(&channel, &user_id)?,
                None,
            );
        }
        Ok(())
    }

    pub fn local_presence(
        &self,
        origin_node: u16,
    ) -> Vec<LocalPresenceEntry> {
        let applications = self
            .presence
            .read()
            .expect("Broadcast presence registry lock poisoned");
        let mut local = Vec::new();
        for (application, channels) in applications.iter() {
            for (channel, members) in channels.iter() {
                for (socket_id, record) in members {
                    if record.origin_node == origin_node {
                        local.push((
                            Arc::clone(application),
                            Arc::clone(channel),
                            Arc::clone(socket_id),
                            record.member.clone(),
                        ));
                    }
                }
            }
        }
        local
    }

    pub fn local_subscriptions(&self) -> Vec<LocalSubscriptionEntry> {
        let applications = self
            .applications
            .read()
            .expect("Broadcast channel registry lock poisoned");
        let mut local = Vec::new();
        for channels in applications.values() {
            for (channel, subscribers) in channels {
                for (socket_id, peer) in subscribers {
                    local.push((
                        Arc::clone(&peer.app_id),
                        Arc::clone(channel),
                        Arc::clone(socket_id),
                        peer.member.clone(),
                    ));
                }
            }
        }
        local
    }

    pub fn publish(
        &self,
        application: &str,
        event: &PublishedEvent,
    ) -> crate::Result<usize> {
        let mut delivered = 0;
        let mut applications = self
            .applications
            .write()
            .expect("Broadcast channel registry lock poisoned");
        let Some(channels) = applications.get_mut(application) else {
            return Ok(0);
        };

        for channel in &event.channels {
            let Some(subscribers) = channels.get_mut(channel.as_str()) else {
                continue;
            };
            let message = ServerMessage::channel_event(
                &event.name,
                channel,
                event.data.clone(),
            )?;
            subscribers.retain(|socket_id, peer| {
                if event.socket_id.as_deref() == Some(socket_id.as_ref()) {
                    return true;
                }
                match peer.sender.try_send(message.clone()) {
                    Ok(()) => {
                        delivered += 1;
                        true
                    }
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        peer.cancellation.cancel();
                        false
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => false,
                }
            });
        }
        channels.retain(|_, subscribers| !subscribers.is_empty());
        Ok(delivered)
    }

    fn broadcast_message(
        &self,
        application: &str,
        channel: &str,
        message: &str,
        excluded_socket: Option<&str>,
    ) {
        let mut applications = self
            .applications
            .write()
            .expect("Broadcast channel registry lock poisoned");
        let Some(subscribers) = applications
            .get_mut(application)
            .and_then(|channels| channels.get_mut(channel))
        else {
            return;
        };
        subscribers.retain(|socket_id, peer| {
            if excluded_socket == Some(socket_id.as_ref()) {
                return true;
            }
            match peer.sender.try_send(message.to_owned()) {
                Ok(()) => true,
                Err(mpsc::error::TrySendError::Full(_)) => {
                    peer.cancellation.cancel();
                    false
                }
                Err(mpsc::error::TrySendError::Closed(_)) => false,
            }
        });
    }
}

fn presence_snapshot(members: &PresenceSockets) -> PresenceSnapshot {
    let mut hash = std::collections::BTreeMap::new();
    for record in members.values() {
        hash.entry(record.member.user_id.clone())
            .or_insert_with(|| record.member.user_info.clone());
    }
    PresenceSnapshot {
        ids: hash.keys().cloned().collect(),
        count: hash.len(),
        hash,
    }
}

#[cfg(test)]
#[path = "../tests/channels.rs"]
mod tests;
