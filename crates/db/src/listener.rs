//! Postgres `LISTEN` for a runtime component, without a clock of its own.
//!
//! The connection is taken from one of the application's pools and held while
//! the listener lives; when it drops, sqlx takes the next one from the same
//! pool and subscribes to the same channels again. Notifications sent while
//! the connection was down are lost, so a gap is reported as
//! [`Notice::Resync`]: the consumer reads its state again from the tables.
//! When the database does not answer, nothing is retried on a timer — the
//! listener waits for the pool's [`PostgresConnected`] and goes on from there.
//!
//! ```ignore
//! let mut channel = ChannelListener::open(&state, &pool, &["hosting_certificate_changed"]).await;
//! loop {
//!     let notice = tokio::select! {
//!         _ = context.cancelled() => break,
//!         notice = channel.next() => notice,
//!     };
//!     let applied = match notice {
//!         Notice::Changed(notes) => apply(notes).await,
//!         Notice::Resync => reload_everything().await,
//!     };
//!     if applied.is_err() {
//!         channel.missed();
//!     }
//! }
//! ```

use claviron_runtime::AppState;
use sqlx::PgPool;
use sqlx::postgres::{PgListener, PgNotification};
use tokio::sync::broadcast;
use tracing::warn;

use crate::provider::PostgresConnected;

/// What a listener has to act on next.
pub enum Notice {
    /// One or more notifications, all that had arrived together.
    Changed(Vec<PgNotification>),
    /// Notifications may have been lost: read the state again.
    Resync,
}

pub struct ChannelListener {
    listener: PgListener,
    connected: broadcast::Receiver<PostgresConnected>,
    /// A resync is owed to the consumer.
    resync: bool,
    /// The database did not answer: wait for a connection before resyncing.
    wait: bool,
    /// The last notice handed out was a resync.
    resyncing: bool,
}

impl ChannelListener {
    /// Subscribe to `channels` over a connection from `pool`. If the database
    /// does not answer, waits for the pool to report a connection and tries
    /// again; the caller ends the wait by dropping the future.
    pub async fn open(
        state: &AppState,
        pool: &PgPool,
        channels: &[&str],
    ) -> Self {
        let mut connected =
            state.events().bus().subscribe::<PostgresConnected>();
        loop {
            match subscribe(pool, channels).await {
                Ok(listener) => {
                    return Self {
                        listener,
                        connected,
                        resync: false,
                        wait: false,
                        resyncing: false,
                    };
                }
                Err(error) => {
                    warn!(
                        "LISTEN {channels:?}: {error}; waiting for the database"
                    );
                    wait_for_connection(&mut connected).await;
                }
            }
        }
    }

    /// The next thing to act on. Meant to sit in a `select!` beside the
    /// cancellation that ends the listener: the owed resync survives a
    /// dropped call, a half-read notification is not promised to.
    pub async fn next(&mut self) -> Notice {
        loop {
            if self.resync {
                if self.wait {
                    wait_for_connection(&mut self.connected).await;
                    self.wait = false;
                }
                self.resync = false;
                self.resyncing = true;
                return Notice::Resync;
            }
            match self.listener.try_recv().await {
                Ok(Some(first)) => {
                    let mut notes = vec![first];
                    while let Some(more) = self.listener.next_buffered() {
                        notes.push(more);
                    }
                    self.resyncing = false;
                    return Notice::Changed(notes);
                }
                // The connection dropped; sqlx has already asked the pool for
                // another. Whatever happened meanwhile is read again.
                Ok(None) => {
                    warn!("LISTEN connection lost; resyncing");
                    self.resync = true;
                }
                Err(error) => {
                    warn!("LISTEN: {error}; waiting for the database");
                    self.resync = true;
                    self.wait = true;
                }
            }
        }
    }

    /// The consumer could not apply what it was given. After a change, the
    /// next [`next`] asks for a resync at once; after a resync that failed,
    /// it waits for the database to answer first, so a database that is down
    /// is not asked again and again.
    ///
    /// [`next`]: Self::next
    pub fn missed(&mut self) {
        self.resync = true;
        self.wait = self.resyncing;
    }
}

async fn subscribe(
    pool: &PgPool,
    channels: &[&str],
) -> sqlx::Result<PgListener> {
    let mut listener = PgListener::connect_with(pool).await?;
    listener.listen_all(channels.iter().copied()).await?;
    Ok(listener)
}

/// Wait for the pool to open a connection. A lagged receiver still means
/// one was opened; a closed bus means the process is going away, and the
/// caller's cancellation will end its loop.
async fn wait_for_connection(
    connected: &mut broadcast::Receiver<PostgresConnected>
) {
    match connected.recv().await {
        Ok(_) | Err(broadcast::error::RecvError::Lagged(_)) => {}
        Err(broadcast::error::RecvError::Closed) => {
            std::future::pending().await
        }
    }
}
