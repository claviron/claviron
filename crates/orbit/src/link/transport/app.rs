//! Application events across a link: this fleet's application ring bound to
//! the link segment it shares with another fleet.
//!
//! The rules live in `orbitive::link::LinkEvents` — forwarded once at the
//! origin, received by one lane, never sent back. This adds what Claviron
//! knows: which ring (the application ring), how an event is routed (by the
//! domain topic inside the envelope, supplied by the crate that owns it), and
//! what this process wants (every live subscription, from [`LinkInterest`]).

use std::sync::Arc;

use claviron_runtime::{AppState, ProcessEventBus};
use orbitive::link::{DirectoryWatcher, LinkEvents, Routing};
use tracing::warn;

use super::{LinkInterest, TransportSegment};
use crate::{ApplicationEventLayout, ApplicationEventRegistry};

/// How application events are matched against interest across a link.
/// Inserted by the crate that owns the application-event envelope; without it
/// nothing crosses.
#[derive(Clone, Copy)]
pub struct LinkEventRouting(pub Routing);

/// What a bridge needs from this process, gathered once.
#[derive(Clone)]
pub struct ApplicationLinkSetup {
    registry: Arc<ApplicationEventRegistry>,
    routing: Routing,
    interest: Arc<LinkInterest>,
    events: ProcessEventBus,
}

impl ApplicationLinkSetup {
    /// `None` when this process has no application ring or no envelope
    /// routing: then there is nothing to bridge.
    pub fn resolve(state: &AppState) -> Option<Self> {
        let registry = state.registry_ref();
        Some(Self {
            registry: registry.resolve::<ApplicationEventRegistry>()?,
            routing: registry.resolve::<LinkEventRouting>()?.0,
            interest: registry.resolve::<LinkInterest>()?,
            events: state.events().bus().clone(),
        })
    }

    /// Bind through the lane this process holds in `segment`. `far` names the
    /// other fleet's roles.
    pub fn for_lane(
        &self,
        segment: &TransportSegment,
        lane: usize,
        far: fn(&str) -> bool,
    ) -> Option<ApplicationLink> {
        let bus = self.registry.bus()?;
        match LinkEvents::for_lane(segment.link(), lane, bus, far, self.routing)
        {
            Ok(bridge) => Some(self.start(bridge)),
            Err(error) => {
                warn!("application events cannot cross this link: {error}");
                None
            }
        }
    }

    /// Bind through a directory watcher: a process that holds no lane still
    /// declares what it wants and forwards what it publishes.
    pub fn for_watcher(
        &self,
        watcher: &DirectoryWatcher,
        far: fn(&str) -> bool,
    ) -> Option<ApplicationLink> {
        let bus = self.registry.bus()?;
        Some(self.start(LinkEvents::for_watcher(
            watcher,
            bus,
            far,
            self.routing,
        )))
    }

    fn start(
        &self,
        bridge: LinkEvents<ApplicationEventLayout>,
    ) -> ApplicationLink {
        let bridge = Arc::new(bridge);
        let mut interest = self.interest.watch();
        bridge.interest(&interest.borrow_and_update()[..]);
        let driving = Arc::clone(&bridge);
        let driver = tokio::spawn(async move {
            let drive = driving.drive();
            tokio::pin!(drive);
            loop {
                tokio::select! {
                    result = &mut drive => {
                        if let Err(error) = result {
                            warn!("application events stopped crossing the link: {error}");
                        }
                        return;
                    }
                    changed = interest.changed() => {
                        if changed.is_err() {
                            // Nobody can declare interest any more; keep forwarding.
                            if let Err(error) = drive.await {
                                warn!("application events stopped crossing the link: {error}");
                            }
                            return;
                        }
                        driving.interest(&interest.borrow_and_update()[..]);
                    }
                }
            }
        });
        ApplicationLink {
            bridge,
            registry: Arc::clone(&self.registry),
            events: self.events.clone(),
            driver,
        }
    }
}

/// A running bridge. Dropping it stops forwarding and takes back this
/// process's interest.
pub struct ApplicationLink {
    bridge: Arc<LinkEvents<ApplicationEventLayout>>,
    registry: Arc<ApplicationEventRegistry>,
    events: ProcessEventBus,
    driver: tokio::task::JoinHandle<()>,
}

impl ApplicationLink {
    /// An event frame read from this process's lane: into the fleet's ring
    /// for the others, and to this process's own subscribers.
    pub fn accept(
        &self,
        frame: &[u8],
    ) {
        match self.bridge.accept(frame) {
            Ok((topic, payload)) => {
                if let Err(error) =
                    self.registry.deliver(&self.events, topic, payload)
                {
                    warn!(
                        topic,
                        "an application event from across the link could not be delivered here: {error}"
                    );
                }
            }
            Err(error) => {
                warn!(
                    "an application event frame could not be accepted: {error}"
                )
            }
        }
    }
}

impl Drop for ApplicationLink {
    fn drop(&mut self) {
        self.driver.abort();
    }
}
