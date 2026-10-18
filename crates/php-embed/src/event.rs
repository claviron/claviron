//! PHP adapter for Claviron's language-neutral application events.
//!
//! A subscription belongs to one SAPI execution context, so request-pool
//! thread reuse cannot leak topics into a later request. Persistent
//! applications multiplex this receiver with their HTTP inbox; tasks and
//! one-shot scripts use the explicit receive primitive.

use std::collections::{HashMap, HashSet, VecDeque};
use std::future::Future;
use std::sync::{Arc, OnceLock, RwLock};
use std::time::Duration;

use arc_swap::ArcSwapOption;
use claviron_app::{ApplicationEvent, ApplicationId, EventValue};
use claviron_orbit::ApplicationEventRegistry;
use claviron_orbit::transport::{InterestGuard, LinkInterest};
use claviron_runtime::{AppState, ProcessEventBus};
use ext_php_rs::convert::IntoZval;
use ext_php_rs::flags::ClassFlags;
use ext_php_rs::types::{ZendClassObject, Zval};
use ext_php_rs::{php_class, php_impl};
use tokio::runtime::Handle;
use tokio::sync::{broadcast, mpsc};
use tokio_util::sync::CancellationToken;

pub(crate) mod value;

pub(crate) use value::from_zval;

/// Immutable application event returned by `claviron_event_receive()` and by
/// a subscribed persistent application's ordinary receive loop.
#[php_class]
#[php(name = "Claviron\\Runtime\\Event")]
#[php(flags = ClassFlags::from_bits_retain(
    ClassFlags::Final.bits()
        | ClassFlags::NoDynamicProperties.bits()
        | ClassFlags::NotSerializable.bits()
))]
pub(crate) struct PhpEventObject {
    id: String,
    topic: String,
    payload: EventValue,
}

#[php_impl]
impl PhpEventObject {
    #[php(getter)]
    pub fn id(&self) -> String {
        self.id.clone()
    }

    #[php(getter)]
    pub fn topic(&self) -> String {
        self.topic.clone()
    }

    #[php(getter)]
    pub fn payload(&self) -> Zval {
        let mut result = Zval::new();
        if value::set_zval(&self.payload, &mut result).is_err() {
            result.set_null();
        }
        result
    }
}

#[derive(Debug)]
pub(crate) struct ReceivedEvent(ApplicationEvent);

impl ReceivedEvent {
    fn topic(&self) -> &str {
        self.0.topic().as_str()
    }

    pub(crate) fn set_zval(
        self,
        result: &mut Zval,
    ) -> Result<(), String> {
        let (id, _, topic, payload) = self.0.into_parts();
        ZendClassObject::new(PhpEventObject {
            id: id.to_string(),
            topic: topic.to_string(),
            payload,
        })
        .set_zval(result, false)
        .map_err(|error| error.to_string())
    }
}

pub(crate) struct EventSubscription {
    application: Option<ApplicationId>,
    topics: Arc<RwLock<HashSet<String>>>,
    receiver: broadcast::Receiver<ApplicationEvent>,
    deferred: VecDeque<ReceivedEvent>,
    /// What this subscription declares across links, topic by topic: a
    /// subscription is what says this process can take the event.
    interest: Option<Arc<LinkInterest>>,
    declared: HashMap<String, InterestGuard>,
}

impl EventSubscription {
    fn new(
        events: &ProcessEventBus,
        application: Option<ApplicationId>,
        interest: Option<Arc<LinkInterest>>,
        capacity: usize,
    ) -> Self {
        let topics = Arc::new(RwLock::new(HashSet::new()));
        let selected = Arc::clone(&topics);
        let target = application.clone();
        // Admission is synchronous with publication. Filtering after receiving
        // would already have let unrelated traffic exhaust the bounded inbox.
        let receiver = events
            .subscribe_filtered_with_capacity::<ApplicationEvent>(
                capacity,
                move |event| {
                    selected
                        .read()
                        .expect("PHP event topics lock poisoned")
                        .contains(event.topic().as_str())
                        && event.target().is_none_or(|application| {
                            target.as_ref() == Some(application)
                        })
                },
            );
        Self {
            application,
            topics,
            receiver,
            deferred: VecDeque::new(),
            interest,
            declared: HashMap::new(),
        }
    }

    pub(crate) fn subscribe(
        &mut self,
        topic: String,
    ) -> bool {
        if !self
            .topics
            .write()
            .expect("PHP event topics lock poisoned")
            .insert(topic.clone())
        {
            return false;
        }
        self.declare(topic);
        true
    }

    pub(crate) fn subscribe_many(
        &mut self,
        topics: impl IntoIterator<Item = String>,
    ) -> usize {
        topics.into_iter().filter(|topic| self.subscribe(topic.clone())).count()
    }

    pub(crate) fn unsubscribe(
        &mut self,
        topic: &str,
    ) -> bool {
        self.declared.remove(topic);
        self.topics
            .write()
            .expect("PHP event topics lock poisoned")
            .remove(topic)
    }

    fn declare(
        &mut self,
        topic: String,
    ) {
        if let Some(interest) = &self.interest {
            let guard = interest.add([topic.clone()]);
            self.declared.insert(topic, guard);
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.topics.read().expect("PHP event topics lock poisoned").is_empty()
    }

    pub(crate) fn defer(
        &mut self,
        event: ReceivedEvent,
    ) {
        self.deferred.push_front(event);
    }

    fn accepts(
        &self,
        event: &ReceivedEvent,
    ) -> bool {
        self.topics
            .read()
            .expect("PHP event topics lock poisoned")
            .contains(event.topic())
            && match event.0.target() {
                Some(target) => self.application.as_ref() == Some(target),
                None => true,
            }
    }

    pub(crate) fn try_receive(
        &mut self
    ) -> Result<Option<ReceivedEvent>, String> {
        while let Some(event) = self.deferred.pop_front() {
            if self.accepts(&event) {
                return Ok(Some(event));
            }
        }
        loop {
            match self.receiver.try_recv() {
                Ok(event) => {
                    let event = ReceivedEvent(event);
                    if self.accepts(&event) {
                        return Ok(Some(event));
                    }
                }
                Err(broadcast::error::TryRecvError::Empty) => return Ok(None),
                Err(broadcast::error::TryRecvError::Closed) => {
                    return Err(
                        "Claviron application event bus closed".to_owned()
                    );
                }
                Err(broadcast::error::TryRecvError::Lagged(skipped)) => {
                    return Err(format!(
                        "PHP application event subscription lagged by {skipped} event(s)"
                    ));
                }
            }
        }
    }

    pub(crate) async fn receive(&mut self) -> Result<ReceivedEvent, String> {
        while let Some(event) = self.deferred.pop_front() {
            if self.accepts(&event) {
                return Ok(event);
            }
        }
        loop {
            match self.receiver.recv().await {
                Ok(event) => {
                    let event = ReceivedEvent(event);
                    if self.accepts(&event) {
                        return Ok(event);
                    }
                }
                Err(broadcast::error::RecvError::Closed) => {
                    return Err(
                        "Claviron application event bus closed".to_owned()
                    );
                }
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    return Err(format!(
                        "PHP application event subscription lagged by {skipped} event(s)"
                    ));
                }
            }
        }
    }
}

pub(crate) enum RuntimeInput<T> {
    Value(T),
    Event(ReceivedEvent),
}

pub(crate) struct PhpEventBridge {
    events: ProcessEventBus,
    runtime: Handle,
    interest: Option<Arc<LinkInterest>>,
    capacity: usize,
}

impl PhpEventBridge {
    pub(crate) fn subscription(
        &self,
        application: Option<ApplicationId>,
    ) -> EventSubscription {
        EventSubscription::new(
            &self.events,
            application,
            self.interest.clone(),
            self.capacity,
        )
    }

    pub(crate) fn publish_for_application(
        &self,
        application: ApplicationId,
        topic: String,
        payload: EventValue,
    ) -> Result<(), String> {
        let event =
            ApplicationEvent::for_application(application, topic, payload)
                .map_err(|error| error.to_string())?;
        self.events.emit(event);
        Ok(())
    }

    pub(crate) fn publish_shared(
        &self,
        topic: String,
        payload: EventValue,
    ) -> Result<(), String> {
        let event = ApplicationEvent::new(topic, payload)
            .map_err(|error| error.to_string())?;
        self.events.emit(event);
        Ok(())
    }

    fn block_on<F, T>(
        &self,
        future: F,
    ) -> Result<T, String>
    where
        F: Future<Output = T>,
    {
        if Handle::try_current().is_ok() {
            return Err("blocking PHP event access must run outside a Tokio runtime thread".into());
        }
        Ok(self.runtime.block_on(future))
    }

    pub(crate) fn receive(
        &self,
        subscription: &mut EventSubscription,
        timeout_ms: u64,
        cancellation: Option<&CancellationToken>,
    ) -> Result<Option<ReceivedEvent>, String> {
        if cancellation.is_some_and(CancellationToken::is_cancelled) {
            return Ok(None);
        }
        if timeout_ms == 0 {
            return subscription.try_receive();
        }
        let timeout = Duration::from_millis(timeout_ms);
        self.block_on(async {
            match cancellation {
                Some(cancellation) => {
                    tokio::select! {
                        biased;
                        _ = cancellation.cancelled() => Ok(None),
                        result = tokio::time::timeout(timeout, subscription.receive()) => {
                            match result {
                                Ok(result) => result.map(Some),
                                Err(_) => Ok(None)
                            }
                        }
                    }
                }
                None => match tokio::time::timeout(timeout, subscription.receive()).await {
                    Ok(result) => result.map(Some),
                    Err(_) => Ok(None)
                }
            }
        })?
    }

    pub(crate) fn receive_with<T>(
        &self,
        receiver: &mut mpsc::Receiver<T>,
        subscription: &mut EventSubscription,
        cancellation: &CancellationToken,
    ) -> Result<Option<RuntimeInput<T>>, String> {
        self.block_on(async {
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => Ok(None),
                value = receiver.recv() => Ok(value.map(RuntimeInput::Value)),
                event = subscription.receive() => event.map(|event| Some(RuntimeInput::Event(event)))
            }
        })?
    }
}

static PHP_EVENTS: OnceLock<ArcSwapOption<PhpEventBridge>> = OnceLock::new();

fn bridge_slot() -> &'static ArcSwapOption<PhpEventBridge> {
    PHP_EVENTS.get_or_init(ArcSwapOption::const_empty)
}

/// Runtime presence does not imply that an event bridge has been prepared.
pub(crate) fn is_configured() -> bool {
    bridge_slot().load().is_some()
}

pub(crate) fn bridge() -> Result<Arc<PhpEventBridge>, String> {
    bridge_slot()
        .load_full()
        .ok_or_else(|| "Claviron PHP events are not configured".to_owned())
}

pub(crate) fn configure_from_state(
    state: &AppState,
    config: crate::PhpEventConfig,
) -> Result<(), String> {
    let runtime = Handle::try_current().map_err(|error| {
        format!("PHP event runtime handle is unavailable: {error}")
    })?;
    bridge_slot().store(Some(Arc::new(PhpEventBridge {
        events: state.events().bus().clone(),
        runtime,
        interest: state.registry_ref().resolve::<LinkInterest>(),
        capacity: config.capacity,
    })));
    Ok(())
}

pub(crate) fn register(
    state: &AppState
) -> claviron_runtime::registry::Result<()> {
    // Idempotent, and the point of calling it here: `ApplicationEventRegistry` only
    // exists when this process is part of a fleet, and it is registered by
    // `claviron_orbit::register`. Resolving without this made "absent" mean
    // either "standalone" or "orbit has not registered yet" — the second is an
    // ordering accident, and the symptom is a bridge that is silently never
    // installed. After this call, absent means standalone and nothing else.
    claviron_orbit::register(state)?;
    if let Some(events) =
        state.registry_ref().resolve::<ApplicationEventRegistry>()
    {
        events.register_bridge::<ApplicationEvent>(state)?;
    }
    Ok(())
}

#[cfg(test)]
#[path = "../tests/event.rs"]
mod tests;
