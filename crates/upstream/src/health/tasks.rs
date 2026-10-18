//! Cold maintenance futures polled inside one Runnable, without child tasks.
use std::future::poll_fn;
use std::pin::Pin;
use std::task::Poll;

use crate::origin::{OriginId, PoolGeneration};
pub(crate) type ProbeFuture = Pin<Box<dyn Future<Output = ()> + Send>>;
type Key = (PoolGeneration, OriginId);
#[derive(Default)]
pub(super) struct ProbeTasks {
    tasks: Vec<(Key, ProbeFuture)>,
}
impl ProbeTasks {
    pub(super) fn contains(
        &self,
        key: Key,
    ) -> bool {
        self.tasks.iter().any(|(current, _)| *current == key)
    }
    pub(super) fn insert(
        &mut self,
        key: Key,
        future: ProbeFuture,
    ) {
        self.tasks.push((key, future));
    }
    pub(super) async fn completed(&mut self) {
        poll_fn(|cx| {
            let mut complete = false;
            self.tasks.retain_mut(|(_, future)| {
                if future.as_mut().poll(cx).is_ready() {
                    complete = true;
                    false
                } else {
                    true
                }
            });
            if complete { Poll::Ready(()) } else { Poll::Pending }
        })
        .await
    }
}

use std::sync::Arc;

use claviron_runtime::RunContext;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use crate::proxy::service::UpstreamService;
use crate::proxy::service::lifecycle::UpstreamLifecycle;
#[derive(Clone, Copy)]
pub(super) enum ProbeSchedule {
    Active,
    Recovery,
}
impl ProbeSchedule {
    fn enabled(
        self,
        service: &UpstreamService,
    ) -> bool {
        match self {
            Self::Active => {
                service.inner.profile.health.active.enabled
                    || service.inner.profile.health.l7.enabled
                    || service.inner.profile.outlier.enabled()
            }
            Self::Recovery => {
                service.inner.profile.health.recovery_probe.enabled
            }
        }
    }
    fn wants_origin(
        self,
        service: &UpstreamService,
        index: usize,
    ) -> bool {
        if !service.inner.profile.origins[index].slot.get().available() {
            return false;
        }
        match self {
            Self::Active => true,
            Self::Recovery => !service.origin_is_healthy(index),
        }
    }
    async fn probe(
        self,
        service: UpstreamService,
        index: usize,
    ) {
        match self {
            Self::Active => service.run_active_origin_loop(index).await,
            Self::Recovery => service.run_origin_validator(index).await,
        }
    }
}
pub(super) async fn run_rounds(
    services: impl Fn() -> Vec<UpstreamService> + Send,
    changed: Arc<Notify>,
    shutdown: CancellationToken,
    context: RunContext,
    schedule: ProbeSchedule,
) {
    let mut tasks = ProbeTasks::default();
    loop {
        // Register before taking the snapshot: notify_waiters is broadcast but
        // carries no permit for a listener that has not yet attached.
        let notified = changed.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        for service in services() {
            if service.lifecycle() != UpstreamLifecycle::Running
                || !schedule.enabled(&service)
            {
                continue;
            }
            let epoch = service.epoch();
            for index in 0..service.inner.profile.origins.len() {
                let key =
                    (epoch.generation, service.inner.profile.origins[index].id);
                if !tasks.contains(key)
                    && schedule.wants_origin(&service, index)
                {
                    let probe = service.clone();
                    if let Some(future) =
                        service.background_future_for(&epoch, async move {
                            schedule.probe(probe, index).await;
                        })
                    {
                        tasks.insert(key, future);
                    }
                }
            }
        }
        tokio::select! {
            biased;
            _ = context.cancelled() => return,
            _ = shutdown.cancelled() => return,
            _ = tasks.completed() => {},
            _ = &mut notified => {},
        }
    }
}
