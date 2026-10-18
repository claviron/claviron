//! Response-class streaks and bounded serving bans; never reachability verdicts.
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::time::Instant;

use crate::config::profile::balance::Scope;
use crate::config::profile::outlier::OutlierConfig;
use crate::health::lock::OUTLIER_LOCK_INDEX;
use crate::origin::Serving;
use crate::proxy::service::UpstreamService;
use crate::proxy::service::lifecycle::ServiceEpoch;

pub(crate) struct State {
    rows: Mutex<Box<[Row]>>,
}
#[derive(Clone)]
struct Row {
    window: Instant,
    five_xx: u32,
    gateway: u32,
    ejections: u32,
    until: Option<Instant>,
    publishing: bool,
}
impl Row {
    fn reset_streak(
        &mut self,
        now: Instant,
    ) {
        self.window = now;
        self.five_xx = 0;
        self.gateway = 0;
    }
    fn ejected(
        &self,
        now: Instant,
    ) -> bool {
        self.until.is_some_and(|deadline| deadline > now)
    }
}
impl State {
    pub(crate) fn remap(
        &self,
        mapping: &[Option<usize>],
    ) -> Self {
        let rows = self.rows.lock().unwrap_or_else(|p| p.into_inner());
        Self {
            rows: Mutex::new(
                mapping
                    .iter()
                    .map(|index| {
                        let mut row = index
                            .map(|index| rows[index].clone())
                            .unwrap_or(Row {
                                window: Instant::now(),
                                five_xx: 0,
                                gateway: 0,
                                ejections: 0,
                                until: None,
                                publishing: false,
                            });
                        // In-progress fleet publication belongs to the old view. Its durable
                        // lease remains authoritative; it cannot latch a new local publisher.
                        row.publishing = false;
                        row
                    })
                    .collect(),
            ),
        }
    }

    pub(crate) fn new(
        config: &OutlierConfig,
        origins: usize,
    ) -> Option<Self> {
        config.enabled().then(|| Self {
            rows: Mutex::new(
                (0..origins)
                    .map(|_| Row {
                        window: Instant::now(),
                        five_xx: 0,
                        gateway: 0,
                        ejections: 0,
                        until: None,
                        publishing: false,
                    })
                    .collect(),
            ),
        })
    }
    fn local_ejected(
        &self,
        index: usize,
    ) -> bool {
        let rows = self.rows.lock().unwrap_or_else(|p| p.into_inner());
        rows[index].ejected(Instant::now())
    }
}
fn cap(
    config: &OutlierConfig,
    origins: usize,
) -> usize {
    (origins.saturating_mul(usize::from(config.max_ejection_percent)) / 100)
        .min(origins.saturating_sub(1))
}
impl UpstreamService {
    /// Current eligibility composes L7 with the independent outlier cause.
    /// Fleet bans survive disabled policy/restart until their authoritative TTL.
    pub(crate) fn origin_serving(
        &self,
        index: usize,
    ) -> Serving {
        self.origin_serving_if_available(index).unwrap_or(Serving::No)
    }
    pub(crate) fn origin_serving_if_available(
        &self,
        index: usize,
    ) -> Option<Serving> {
        let origin = &self.inner.profile.origins[index];
        if origin.slot.get().outlier_ejected() {
            self.expire_fleet_ejection(index);
        }
        let serving = origin.slot.get().read(|slot| slot.serving())?;
        if serving == Serving::No {
            return Some(serving);
        }
        if self.inner.profile.outlier.enabled()
            && (self.inner.profile.outlier.scope == Scope::Worker
                || self.inner.runtime.lock.is_none())
            && self
                .epoch()
                .outlier
                .as_ref()
                .is_some_and(|state| state.local_ejected(index))
        {
            Some(Serving::No)
        } else {
            Some(serving)
        }
    }
    pub(crate) fn record_outlier_status(
        &self,
        index: usize,
        status: http::StatusCode,
        epoch: &Arc<ServiceEpoch>,
    ) {
        let Some(state) = &epoch.outlier else {
            return;
        };
        // A response accepted by a retired epoch cannot publish a new ban.
        if self.is_shutting_down()
            || epoch.background.is_cancelled()
            || !Arc::ptr_eq(epoch, &self.epoch())
        {
            return;
        }
        let config = &self.inner.profile.outlier;
        let fleet =
            config.scope == Scope::Fleet && self.inner.runtime.lock.is_some();
        let now = Instant::now();
        let duration = {
            let mut rows = state.rows.lock().unwrap_or_else(|p| p.into_inner());
            if epoch.background.is_cancelled() {
                return;
            }
            let row = &mut rows[index];
            if now.duration_since(row.window) >= config.interval {
                row.reset_streak(now);
            }
            let code = status.as_u16();
            row.five_xx = if (500..600).contains(&code) {
                row.five_xx.saturating_add(1)
            } else {
                0
            };
            row.gateway = if matches!(code, 502..=504) {
                row.gateway.saturating_add(1)
            } else {
                0
            };
            let threshold = (config.consecutive_5xx > 0
                && row.five_xx >= config.consecutive_5xx)
                || (config.consecutive_gateway_errors > 0
                    && row.gateway >= config.consecutive_gateway_errors);
            if !threshold || row.publishing || row.ejected(now) {
                return;
            }
            let Some(duration) = config
                .base_ejection_time
                .checked_mul(row.ejections.saturating_add(1))
            else {
                return;
            };
            if now.checked_add(duration).is_none() {
                return;
            }
            if !fleet {
                let banned = rows.iter().filter(|row| row.ejected(now)).count();
                if banned >= cap(config, rows.len()) {
                    return;
                }
                let row = &mut rows[index];
                row.until = now.checked_add(duration);
                row.ejections = row.ejections.saturating_add(1);
                row.reset_streak(now);
            } else {
                rows[index].publishing = true;
            }
            duration
        };
        let admitted = !fleet || self.publish_fleet_ejection(index, duration);
        if fleet {
            let mut rows = state.rows.lock().unwrap_or_else(|p| p.into_inner());
            let row = &mut rows[index];
            row.publishing = false;
            if admitted {
                row.ejections = row.ejections.saturating_add(1);
                row.reset_streak(now);
                // Fleet selection reads the durable lease; this local deadline
                // only avoids duplicate publications by the same response owner.
                row.until = now.checked_add(duration);
            }
        }
        if admitted {
            self.record_outlier_ejection(index);
            self.refresh_origin_health(index);
            tracing::debug!(profile = %self.inner.profile.name, origin = %self.inner.profile.origins[index].label,
                ?duration, scope = ?config.scope, "upstream origin ejected");
        }
    }
    fn publish_fleet_ejection(
        &self,
        index: usize,
        duration: Duration,
    ) -> bool {
        let lock = self
            .inner
            .runtime
            .lock
            .as_ref()
            .expect("fleet ejection capability");
        let result = self.inner.runtime.state_table.with_structural(|| {
            // Expiry and publication share the table's structural process lock.
            // A peer cannot count a stale ban or exceed the profile cap in a race.
            self.expire_fleet_ejections_locked();
            let origins = &self.inner.profile.origins;
            if origins[index].slot.get().outlier_ejected()
                || origins.iter().filter(|o| o.slot.get().outlier_ejected()).count() >= cap(&self.inner.profile.outlier, origins.len())
            { return false; }
            match lock.begin_ejection(&origins[index].lock[OUTLIER_LOCK_INDEX], duration) {
                Ok(true) => {
                    let slot = origins[index].slot.get();
                    let before = slot.serving();
                    // Publish durable expiry authority before the serving deny.
                    if !slot.set_outlier_ejected(true) { return false; }
                    self.publish_origin_serving_event(index, before, slot.serving());
                    true
                }
                Ok(false) => false,
                Err(error) => {
                    tracing::warn!(profile = %self.inner.profile.name, %error, "upstream ejection lease failed");
                    false
                }
            }
        });
        match result {
            Ok(admitted) => admitted,
            Err(error) => {
                tracing::warn!(%error, "upstream ejection publication failed");
                false
            }
        }
    }
    fn expire_fleet_ejections_locked(&self) {
        let Some(lock) = &self.inner.runtime.lock else {
            return;
        };
        for (index, origin) in self.inner.profile.origins.iter().enumerate() {
            let slot = origin.slot.get();
            if !slot.outlier_ejected() {
                continue;
            }
            match lock.ejection_current(&origin.lock[OUTLIER_LOCK_INDEX]) {
                Ok(false) => {
                    let before = slot.serving();
                    if !slot.set_outlier_ejected(false) {
                        continue;
                    }
                    self.publish_origin_serving_event(
                        index,
                        before,
                        slot.serving(),
                    );
                    tracing::debug!(profile = %self.inner.profile.name, origin = %origin.label, "upstream ejection expired");
                }
                Ok(true) => {}
                // An unreadable authority is not permission to clear a ban.
                Err(error) => {
                    tracing::warn!(%error, "upstream ejection expiry read failed")
                }
            }
        }
    }
    fn expire_fleet_ejection(
        &self,
        _index: usize,
    ) {
        if let Err(error) = self
            .inner
            .runtime
            .state_table
            .with_structural(|| self.expire_fleet_ejections_locked())
        {
            tracing::warn!(%error, "upstream ejection expiry failed");
        }
    }
    pub(crate) async fn run_outlier_origin_loop(
        self,
        index: usize,
    ) {
        let mut ticker =
            tokio::time::interval(self.inner.profile.outlier.interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        ticker.tick().await;
        loop {
            tokio::select! { biased; _ = self.wait_shutdown() => return, _ = ticker.tick() => {} }
            self.refresh_origin_health(index);
        }
    }
}
