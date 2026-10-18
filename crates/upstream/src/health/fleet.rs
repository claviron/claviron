//! Owns remote origin-event application and recovery handoff publication.

use super::event::{
    OriginHealthEvent, OriginServingEvent, RecoveryVacatedEvent,
    UpstreamOriginHealth,
};
use crate::origin::OriginHealth;
use crate::proxy::service::UpstreamService;

impl UpstreamService {
    pub(crate) fn publish_origin_health_event(
        &self,
        origin_index: usize,
        before: OriginHealth,
        after: OriginHealth,
    ) {
        if before == after {
            return;
        }
        if event_health(before) == event_health(after) {
            return;
        }
        let Some(events) = &self.inner.runtime.events else {
            return;
        };
        let Some(health) = event_health(after) else {
            return;
        };
        let Some(origin) = self.inner.profile.origins.get(origin_index) else {
            return;
        };

        events.emit(OriginHealthEvent {
            origin_id: origin.id,
            profile: self.inner.profile.name.clone(),
            origin_name: Some(origin.label.to_string()),
            origin_source: origin.source.to_string(),
            health,
        });
    }

    pub(crate) fn apply_origin_health_event(
        &self,
        event: OriginHealthEvent,
    ) {
        if self.is_shutting_down() || event.profile != self.inner.profile.name {
            return;
        }
        let Some(origin_index) = self.origin_index_for_event(&event) else {
            return;
        };

        // Events may arrive late or be missed. Only the current table can
        // determine health; payloads never overwrite a newer slot verdict.
        self.refresh_origin_health(origin_index);
        self.ensure_origin_recovery(origin_index);
    }

    pub(crate) fn apply_recovery_vacated_event(
        &self,
        event: RecoveryVacatedEvent,
    ) {
        if self.is_shutting_down() || event.profile != self.inner.profile.name {
            return;
        }
        let Some(lock) = &self.inner.runtime.lock else {
            return;
        };
        if event.target_owner != lock.owner() {
            return;
        }
        let Some(origin_index) = self
            .origin_index_for_parts(&event.origin_name, &event.origin_source)
        else {
            return;
        };

        self.refresh_origin_health(origin_index);
        self.ensure_origin_recovery(origin_index);
    }

    fn origin_index_for_event(
        &self,
        event: &OriginHealthEvent,
    ) -> Option<usize> {
        self.origin_index_for_id(
            event.origin_id,
            &event.origin_name,
            &event.origin_source,
        )
    }

    pub(crate) fn publish_origin_serving_event(
        &self,
        index: usize,
        before: crate::origin::Serving,
        after: crate::origin::Serving,
    ) {
        if before == after {
            return;
        }
        let Some(events) = &self.inner.runtime.events else {
            return;
        };
        let origin = &self.inner.profile.origins[index];
        events.emit(OriginServingEvent {
            origin_id: origin.id,
            profile: self.inner.profile.name.clone(),
            origin_name: Some(origin.label.to_string()),
            origin_source: origin.source.to_string(),
            serving: after,
        });
    }
    pub(crate) fn apply_origin_serving_event(
        &self,
        event: OriginServingEvent,
    ) {
        if self.is_shutting_down() || event.profile != self.inner.profile.name {
            return;
        }
        if let Some(index) = self.origin_index_for_id(
            event.origin_id,
            &event.origin_name,
            &event.origin_source,
        ) {
            self.refresh_origin_health(index);
        }
    }
    fn origin_index_for_id(
        &self,
        id: crate::origin::OriginId,
        name: &Option<String>,
        source: &str,
    ) -> Option<usize> {
        let matches = |origin: &crate::origin::Origin| {
            origin.source.as_ref() == source
                && (origin.label == origin.source
                    || name.as_ref().is_none_or(|remote| {
                        remote.as_str() == origin.label.as_ref()
                    }))
        };
        if self.inner.profile.origins.get(id.index()).is_some_and(matches) {
            Some(id.index())
        } else {
            self.inner.profile.origins.iter().position(matches)
        }
    }
    fn origin_index_for_parts(
        &self,
        origin_name: &Option<String>,
        origin_source: &str,
    ) -> Option<usize> {
        self.inner.profile.origins.iter().position(|origin| {
            origin.source.as_ref() == origin_source
                && (origin.label == origin.source
                    || origin_name.as_ref().is_none_or(|remote| {
                        remote.as_str() == origin.label.as_ref()
                    }))
        })
    }
}

fn event_health(health: OriginHealth) -> Option<UpstreamOriginHealth> {
    match health {
        OriginHealth::Healthy => Some(UpstreamOriginHealth::Healthy),
        OriginHealth::Down | OriginHealth::HalfOpen => {
            Some(UpstreamOriginHealth::Down)
        }
    }
}

mod bridge;
pub(crate) use bridge::UpstreamHealthBridge;
