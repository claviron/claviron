//! Dense per-profile banks; registration is cold, recording is indexed.
use std::collections::BTreeMap;
use std::sync::atomic::Ordering;
use std::sync::{Arc, LazyLock, RwLock, Weak};

use super::Scalars;
use super::slot::OriginSlot;
use crate::compile::profile::Profile;

pub static UPSTREAM_METRICS: LazyLock<UpstreamMetrics> =
    LazyLock::new(UpstreamMetrics::new);
#[derive(Debug, Default)]
pub struct UpstreamMetrics {
    pub(super) profiles: RwLock<BTreeMap<Arc<str>, ProfileSeries>>,
}
#[derive(Debug)]
pub(super) struct ProfileSeries {
    pub(super) current: Arc<ProfileMetrics>,
    pub(super) retired: Vec<Arc<ProfileMetrics>>,
}
#[derive(Debug)]
pub(crate) struct ProfileMetrics {
    pub(crate) origins: Box<[OriginSlot]>,
    pub(super) name: Arc<str>,
    pub(super) labels: Box<[Arc<str>]>,
    pub(super) capacity: u64,
    definition: Weak<Profile>,
}
impl UpstreamMetrics {
    pub fn new() -> Self {
        Self::default()
    }
    pub(crate) fn register(
        &self,
        profile: &Arc<Profile>,
    ) -> Arc<ProfileMetrics> {
        let mut profiles =
            self.profiles.write().unwrap_or_else(|p| p.into_inner());
        if let Some(series) = profiles.get(profile.name.as_str())
            && series.current.definition.ptr_eq(&Arc::downgrade(profile))
        {
            return series.current.clone();
        }
        let bank = Arc::new(ProfileMetrics {
            name: Arc::from(profile.name.as_str()),
            labels: profile.origins.iter().map(|o| o.label.clone()).collect(),
            origins: profile
                .origins
                .iter()
                .map(|_| OriginSlot::default())
                .collect(),
            capacity: (profile.origins.len() * profile.capacity.max_live())
                as u64,
            definition: Arc::downgrade(profile),
        });
        match profiles.get_mut(profile.name.as_str()) {
            Some(series) => {
                series
                    .retired
                    .push(std::mem::replace(&mut series.current, bank.clone()));
                series.compact();
            }
            None => {
                profiles.insert(
                    bank.name.clone(),
                    ProfileSeries {
                        current: bank.clone(),
                        retired: Vec::new(),
                    },
                );
            }
        }
        bank
    }
    pub fn scalars(&self) -> Scalars {
        let profiles = self.profiles.read().unwrap_or_else(|p| p.into_inner());
        let mut total = Scalars::default();
        for series in profiles.values() {
            for bank in
                series.retired.iter().chain(std::iter::once(&series.current))
            {
                for slot in &bank.origins {
                    total.requests_total += sum(&slot.requests);
                    total.request_errors_total +=
                        slot.requests[5].load(Ordering::Relaxed);
                    total.probes_total +=
                        slot.probes.iter().map(|row| sum(row)).sum::<u64>();
                    total.probe_failures_total += slot
                        .probes
                        .iter()
                        .map(|row| sum(&row[1..]))
                        .sum::<u64>();
                    total.state_transitions_total += sum(&slot.transitions);
                    total.retries_total += sum(&slot.retries);
                    total.ejections_total +=
                        slot.ejections.load(Ordering::Relaxed);
                }
            }
            total.configured_live_capacity += series.current.capacity;
        }
        total
    }
}
fn sum(values: &[std::sync::atomic::AtomicU64]) -> u64 {
    values.iter().map(|v| v.load(Ordering::Relaxed)).sum()
}

impl ProfileSeries {
    fn compact(&mut self) {
        // An unreferenced retired bank has no remaining producer. Fold matching
        // label sets so repeated reloads do not retain a bank per generation.
        for j in (0..self.retired.len()).rev() {
            if Arc::strong_count(&self.retired[j]) != 1 {
                continue;
            }
            if let Some(i) = (0..j).find(|&i| {
                Arc::strong_count(&self.retired[i]) == 1
                    && self.retired[i].labels == self.retired[j].labels
            }) {
                for (dst, src) in
                    self.retired[i].origins.iter().zip(&self.retired[j].origins)
                {
                    add(&dst.requests, &src.requests);
                    for (dst, src) in dst.probes.iter().zip(&src.probes) {
                        add(dst, src);
                    }
                    add(&dst.transitions, &src.transitions);
                    add(&dst.retries, &src.retries);
                    dst.ejections.fetch_add(
                        src.ejections.load(Ordering::Relaxed),
                        Ordering::Relaxed,
                    );
                }
                self.retired.swap_remove(j);
            }
        }
    }
}
fn add(
    dst: &[std::sync::atomic::AtomicU64],
    src: &[std::sync::atomic::AtomicU64],
) {
    for (dst, src) in dst.iter().zip(src) {
        dst.fetch_add(src.load(Ordering::Relaxed), Ordering::Relaxed);
    }
}
