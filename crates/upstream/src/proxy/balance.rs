//! Prepared selection policy; requests borrow keys and allocate no candidate Vec.
use std::sync::Arc;

use crate::origin::OriginId;
use crate::proxy::service::UpstreamService;

pub(crate) mod consistent_hash;
mod key;
pub(crate) mod least_connections;
mod peak_ewma;
pub(crate) mod round_robin;
mod weight;
pub(crate) use key::RequestKeyView;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Prepared {
    RoundRobin { weighted: bool },
    PeakEwma(std::time::Duration),
    LeastConnections(crate::config::profile::balance::Scope),
    ConsistentHash(consistent_hash::Ring),
    First,
}
impl Prepared {
    pub(crate) fn compile(
        name: &str,
        config: &crate::config::profile::balance::BalanceConfig,
        origins: &[crate::origin::Origin],
    ) -> Result<Arc<Self>, crate::config::error::ConfigError> {
        use crate::config::profile::balance::{Algorithm, Scope};
        let invalid =
            |field, detail| crate::config::error::ConfigError::InvalidBalance {
                profile: name.into(),
                field,
                detail,
            };
        if config.scope == Scope::Fleet
            && config.algorithm != Algorithm::LeastConnections
        {
            return Err(invalid(
                "scope",
                "fleet scope is only supported by least_connections",
            ));
        }
        if !config.slow_start.window.is_zero()
            && !(1..=100).contains(&config.slow_start.initial_percent)
        {
            return Err(invalid(
                "slow_start.initial_percent",
                "must be within 1..=100 when slow start is enabled",
            ));
        }
        let prepared = match config.algorithm {
            Algorithm::RoundRobin => Self::RoundRobin {
                weighted: !config.slow_start.window.is_zero()
                    || origins.iter().any(|origin| origin.weight != 1),
            },
            Algorithm::LeastConnections => Self::LeastConnections(config.scope),
            Algorithm::First => Self::First,
            Algorithm::ConsistentHash => Self::ConsistentHash(
                consistent_hash::Ring::compile(name, config, origins)?,
            ),
            Algorithm::PeakEwma => {
                if config.decay.is_zero() {
                    return Err(invalid("decay", "must be greater than zero"));
                }
                Self::PeakEwma(config.decay)
            }
        };
        Ok(Arc::new(prepared))
    }
}

pub(crate) struct Health<'a> {
    pub(crate) service: &'a UpstreamService,
    pub(crate) pools: &'a crate::proxy::service::runtime::ProtocolPools,
}
impl Health<'_> {
    fn selectable(
        &self,
        index: usize,
    ) -> bool {
        self.service.origin_is_selectable(index)
    }
}
pub(crate) trait Balancer {
    fn order<'a>(
        &'a self,
        request: &RequestKeyView<'_>,
        health: Health<'a>,
    ) -> Candidates<'a>;
    fn observe(
        &self,
        id: OriginId,
        latency: std::time::Duration,
    );
}
pub(crate) struct Selection {
    prepared: Arc<Prepared>,
    cursor: round_robin::RoundRobinCursor,
    profile: Arc<crate::compile::profile::Profile>,
    smooth: Option<weight::Smooth>,
    ewma: Option<peak_ewma::Window>,
}
impl Selection {
    pub(crate) fn new(profile: Arc<crate::compile::profile::Profile>) -> Self {
        let smooth = matches!(
            profile.balancer.as_ref(),
            Prepared::RoundRobin { weighted: true }
        )
        .then(|| weight::Smooth::new(profile.origins.len()));
        let ewma = match profile.balancer.as_ref() {
            Prepared::PeakEwma(decay) => {
                Some(peak_ewma::Window::new(profile.origins.len(), *decay))
            }
            _ => None,
        };
        Self {
            prepared: profile.balancer.clone(),
            profile,
            cursor: Default::default(),
            smooth,
            ewma,
        }
    }
    pub(crate) fn reset(&self) {
        self.cursor.reset();
        if let Some(smooth) = &self.smooth {
            smooth.reset();
        }
        if let Some(ewma) = &self.ewma {
            ewma.reset();
        }
    }
}
enum Order {
    Circular { start: usize, offset: usize },
    Hash { key: u64, previous: Option<(u64, usize)> },
    Ranked { start: usize },
}
// One bit per possible OriginId prevents duplicate attempts when live costs
// change during a failed acquisition. Wire id width fixes this, not operator policy.
const SEEN_WORDS: usize = (u16::MAX as usize + 1) / u64::BITS as usize;
pub(crate) struct Candidates<'a> {
    health: Health<'a>,
    selection: &'a Selection,
    now: u64,
    order: Order,
    // The bitmap spans the frozen u16 OriginId domain, including detached profiles
    // larger than managed-table capacity. No request Vec/box or capacity setting.
    seen: Option<[u64; SEEN_WORDS]>,
}
impl Candidates<'_> {
    pub(crate) fn rewind(&mut self) {
        match &mut self.order {
            Order::Circular { offset, .. } => *offset = 0,
            Order::Hash { previous, .. } => *previous = None,
            Order::Ranked { .. } => {}
        }
        if let Some(seen) = &mut self.seen {
            seen.fill(0);
        }
    }
}
impl Balancer for Selection {
    fn order<'a>(
        &'a self,
        request: &RequestKeyView<'_>,
        health: Health<'a>,
    ) -> Candidates<'a> {
        let len = health.service.inner.profile.origins.len();
        let now = if self.profile.balance.slow_start.window.is_zero() {
            0
        } else {
            crate::state::layout::now_millis()
        };
        let order = match self.prepared.as_ref() {
            Prepared::RoundRobin { .. } => {
                let start = self.cursor.next_index(len).unwrap_or(0);
                let start = self
                    .smooth
                    .as_ref()
                    .and_then(|smooth| {
                        smooth.choose(
                            &health,
                            start,
                            &self.profile.balance.slow_start,
                            now,
                        )
                    })
                    .unwrap_or(start);
                Order::Circular { start, offset: 0 }
            }
            Prepared::LeastConnections(_) | Prepared::PeakEwma(_) => {
                Order::Ranked {
                    start: self.cursor.next_index(len).unwrap_or(0),
                }
            }
            // Declared order, starting from this worker's own origin: four
            // workers over eight origins keep four processes warm, each fed
            // by one worker, instead of all four piling onto the first.
            Prepared::First => Order::Ranked {
                start: health.service.inner.runtime.worker % len.max(1),
            },
            Prepared::ConsistentHash(ring) => {
                Order::Hash { key: ring.key.hash(request), previous: None }
            }
        };
        let seen = if matches!(order, Order::Ranked { .. }) {
            Some([0; SEEN_WORDS])
        } else {
            None
        };
        Candidates { health, selection: self, now, order, seen }
    }
    fn observe(
        &self,
        id: OriginId,
        latency: std::time::Duration,
    ) {
        if let Some(ewma) = &self.ewma {
            ewma.observe(id.index(), latency);
        }
    }
}
impl Iterator for Candidates<'_> {
    type Item = OriginId;
    fn next(&mut self) -> Option<OriginId> {
        let origins = &self.health.service.inner.profile.origins;
        match &mut self.order {
            Order::Circular { start, offset } => {
                while *offset < origins.len() {
                    let index = (*start + *offset) % origins.len();
                    *offset += 1;
                    if self.health.selectable(index) {
                        return Some(origins[index].id);
                    }
                }
                None
            }
            Order::Ranked { start } => {
                let seen = self.seen.as_mut().expect("ranked bitmap");
                let mut best: Option<(usize, Score)> = None;
                let now = tokio::time::Instant::now();
                for offset in 0..origins.len() {
                    let index = (*start + offset) % origins.len();
                    if seen[index / 64] & (1 << (index % 64)) != 0
                        || !self.health.selectable(index)
                    {
                        continue;
                    }
                    let score = match self.selection.prepared.as_ref() {
                        Prepared::LeastConnections(scope) => Score::Ratio {
                            load: least_connections::load(
                                &self.health,
                                origins[index].id,
                                *scope,
                            ) as u64,
                            weight: weight::effective(
                                &origins[index],
                                &self.selection.profile.balance.slow_start,
                                self.now,
                            ),
                        },
                        Prepared::PeakEwma(_) => Score::Latency(
                            self.selection.ewma.as_ref().expect("EWMA").cost(
                                index,
                                self.health.pools.in_flight(origins[index].id),
                                now,
                            ),
                        ),
                        Prepared::First => {
                            // Full is the pool's own capacity unless the origin says
                            // otherwise — HAProxy's `maxconn`, which is both. The
                            // rank is the distance from this worker's start, not
                            // the declared index.
                            let load =
                                self.health.pools.in_flight(origins[index].id)
                                    as u64;
                            let limit =
                                origins[index].max_live.unwrap_or_else(|| {
                                    self.selection.profile.capacity.max_live()
                                }) as u64;
                            Score::Fill {
                                full: load >= limit,
                                index: offset as u64,
                                load,
                                limit,
                            }
                        }
                        _ => unreachable!(),
                    };
                    if best
                        .as_ref()
                        .is_none_or(|(_, current)| score.less_than(current))
                    {
                        best = Some((index, score));
                    }
                }
                let (index, _) = best?;
                seen[index / 64] |= 1 << (index % 64);
                Some(origins[index].id)
            }
            Order::Hash { key, previous } => {
                let Prepared::ConsistentHash(ring) =
                    self.selection.prepared.as_ref()
                else {
                    unreachable!()
                };
                let best = origins
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| self.health.selectable(*i))
                    .map(|(i, _)| (ring.distance(i, *key), i))
                    .filter(|rank| previous.is_none_or(|last| *rank > last))
                    .min()?;
                *previous = Some(best);
                Some(origins[best.1].id)
            }
        }
    }
}
impl UpstreamService {}

// Exact integer ratios avoid losing least-connection ordering to float rounding.
enum Score {
    Ratio {
        load: u64,
        weight: u64,
    },
    Latency(f64),
    /// An origin below its limit beats any at it; below it, declared order
    /// decides; at it, the one least full.
    Fill {
        full: bool,
        index: u64,
        load: u64,
        limit: u64,
    },
}
impl Score {
    fn less_than(
        &self,
        other: &Self,
    ) -> bool {
        match (self, other) {
            (
                Self::Ratio { load: a, weight: aw },
                Self::Ratio { load: b, weight: bw },
            ) => {
                u128::from(*a) * u128::from(*bw)
                    < u128::from(*b) * u128::from(*aw)
            }
            (Self::Latency(a), Self::Latency(b)) => a < b,
            (
                Self::Fill {
                    full: a_full,
                    index: a_index,
                    load: a_load,
                    limit: a_limit,
                },
                Self::Fill {
                    full: b_full,
                    index: b_index,
                    load: b_load,
                    limit: b_limit,
                },
            ) => match (a_full, b_full) {
                (false, true) => true,
                (true, false) => false,
                (false, false) => a_index < b_index,
                (true, true) => {
                    u128::from(*a_load) * u128::from(*b_limit)
                        < u128::from(*b_load) * u128::from(*a_limit)
                }
            },
            _ => unreachable!("one ranked algorithm per iterator"),
        }
    }
}
