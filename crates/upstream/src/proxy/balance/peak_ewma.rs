//! Worker-local response-header peaks; idle decay and held-permit penalty.
use std::sync::Mutex;
use std::time::Duration;

use tokio::time::Instant;
struct Sample {
    value: f64,
    at: Instant,
}
pub(super) struct Window {
    samples: Box<[Mutex<Sample>]>,
    decay: Duration,
}
impl Window {
    pub(super) fn new(
        len: usize,
        decay: Duration,
    ) -> Self {
        let now = Instant::now();
        Self {
            samples: (0..len)
                .map(|_| Mutex::new(Sample { value: 1.0, at: now }))
                .collect(),
            decay,
        }
    }
    pub(super) fn reset(&self) {
        let now = Instant::now();
        for sample in &self.samples {
            *sample.lock().unwrap_or_else(|p| p.into_inner()) =
                Sample { value: 1.0, at: now };
        }
    }
    pub(super) fn observe(
        &self,
        index: usize,
        latency: Duration,
    ) {
        let now = Instant::now();
        let measured = latency.as_secs_f64() * 1_000_000.0;
        let mut sample =
            self.samples[index].lock().unwrap_or_else(|p| p.into_inner());
        let elapsed = now.saturating_duration_since(sample.at).as_secs_f64();
        let factor = (-elapsed / self.decay.as_secs_f64()).exp();
        sample.value = if measured > sample.value {
            measured
        } else {
            sample.value * factor + measured * (1.0 - factor)
        };
        sample.at = now;
    }
    pub(super) fn cost(
        &self,
        index: usize,
        load: usize,
        now: Instant,
    ) -> f64 {
        let sample =
            self.samples[index].lock().unwrap_or_else(|p| p.into_inner());
        let elapsed = now.saturating_duration_since(sample.at).as_secs_f64();
        // One microsecond is the positive signal unit, not a transport deadline.
        // Zero-cost idle origins would defeat the in-flight penalty entirely.
        let estimate = (sample.value
            * (-elapsed / self.decay.as_secs_f64()).exp())
        .max(1.0);
        estimate * (load as f64 + 1.0)
    }
}
