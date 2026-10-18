//! Labels and Prometheus rows are materialized only for export.
use std::sync::atomic::Ordering;

use serde::{Deserialize, Serialize};

use super::store::ProfileMetrics;
use super::{UPSTREAM_METRICS, UpstreamMetrics};

// Health gauge encoding. Three-valued so an operator can render a single
// origin's state with a discrete gauge instead of two booleans.
pub const HEALTH_VALUE_HEALTHY: u64 =
    crate::origin::OriginHealth::Healthy as u64;
pub const HEALTH_VALUE_DOWN: u64 = crate::origin::OriginHealth::Down as u64;
pub const HEALTH_VALUE_HALF_OPEN: u64 =
    crate::origin::OriginHealth::HalfOpen as u64;

// Probe source label values.
pub const PROBE_SOURCE_DROP: &str =
    crate::health::probe::ProbeKind::Drop.as_str();
pub const PROBE_SOURCE_RECOVERY: &str =
    crate::health::probe::ProbeKind::Recovery.as_str();
pub const PROBE_SOURCE_ACTIVE: &str =
    crate::health::probe::ProbeKind::Active.as_str();

// Probe result label values.
pub const PROBE_RESULT_OK: &str =
    crate::health::probe::ProbeOutcome::Ok.as_str();
pub const PROBE_RESULT_FAIL: &str =
    crate::health::probe::ProbeOutcome::Fail.as_str();
pub const PROBE_RESULT_TIMEOUT: &str =
    crate::health::probe::ProbeOutcome::Timeout.as_str();

// State transition direction.
pub const TRANSITION_HEALTHY_TO_DOWN: &str = "healthy_to_down";
pub const TRANSITION_DOWN_TO_HEALTHY: &str = "down_to_healthy";

impl UpstreamMetrics {
    pub fn snapshot(&self) -> UpstreamMetricsSnapshot {
        let profiles = self.profiles.read().unwrap_or_else(|p| p.into_inner());
        let mut result = UpstreamMetricsSnapshot::default();
        for series in profiles.values() {
            for bank in &series.retired {
                result.merge(&rows(bank, false));
            }
            result.merge(&rows(&series.current, true));
        }
        result
    }
    pub fn write_prometheus(
        &self,
        output: &mut String,
    ) {
        self.snapshot().write_prometheus(output);
    }
}
fn rows(
    bank: &ProfileMetrics,
    current: bool,
) -> UpstreamMetricsSnapshot {
    let mut rows = UpstreamMetricsSnapshot::default();
    let profile = bank.name.as_ref();
    for (index, slot) in bank.origins.iter().enumerate() {
        let origin = bank.labels[index].as_ref();
        for (class, count) in
            super::slot::REQUEST_LABELS.iter().zip(&slot.requests)
        {
            rows.requests_total.push(LabeledCounter3 {
                a: profile.into(),
                b: origin.into(),
                c: (*class).into(),
                value: count.load(Ordering::Relaxed),
            });
        }
        for (source, outcomes) in
            super::slot::PROBE_LABELS.iter().zip(&slot.probes)
        {
            for (outcome, count) in
                super::slot::OUTCOME_LABELS.iter().zip(outcomes)
            {
                rows.probe_total.push(LabeledCounter4 {
                    a: profile.into(),
                    b: origin.into(),
                    c: (*source).into(),
                    d: (*outcome).into(),
                    value: count.load(Ordering::Relaxed),
                });
            }
        }
        for (direction, count) in
            [TRANSITION_HEALTHY_TO_DOWN, TRANSITION_DOWN_TO_HEALTHY]
                .iter()
                .zip(&slot.transitions)
        {
            rows.state_transitions_total.push(LabeledCounter3 {
                a: profile.into(),
                b: origin.into(),
                c: (*direction).into(),
                value: count.load(Ordering::Relaxed),
            });
        }
        for (reason, count) in
            super::slot::RETRY_LABELS.iter().zip(&slot.retries)
        {
            rows.retries_total.push(LabeledCounter3 {
                a: profile.into(),
                b: origin.into(),
                c: (*reason).into(),
                value: count.load(Ordering::Relaxed),
            });
        }
        rows.rejections_total.push(LabeledCounter3 {
            a: profile.into(),
            b: origin.into(),
            c: "breaker_open".into(),
            value: slot.breaker_rejections.load(Ordering::Relaxed),
        });
        rows.ejections_total.push(LabeledGauge2 {
            a: profile.into(),
            b: origin.into(),
            value: slot.ejections.load(Ordering::Relaxed),
        });
        if current {
            rows.origin_health.push(LabeledGauge2 {
                a: profile.into(),
                b: origin.into(),
                value: slot.health.load(Ordering::Relaxed).into(),
            });
            rows.origin_serving.push(LabeledGauge2 {
                a: profile.into(),
                b: origin.into(),
                value: slot.serving.load(Ordering::Relaxed).into(),
            });
        }
    }
    if current {
        rows.configured_live_capacity
            .push(LabeledGauge1 { a: profile.into(), value: bank.capacity });
    }
    rows
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct UpstreamMetricsSnapshot {
    pub requests_total: Vec<LabeledCounter3>,
    pub probe_total: Vec<LabeledCounter4>,
    pub state_transitions_total: Vec<LabeledCounter3>,
    pub origin_health: Vec<LabeledGauge2>,
    pub configured_live_capacity: Vec<LabeledGauge1>,
    #[serde(default)]
    pub retries_total: Vec<LabeledCounter3>,
    #[serde(default)]
    pub ejections_total: Vec<LabeledGauge2>,
    #[serde(default)]
    pub origin_serving: Vec<LabeledGauge2>,
    #[serde(default)]
    pub rejections_total: Vec<LabeledCounter3>,
}

pub type UpstreamScalarMetricsSnapshot = super::Scalars;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LabeledGauge1 {
    pub a: String,
    pub value: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LabeledCounter3 {
    pub a: String,
    pub b: String,
    pub c: String,
    pub value: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LabeledCounter4 {
    pub a: String,
    pub b: String,
    pub c: String,
    pub d: String,
    pub value: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LabeledGauge2 {
    pub a: String,
    pub b: String,
    pub value: u64,
}

impl UpstreamMetricsSnapshot {
    pub fn capture() -> Self {
        UPSTREAM_METRICS.snapshot()
    }

    /// Field-wise sum into self. Counters add per-label. If two snapshots ever
    /// publish conflicting health-state values for the same origin, the max
    /// wins — Down/HalfOpen carry stronger signal than Healthy.
    pub fn merge(
        &mut self,
        other: &Self,
    ) {
        merge_counters3(&mut self.requests_total, &other.requests_total);
        merge_counters4(&mut self.probe_total, &other.probe_total);
        merge_counters3(
            &mut self.state_transitions_total,
            &other.state_transitions_total,
        );
        merge_gauges_max(&mut self.origin_health, &other.origin_health);
        merge_gauges1_sum(
            &mut self.configured_live_capacity,
            &other.configured_live_capacity,
        );
        merge_counters3(&mut self.retries_total, &other.retries_total);
        merge_gauges2_sum(&mut self.ejections_total, &other.ejections_total);
        merge_serving(&mut self.origin_serving, &other.origin_serving);
        merge_counters3(&mut self.rejections_total, &other.rejections_total);
    }

    pub fn scalar_totals(&self) -> UpstreamScalarMetricsSnapshot {
        UpstreamScalarMetricsSnapshot {
            requests_total: self
                .requests_total
                .iter()
                .map(|row| row.value)
                .sum(),
            request_errors_total: self
                .requests_total
                .iter()
                .filter(|row| row.c == "error")
                .map(|row| row.value)
                .sum(),
            probes_total: self.probe_total.iter().map(|row| row.value).sum(),
            probe_failures_total: self
                .probe_total
                .iter()
                .filter(|row| {
                    row.d == PROBE_RESULT_FAIL || row.d == PROBE_RESULT_TIMEOUT
                })
                .map(|row| row.value)
                .sum(),
            state_transitions_total: self
                .state_transitions_total
                .iter()
                .map(|row| row.value)
                .sum(),
            configured_live_capacity: self
                .configured_live_capacity
                .iter()
                .map(|row| row.value)
                .sum(),
            retries_total: self.retries_total.iter().map(|row| row.value).sum(),
            ejections_total: self
                .ejections_total
                .iter()
                .map(|row| row.value)
                .sum(),
        }
    }

    pub fn write_prometheus(
        &self,
        output: &mut String,
    ) {
        use std::fmt::Write as FmtWrite;

        write_counter3(
            output,
            "claviron_upstream_requests_total",
            "Total upstream requests by profile, origin and status class",
            "profile",
            "origin",
            "status_class",
            &self.requests_total,
        );

        write_counter4(
            output,
            "claviron_upstream_probe_total",
            "Total upstream availability probes by profile, origin, source and result",
            "profile",
            "origin",
            "source",
            "result",
            &self.probe_total,
        );

        write_counter3(
            output,
            "claviron_upstream_state_transitions_total",
            "Total upstream origin state transitions by direction",
            "profile",
            "origin",
            "direction",
            &self.state_transitions_total,
        );

        write_gauge2(
            output,
            "claviron_upstream_origin_health",
            "Upstream origin health (0=Healthy, 1=Down, 2=HalfOpen)",
            "profile",
            "origin",
            &self.origin_health,
        );

        write_gauge1(
            output,
            "claviron_upstream_configured_live_capacity",
            "Configured upstream live capacity by profile",
            "profile",
            &self.configured_live_capacity,
        );

        write_counter3(
            output,
            "claviron_upstream_retries_total",
            "Total upstream request retries by reason",
            "profile",
            "origin",
            "reason",
            &self.retries_total,
        );
        write_metric2(
            output,
            "claviron_upstream_ejections_total",
            "Total upstream origin ejections",
            "counter",
            &self.ejections_total,
        );
        write_gauge2(
            output,
            "claviron_upstream_origin_serving",
            "Upstream origin serving (0=No, 1=Yes)",
            "profile",
            "origin",
            &self.origin_serving,
        );
        write_counter3(
            output,
            "claviron_upstream_rejections_total",
            "Upstream admission rejections",
            "profile",
            "origin",
            "cause",
            &self.rejections_total,
        );
        let _ = writeln!(output);
    }
}

impl UpstreamScalarMetricsSnapshot {
    pub fn capture() -> Self {
        UPSTREAM_METRICS.scalars()
    }

    pub fn merge(
        &mut self,
        other: &Self,
    ) {
        self.requests_total += other.requests_total;
        self.request_errors_total += other.request_errors_total;
        self.probes_total += other.probes_total;
        self.probe_failures_total += other.probe_failures_total;
        self.state_transitions_total += other.state_transitions_total;
        self.configured_live_capacity += other.configured_live_capacity;
        self.retries_total += other.retries_total;
        self.ejections_total += other.ejections_total;
    }

    pub fn write_prometheus(
        &self,
        output: &mut String,
    ) {
        use std::fmt::Write as FmtWrite;

        let _ = writeln!(
            output,
            "# HELP claviron_upstream_requests_aggregate_total Total upstream requests across all workers"
        );
        let _ = writeln!(
            output,
            "# TYPE claviron_upstream_requests_aggregate_total counter"
        );
        let _ = writeln!(
            output,
            "claviron_upstream_requests_aggregate_total {}",
            self.requests_total
        );

        let _ = writeln!(
            output,
            "# HELP claviron_upstream_request_errors_aggregate_total Total upstream transport/request errors across all workers"
        );
        let _ = writeln!(
            output,
            "# TYPE claviron_upstream_request_errors_aggregate_total counter"
        );
        let _ = writeln!(
            output,
            "claviron_upstream_request_errors_aggregate_total {}",
            self.request_errors_total
        );

        let _ = writeln!(
            output,
            "# HELP claviron_upstream_probes_aggregate_total Total upstream probes across all workers"
        );
        let _ = writeln!(
            output,
            "# TYPE claviron_upstream_probes_aggregate_total counter"
        );
        let _ = writeln!(
            output,
            "claviron_upstream_probes_aggregate_total {}",
            self.probes_total
        );

        let _ = writeln!(
            output,
            "# HELP claviron_upstream_probe_failures_aggregate_total Total failed or timed-out upstream probes across all workers"
        );
        let _ = writeln!(
            output,
            "# TYPE claviron_upstream_probe_failures_aggregate_total counter"
        );
        let _ = writeln!(
            output,
            "claviron_upstream_probe_failures_aggregate_total {}",
            self.probe_failures_total
        );

        let _ = writeln!(
            output,
            "# HELP claviron_upstream_state_transitions_aggregate_total Total upstream origin health state transitions across all workers"
        );
        let _ = writeln!(
            output,
            "# TYPE claviron_upstream_state_transitions_aggregate_total counter"
        );
        let _ = writeln!(
            output,
            "claviron_upstream_state_transitions_aggregate_total {}",
            self.state_transitions_total
        );

        let _ = writeln!(
            output,
            "# HELP claviron_upstream_configured_live_capacity_aggregate Configured upstream live capacity across all workers"
        );
        let _ = writeln!(
            output,
            "# TYPE claviron_upstream_configured_live_capacity_aggregate gauge"
        );
        let _ = writeln!(
            output,
            "claviron_upstream_configured_live_capacity_aggregate {}",
            self.configured_live_capacity
        );
        let _ = writeln!(
            output,
            "# HELP claviron_upstream_retries_aggregate_total Total upstream retries across workers\n# TYPE claviron_upstream_retries_aggregate_total counter\nclaviron_upstream_retries_aggregate_total {}",
            self.retries_total
        );
        let _ = writeln!(
            output,
            "# HELP claviron_upstream_ejections_aggregate_total Total upstream ejections across workers\n# TYPE claviron_upstream_ejections_aggregate_total counter\nclaviron_upstream_ejections_aggregate_total {}",
            self.ejections_total
        );
        let _ = writeln!(output);
    }
}

fn merge_counters3(
    dst: &mut Vec<LabeledCounter3>,
    src: &[LabeledCounter3],
) {
    for item in src {
        match dst
            .iter_mut()
            .find(|d| d.a == item.a && d.b == item.b && d.c == item.c)
        {
            Some(d) => d.value += item.value,
            None => dst.push(item.clone()),
        }
    }
}

fn merge_counters4(
    dst: &mut Vec<LabeledCounter4>,
    src: &[LabeledCounter4],
) {
    for item in src {
        match dst.iter_mut().find(|d| {
            d.a == item.a && d.b == item.b && d.c == item.c && d.d == item.d
        }) {
            Some(d) => d.value += item.value,
            None => dst.push(item.clone()),
        }
    }
}

fn merge_gauges_max(
    dst: &mut Vec<LabeledGauge2>,
    src: &[LabeledGauge2],
) {
    for item in src {
        match dst.iter_mut().find(|d| d.a == item.a && d.b == item.b) {
            Some(d) => d.value = d.value.max(item.value),
            None => dst.push(item.clone()),
        }
    }
}

fn write_counter3(
    output: &mut String,
    name: &str,
    help: &str,
    label_a: &str,
    label_b: &str,
    label_c: &str,
    rows: &[LabeledCounter3],
) {
    use std::fmt::Write as FmtWrite;
    let _ = writeln!(output, "# HELP {name} {help}");
    let _ = writeln!(output, "# TYPE {name} counter");
    for row in rows {
        let _ = writeln!(
            output,
            "{name}{{{label_a}=\"{}\",{label_b}=\"{}\",{label_c}=\"{}\"}} {}",
            escape_label(&row.a),
            escape_label(&row.b),
            escape_label(&row.c),
            row.value
        );
    }
}

#[allow(clippy::too_many_arguments)]
fn write_counter4(
    output: &mut String,
    name: &str,
    help: &str,
    label_a: &str,
    label_b: &str,
    label_c: &str,
    label_d: &str,
    rows: &[LabeledCounter4],
) {
    use std::fmt::Write as FmtWrite;
    let _ = writeln!(output, "# HELP {name} {help}");
    let _ = writeln!(output, "# TYPE {name} counter");
    for row in rows {
        let _ = writeln!(
            output,
            "{name}{{{label_a}=\"{}\",{label_b}=\"{}\",{label_c}=\"{}\",{label_d}=\"{}\"}} {}",
            escape_label(&row.a),
            escape_label(&row.b),
            escape_label(&row.c),
            escape_label(&row.d),
            row.value
        );
    }
}

fn merge_gauges1_sum(
    dst: &mut Vec<LabeledGauge1>,
    src: &[LabeledGauge1],
) {
    for item in src {
        match dst.iter_mut().find(|d| d.a == item.a) {
            Some(d) => d.value += item.value,
            None => dst.push(item.clone()),
        }
    }
}

fn write_gauge1(
    output: &mut String,
    name: &str,
    help: &str,
    label_a: &str,
    rows: &[LabeledGauge1],
) {
    use std::fmt::Write as FmtWrite;
    let _ = writeln!(output, "# HELP {name} {help}");
    let _ = writeln!(output, "# TYPE {name} gauge");
    for row in rows {
        let _ = writeln!(
            output,
            "{name}{{{label_a}=\"{}\"}} {}",
            escape_label(&row.a),
            row.value
        );
    }
}

fn write_gauge2(
    output: &mut String,
    name: &str,
    help: &str,
    label_a: &str,
    label_b: &str,
    rows: &[LabeledGauge2],
) {
    use std::fmt::Write as FmtWrite;
    let _ = writeln!(output, "# HELP {name} {help}");
    let _ = writeln!(output, "# TYPE {name} gauge");
    for row in rows {
        let _ = writeln!(
            output,
            "{name}{{{label_a}=\"{}\",{label_b}=\"{}\"}} {}",
            escape_label(&row.a),
            escape_label(&row.b),
            row.value
        );
    }
}

fn escape_label(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for c in value.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            _ => out.push(c),
        }
    }
    out
}

fn merge_gauges2_sum(
    dst: &mut Vec<LabeledGauge2>,
    src: &[LabeledGauge2],
) {
    for row in src {
        if let Some(old) =
            dst.iter_mut().find(|old| old.a == row.a && old.b == row.b)
        {
            old.value += row.value;
        } else {
            dst.push(row.clone());
        }
    }
}
fn merge_serving(
    dst: &mut Vec<LabeledGauge2>,
    src: &[LabeledGauge2],
) {
    for row in src {
        if let Some(old) =
            dst.iter_mut().find(|old| old.a == row.a && old.b == row.b)
        {
            old.value = old.value.min(row.value);
        } else {
            dst.push(row.clone());
        }
    }
}
fn write_metric2(
    output: &mut String,
    name: &str,
    help: &str,
    kind: &str,
    rows: &[LabeledGauge2],
) {
    use std::fmt::Write;
    let _ = writeln!(output, "# HELP {name} {help}\n# TYPE {name} {kind}");
    for row in rows {
        let _ = writeln!(
            output,
            "{name}{{profile=\"{}\",origin=\"{}\"}} {}",
            escape_label(&row.a),
            escape_label(&row.b),
            row.value
        );
    }
}
