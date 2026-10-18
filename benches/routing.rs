//! Outline for prepared-routing microbenchmarks; no cases are registered yet.
//!
//! Intended groups:
//! - Prepared vhost selection, including aliases and unknown-host misses.
//! - Exact and segment-aware route selection over declared table sizes.
//! - Native intercept versus delegation to a local fallback stand-in.
//!
//! Keep configuration parsing and router construction outside dispatch timing.
//! Match request shapes, response work, and policy between compared paths.
//! Do not boot services, execute guest runtimes, or contact an upstream.
//! A fallback stand-in measures routing cost, not application throughput.
//! See README.md for the shared measurement and reporting boundaries.
