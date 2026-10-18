//! Outline for HTTP operation microbenchmarks; no cases are registered yet.
//!
//! Intended groups:
//! - Authority and header parsing from fixed valid/invalid input sets.
//! - Cookie lookup/joining with declared header counts and input lengths.
//! - Path-prefix operations with exact, segment-boundary, and miss cases.
//!
//! Prepare inputs outside timing; name parsing and lookup cases separately.
//! Consume results and document whether allocation is part of each operation.
//! Do not include listeners, TLS, provider boot, or application execution.
//! See README.md for the shared measurement and reporting boundaries.
