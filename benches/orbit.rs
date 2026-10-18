//! Outline for Claviron Orbit codec benchmarks; no cases are registered yet.
//!
//! Intended groups:
//! - Encoding existing Claviron event/application envelopes.
//! - Decoding the same formats from prepared byte sequences.
//! - Valid and rejected inputs with declared payload sizes.
//!
//! Keep encode-only, decode-only, and round-trip cases distinct.
//! State buffer reuse and allocation policy; consume and validate outputs.
//! Do not attach SHM, boot a fleet, or include peer execution in codec timing.
//! Primitive stream/transport comparisons belong to Orbitive, not this suite.
//! See README.md for the shared measurement and reporting boundaries.
