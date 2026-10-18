//! Rust capabilities this application exposes to the interpreter.
//!
//! Handlers registered here are reached from Python with
//! `await gufi.invoke("...", payload)` — in this same process, without
//! leaving it.
//!
