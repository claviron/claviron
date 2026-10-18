//! The request path: everything one upstream request touches between the door
//! and the origin socket.
//!
//! A request arrives at [`service`], which checks policy, picks an origin with
//! `balance`, borrows or opens a connection through `connect`, and hands it
//! to `h1` or `h2`; `body` wraps what flows back, and `failure` names
//! whatever stopped it. Nothing outside this module runs per request — the
//! compiler, the fleet state and the health machinery all did their work
//! before the request existed, and the profile carries the result.

pub(crate) mod balance;
pub(crate) mod body;
pub(crate) mod connect;
pub(crate) mod failure;
pub(crate) mod h1;
pub(crate) mod h2;
pub(crate) mod lease;
pub mod service;
