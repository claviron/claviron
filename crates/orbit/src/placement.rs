//! Claviron-facing exports of Continuo's transport-neutral placement contracts.

pub use continuo::fleet::{
    Dispatch, DispatchPolicy, DispatchRequest, Dispatched, Dispatcher, Scope,
    Scoped, ServiceExecution,
};

mod controller;
pub use controller::{OrbitDispatcher, register_dispatcher};

#[cfg(test)]
#[path = "../tests/placement.rs"]
mod tests;
