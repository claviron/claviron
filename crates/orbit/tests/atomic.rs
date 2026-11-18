use std::sync::Arc;

use claviron_runtime::registry::Bootable;
use claviron_runtime::{AppState, Register};
use orbitive::Fleet;

use super::*;

orbital! {
    /// The value under test.
    Hits: i64 = 0, "hits"
    /// A second declaration of the same name, with another initial value.
    Requests: i64 = 7, "hits"
}

/// A registry whose Orbitals provider holds an in-memory table, as its boot
/// leaves it. Only the declarations under test boot.
fn state_with_table(name: &str) -> (AppState, Arc<OrbitalsProvider>) {
    let state = AppState::standalone(name, ".");
    register(&state).expect("Orbitals registration");
    let provider = state
        .registry_ref()
        .resolve::<OrbitalsProvider>()
        .expect("OrbitalsProvider is registered");
    let fleet = Arc::new(Fleet::join(name, 1).expect("in-memory fleet"));
    let orbitals = Orbitals::new(fleet).expect("Orbitals table");
    provider.resources().orbitals.store(Some(Arc::new(orbitals)));
    (state, provider)
}

/// Register `Declare<T>`, boot it, and resolve `T` as `Provide<T>` does.
async fn declare<T: Declared>(
    state: &AppState,
    provider: &Arc<OrbitalsProvider>,
) -> Arc<T> {
    Declare::<T>::register(state).expect("declaration registers");
    let declaration = state
        .registry_ref()
        .resolve::<Declare<T>>()
        .expect("declaration component is registered");
    declaration
        .boot((Arc::clone(provider),), state)
        .await
        .expect("declaration boots");
    state.registry_ref().resolve::<T>().expect("declared value is provided")
}

#[tokio::test(flavor = "current_thread")]
async fn a_declared_value_registers_and_is_provided() {
    let (state, provider) = state_with_table("orbital-provided");

    let hits = declare::<Hits>(&state, &provider).await;
    assert_eq!(hits.fetch_add(1), 0);

    let provided = state.registry_ref().resolve::<Hits>().unwrap();
    assert_eq!(provided.load(), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn two_declarations_of_one_name_share_a_value() {
    let (state, provider) = state_with_table("orbital-shared");

    let hits = declare::<Hits>(&state, &provider).await;
    let requests = declare::<Requests>(&state, &provider).await;

    // The first declaration installed its initial value; the second found it.
    assert_eq!(requests.load(), 0);
    hits.fetch_add(3);
    assert_eq!(requests.load(), 3);
    assert_eq!(hits.id(), requests.id());
}
