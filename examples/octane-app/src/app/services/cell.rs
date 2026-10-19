use lumenize::Register;
use lumenize::claviron::orbit::{CellError, Orbital};
use lumenize::claviron::runtime::{AppState, registry};
use std::sync::Arc;

/// An application-owned Orbit cell, accessed by its Rust requests.
#[derive(Default)]
pub struct OrbitalCellService(Orbital<u64>);

impl OrbitalCellService {
    pub fn value(&self) -> Result<u64, CellError> {
        self.0.load()
    }

    pub fn hit(&self, value: u64) -> Result<u64, CellError> {
        self.0.fetch_add(value)
    }
}

impl Register<AppState> for OrbitalCellService {
    fn register(state: &AppState) -> registry::Result<()> {
        state
            .registry_ref()
            .insert(Arc::new(Self::default()))
            .map(|_| ())
    }
}
