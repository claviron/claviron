use claviron::{AppState, registry};

#[cfg(feature = "bench")]
mod bench;

pub fn bootstrap(state: &AppState) -> registry::Result<()> {
    #[cfg(feature = "bench")]
    bench::register(state)?;
    #[cfg(not(feature = "bench"))]
    let _ = state;
    Ok(())
}
