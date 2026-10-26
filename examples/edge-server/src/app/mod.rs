use claviron::{AppState, registry};

#[cfg(feature = "bench")]
mod bench;

pub fn bootstrap(state: &AppState) -> registry::Result<()> {
    #[cfg(feature = "bench")]
    {
        bench::register(state)?;
        claviron::bench::edge::BenchService::register_with_path(
            state,
            state.config_dir().join("native.yaml"),
        )?;
    }
    #[cfg(not(feature = "bench"))]
    let _ = state;
    Ok(())
}
