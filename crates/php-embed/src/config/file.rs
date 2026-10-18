//! Configured registration's synchronous I/O boundary.
//!
//! Registration happens before provider boot. `std::fs` is appropriate here;
//! future async reload must load a candidate off the executor and validate it
//! before publication. Reading a file must never mutate a running PHP engine.

use std::io::ErrorKind;

use claviron_runtime::AppState;
use claviron_runtime::registry::{Error, Result};

use crate::PhpEmbedRuntime;

mod schema;

pub(crate) fn load_script(
    state: &AppState,
    name: &str,
    raw: &str,
    description: claviron_app::ApplicationDescription,
) -> Result<PhpEmbedRuntime> {
    let config = schema::parse(raw, |key| state.env_snapshot().get(key))?;
    let plan = config.resolve()?;
    if plan.has_applications() {
        return Err(Error::msg(
            "script runner configuration cannot declare persistent applications",
        ));
    }
    let runtime = plan.build_script(name, description);
    runtime.validate()?;
    Ok(runtime)
}

pub(crate) fn load_application(
    state: &AppState,
    name: &str,
    raw: &str,
    description: claviron_app::ApplicationDescription,
) -> Result<PhpEmbedRuntime> {
    let config = schema::parse_application(raw, name, |key| {
        state.env_snapshot().get(key)
    })?;
    let runtime = config.resolve()?.describe(description).build();
    runtime.validate()?;
    Ok(runtime)
}

/// New processes load a new plan; service restarts reuse the resolved plan.
pub(crate) fn load_service(state: &AppState) -> Result<PhpEmbedRuntime> {
    let path = state.config_dir().join("php.yaml");
    let config = match std::fs::read_to_string(&path) {
        Ok(raw) => schema::parse(&raw, |key| state.env_snapshot().get(key))?,
        Err(error) if error.kind() == ErrorKind::NotFound => {
            schema::PhpConfig::default()
        }
        Err(error) => {
            return Err(Error::msg(format!("{}: {error}", path.display())));
        }
    };
    let plan = config.resolve()?;
    let runtime = plan.build();
    runtime.validate()?;
    Ok(runtime)
}
