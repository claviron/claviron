//! Optional identity supplied by the embedding application, not inferred from
//! the runtime crate's package version or from the executable filename.

/// The embedding application's build, registered once per process for the
/// management surfaces that report it. An application registers it with
/// [`register!`] during bootstrap, which every re-executed worker runs too.
#[derive(Clone, Debug)]
pub struct ApplicationBuild {
    pub version: &'static str,
}

/// Register the calling crate's [`ApplicationBuild`] in this process.
///
/// A macro rather than a function: it expands in the crate that invokes it,
/// so `CARGO_PKG_VERSION` is that crate's version. A function here would
/// report the runtime's own version, which is [`VERSION`](crate::VERSION).
/// A second registration in the same process is refused.
///
/// ```ignore
/// // In the application's bootstrap, which every process runs:
/// claviron::application_build::register!(state)?;
/// ```
#[doc(hidden)]
#[macro_export]
macro_rules! __register_application_build_from_pkg_version {
    ($state:expr) => {
        $state
            .registry_ref()
            .insert(::std::sync::Arc::new($crate::ApplicationBuild {
                version: ::core::env!("CARGO_PKG_VERSION"),
            }))
            .map(|_| ())
    };
}

#[doc(inline)]
pub use crate::__register_application_build_from_pkg_version as register;

/// The application version registered in this process with [`register!`],
/// or `None` when the application registered none.
pub fn version(state: &crate::AppState) -> Option<&'static str> {
    state
        .registry_ref()
        .resolve::<ApplicationBuild>()
        .map(|build| build.version)
}
