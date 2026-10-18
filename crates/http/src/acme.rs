use std::path::PathBuf;
use std::sync::Arc;

use arc_swap::ArcSwapOption;

/// Runtime source of truth for the directory shared by the ACME token writer
/// and the cleartext HTTP-01 reader.
///
/// The ACME provider publishes this value from `acme.yaml`. HTTP listeners
/// only opt into HTTP-01; they do not carry a second path configuration.
#[derive(Default)]
pub struct Http01ChallengeDirectory {
    current: ArcSwapOption<PathBuf>,
}

impl Http01ChallengeDirectory {
    pub fn publish(
        &self,
        path: Option<PathBuf>,
    ) {
        self.current.store(path.map(Arc::new));
    }

    pub fn snapshot(&self) -> Option<Arc<PathBuf>> {
        self.current.load_full()
    }
}
