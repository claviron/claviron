use std::path::PathBuf;
use std::sync::{Arc, Mutex, Weak};

use arc_swap::ArcSwapOption;

/// Runtime source of truth for the directory shared by the ACME token writer
/// and the cleartext HTTP-01 reader.
///
/// The ACME provider publishes this value from `acme.yaml`. HTTP listeners
/// only opt into HTTP-01; they do not carry a second path configuration.
#[derive(Default)]
pub struct Http01ChallengeDirectory {
    current: ArcSwapOption<PathBuf>,
    observers: Mutex<Vec<Weak<dyn Http01DirectoryObserver>>>,
}

/// Rebuild a prepared reader when its directory publication changes.
/// Called on the publishing control path, never while serving a request.
pub trait Http01DirectoryObserver: Send + Sync {
    fn directory_changed(&self);
}

impl Http01ChallengeDirectory {
    pub fn publish(
        &self,
        path: Option<PathBuf>,
    ) {
        let observers = {
            let mut observers =
                self.observers.lock().expect("HTTP-01 observers poisoned");
            if self.current.load_full().as_deref() == path.as_ref() {
                return;
            }
            self.current.store(path.map(Arc::new));
            let mut live = Vec::new();
            observers.retain(|observer| {
                if let Some(observer) = observer.upgrade() {
                    live.push(observer);
                    true
                } else {
                    false
                }
            });
            live
        };
        for observer in observers {
            observer.directory_changed();
        }
    }

    pub fn snapshot(&self) -> Option<Arc<PathBuf>> {
        self.current.load_full()
    }

    pub fn observe(
        &self,
        observer: Weak<dyn Http01DirectoryObserver>,
    ) {
        let mut observers =
            self.observers.lock().expect("HTTP-01 observers poisoned");
        observers.retain(|observer| observer.strong_count() != 0);
        if !observers.iter().any(|registered| registered.ptr_eq(&observer)) {
            observers.push(observer);
        }
    }

    pub fn unobserve(
        &self,
        observer: &Weak<dyn Http01DirectoryObserver>,
    ) {
        self.observers.lock().expect("HTTP-01 observers poisoned").retain(
            |registered| {
                registered.strong_count() != 0 && !registered.ptr_eq(observer)
            },
        );
    }
}

#[cfg(test)]
#[path = "../tests/unit/acme.rs"]
mod tests;
