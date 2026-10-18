//! Atomic-like declarations bound to Claviron's fleet cell tables at boot.

use std::io;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex, Weak};

use arc_swap::ArcSwapOption;
use orbitive::cell::{self, CellId, CellType, Cells, Claim};

type CellsSlot = ArcSwapOption<Cells>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Orbit cell infrastructure is not prepared")]
    NotPrepared,
    #[error("Orbit cell allocation failed: {0}")]
    Allocation(#[source] Arc<cell::Error>),
    #[error(transparent)]
    Value(#[from] cell::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

trait Declaration: Send + Sync {
    fn bind(
        &self,
        cells: &Cells,
    ) -> Result<()>;
}

struct ManagedCells {
    // Neither pending declarations nor this binding keep an application alive.
    owner: Weak<CellsSlot>,
    pending: Vec<Weak<dyn Declaration>>,
}

static MANAGED_CELLS: Mutex<ManagedCells> =
    Mutex::new(ManagedCells { owner: Weak::new(), pending: Vec::new() });

/// Open the tables and bind declarations before any application boot/run.
/// The managed SHM provider owns validation, reuse and physical setup errors.
pub(super) fn install(
    slot: &Arc<CellsSlot>,
    prepare: impl FnOnce() -> cell::Result<Cells>,
) -> Result<()> {
    let mut managed = MANAGED_CELLS.lock().map_err(|_| {
        cell::Error::Io(io::Error::other(
            "managed Orbit cells binding is poisoned",
        ))
    })?;
    if let Some(current) = managed.owner.upgrade()
        && !Arc::ptr_eq(&current, slot)
    {
        return Err(cell::Error::Io(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "managed Orbit cells already belong to another live runtime",
        ))
        .into());
    }
    let cells = prepare()?;
    // Keep the pending list on failure so a rejected boot can be retried.
    for declaration in managed.pending.iter().filter_map(Weak::upgrade) {
        declaration.bind(&cells)?;
    }
    slot.store(Some(Arc::new(cells)));
    managed.owner = Arc::downgrade(slot);
    managed.pending.clear();
    Ok(())
}

struct Value<T: CellType> {
    initial: T,
    handle: ArcSwapOption<cell::Orbital<T>>,
    allocation_error: ArcSwapOption<cell::Error>,
}

impl<T: CellType> Value<T> {
    fn new(initial: T) -> Self {
        Self {
            initial,
            handle: ArcSwapOption::const_empty(),
            allocation_error: ArcSwapOption::const_empty(),
        }
    }

    fn with_handle<R>(
        &self,
        operation: impl FnOnce(&cell::Orbital<T>) -> cell::Result<R>,
    ) -> Result<R> {
        let handle = self.handle.load();
        if let Some(handle) = handle.as_ref() {
            return operation(handle).map_err(Error::Value);
        }
        match self.allocation_error.load_full() {
            Some(error) => Err(Error::Allocation(error)),
            None => Err(Error::NotPrepared),
        }
    }
}

impl<T: CellType> Declaration for Value<T> {
    fn bind(
        &self,
        cells: &Cells,
    ) -> Result<()> {
        // Binding is serialized by MANAGED_CELLS, never by a value operation.
        if self.handle.load().is_some() {
            return Ok(());
        }
        match cells.allocate(self.initial) {
            Ok(handle) => {
                self.handle.store(Some(Arc::new(handle)));
                self.allocation_error.store(None);
                Ok(())
            }
            Err(error) => {
                let error = Arc::new(error);
                self.allocation_error.store(Some(Arc::clone(&error)));
                Err(Error::Allocation(error))
            }
        }
    }
}

impl<T: CellType> Drop for Value<T> {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.load_full() {
            match handle.as_ref().clone().release() {
                Ok(()) | Err(cell::Error::Stale(_)) => {}
                Err(error) => {
                    tracing::error!(%error, "managed Orbit cell release failed")
                }
            }
        }
    }
}

/// An application value backed by the fleet's common cell tables.
///
/// Declare fields with `Orbital::new(initial)` or `Default`; Claviron binds
/// them during its own startup, before application providers boot. No cell
/// provider, boot callback or table setup is required in the application.
/// Before binding, `load` returns the declared initial value. Mutations require
/// prepared infrastructure; they never pretend an unshared write succeeded.
///
/// Use [`Self::try_new`] to report allocation errors immediately when creating
/// a cell while the runtime is already running. `new` also binds immediately
/// in a running runtime, but reports a failed allocation through value operations.
///
/// Clones own the same cell. The last managed clone releases its slot. Opening
/// its id through the low-level Cells API borrows that lifetime; it does not
/// extend ownership. Explicit [`Self::release`] invalidates every holder.
#[derive(Clone)]
pub struct Orbital<T: CellType>(Arc<Value<T>>);

impl<T: CellType> Orbital<T> {
    /// Declare a value; shared infrastructure is Claviron's responsibility.
    pub fn new(initial: T) -> Self {
        let value = Arc::new(Value::new(initial));
        match MANAGED_CELLS.lock() {
            Ok(mut managed) => {
                if let Some(cells) =
                    managed.owner.upgrade().and_then(|slot| slot.load_full())
                {
                    // The value retains the exact error for fallible operations.
                    let _ = value.bind(&cells);
                } else {
                    managed.pending.retain(|entry| entry.strong_count() > 0);
                    let declaration: Arc<dyn Declaration> = value.clone();
                    managed.pending.push(Arc::downgrade(&declaration));
                }
            }
            Err(_) => {
                value.allocation_error.store(Some(Arc::new(cell::Error::Io(
                    io::Error::other("managed Orbit cells binding is poisoned"),
                ))))
            }
        }
        Self(value)
    }

    /// Allocate in an already prepared runtime, reporting failure immediately.
    pub fn try_new(initial: T) -> Result<Self> {
        let value = Self::new(initial);
        value.id()?;
        Ok(value)
    }

    /// The shared id is available once Claviron has bound the value.
    pub fn id(&self) -> Result<CellId> {
        self.0.with_handle(|handle| Ok(handle.id()))
    }

    pub fn load(&self) -> Result<T> {
        let handle = self.0.handle.load();
        match handle.as_ref() {
            Some(handle) => handle.load().map_err(Error::Value),
            None => match self.0.allocation_error.load_full() {
                Some(error) => Err(Error::Allocation(error)),
                None => Ok(self.0.initial),
            },
        }
    }

    pub fn store(
        &self,
        value: T,
    ) -> Result<()> {
        self.0.with_handle(|handle| handle.store(value))
    }

    pub fn swap(
        &self,
        value: T,
    ) -> Result<T> {
        self.0.with_handle(|handle| handle.swap(value))
    }

    pub fn compare_exchange(
        &self,
        current: T,
        new: T,
    ) -> Result<std::result::Result<T, T>> {
        self.0.with_handle(|handle| handle.compare_exchange(current, new))
    }

    pub fn version(&self) -> Result<u32> {
        self.0.with_handle(cell::Orbital::version)
    }

    /// Blocking shared-word wait; cancellation is supplied explicitly below.
    pub fn wait_changed(
        &self,
        since: u32,
    ) -> Result<u32> {
        self.0.with_handle(|handle| handle.wait_changed(since))
    }

    pub fn wait_changed_until(
        &self,
        since: u32,
        interrupt: &AtomicBool,
    ) -> Result<Option<u32>> {
        self.0.with_handle(|handle| handle.wait_changed_until(since, interrupt))
    }

    pub fn wake_waiters(&self) -> Result<()> {
        self.0.with_handle(cell::Orbital::wake_waiters)
    }

    pub fn release(self) -> Result<()> {
        self.0.with_handle(|handle| handle.clone().release())
    }
}

impl<T: CellType + Default> Default for Orbital<T> {
    fn default() -> Self {
        Self::new(T::default())
    }
}

macro_rules! arithmetic {
    ($type:ty, $($method:ident),+) => {
        impl Orbital<$type> {
            $(pub fn $method(&self, by: $type) -> Result<$type> {
                self.0.with_handle(|handle| handle.$method(by))
            })+
        }
    };
}

arithmetic!(u64, fetch_add, fetch_sub);
arithmetic!(i64, fetch_add, fetch_sub);
arithmetic!(f64, fetch_add);

impl Orbital<i64> {
    pub fn add_until(
        &self,
        by: i64,
        limit: i64,
    ) -> Result<Claim> {
        self.0.with_handle(|handle| handle.add_until(by, limit))
    }
}

impl Orbital<bool> {
    pub fn set(&self) -> Result<bool> {
        self.swap(true)
    }

    pub fn clear(&self) -> Result<bool> {
        self.swap(false)
    }
}

#[cfg(test)]
#[path = "../../tests/cell/orbital.rs"]
mod tests;
