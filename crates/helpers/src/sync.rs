//! Lock guards that survive a previous holder's panic.

use std::sync::{Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};

/// Take the guard even when a previous holder panicked while holding it.
///
/// **Read the rule before reaching for this.** Poisoning is a real signal: it
/// says a thread died partway through whatever it was doing under the lock.
/// Ignoring it is correct only where the protected value cannot be left in a
/// state the next reader would misread — a plain map or set of handles, where
/// an entry is either in or out and no invariant spans two operations.
///
/// It is wrong wherever a panic could leave two fields disagreeing, a counter
/// half-applied, or a buffer partly written. There, the poison is the warning
/// it was designed to be and honouring it is the point.
///
/// What this replaces is `.lock().unwrap()`, which is the worst of both: it
/// neither trusts the data nor recovers, it just turns one thread's panic into
/// a permanent outage, because every later acquisition panics too.
pub trait Unpoisoned<T: ?Sized> {
    type ReadGuard<'a>
    where
        Self: 'a;
    type WriteGuard<'a>
    where
        Self: 'a;

    /// Shared access, poisoning ignored.
    fn read_unpoisoned(&self) -> Self::ReadGuard<'_>;

    /// Exclusive access, poisoning ignored.
    fn write_unpoisoned(&self) -> Self::WriteGuard<'_>;
}

impl<T: ?Sized> Unpoisoned<T> for RwLock<T> {
    type ReadGuard<'a>
        = RwLockReadGuard<'a, T>
    where
        Self: 'a;
    type WriteGuard<'a>
        = RwLockWriteGuard<'a, T>
    where
        Self: 'a;

    fn read_unpoisoned(&self) -> RwLockReadGuard<'_, T> {
        self.read().unwrap_or_else(|error| error.into_inner())
    }

    fn write_unpoisoned(&self) -> RwLockWriteGuard<'_, T> {
        self.write().unwrap_or_else(|error| error.into_inner())
    }
}

impl<T: ?Sized> Unpoisoned<T> for Mutex<T> {
    type ReadGuard<'a>
        = MutexGuard<'a, T>
    where
        Self: 'a;
    type WriteGuard<'a>
        = MutexGuard<'a, T>
    where
        Self: 'a;

    /// A mutex has one kind of access; both spellings give it, so a call site
    /// reads the same whether the lock behind it is a `Mutex` or an `RwLock`.
    fn read_unpoisoned(&self) -> MutexGuard<'_, T> {
        self.lock_unpoisoned()
    }

    fn write_unpoisoned(&self) -> MutexGuard<'_, T> {
        self.lock_unpoisoned()
    }
}

/// Named for what a `Mutex` actually does, for call sites that would rather
/// say `lock`.
pub trait LockUnpoisoned<T: ?Sized> {
    fn lock_unpoisoned(&self) -> MutexGuard<'_, T>;
}

impl<T: ?Sized> LockUnpoisoned<T> for Mutex<T> {
    fn lock_unpoisoned(&self) -> MutexGuard<'_, T> {
        self.lock().unwrap_or_else(|error| error.into_inner())
    }
}
