//! A module's service: a real runnable, whose work is a native call on a thread
//! of its own. It ends when `cancel` says so and nothing can force it.

use std::ffi::c_void;
use std::sync::{Arc, Condvar, Mutex};

use async_trait::async_trait;
use claviron_runtime::{AppState, RunContext, Runnable, registry};
use tokio::sync::oneshot;

use crate::abi::{self, Service};
use crate::registrar::contained;

#[derive(Default)]
struct CancelState {
    cancelled: bool,
    notify: Vec<(unsafe extern "C" fn(*mut c_void), SendPtr)>,
}

/// What a running service watches. A module holds it as `claviron_cancel*`.
#[derive(Default)]
pub(crate) struct Cancel {
    state: Mutex<CancelState>,
    changed: Condvar,
}

impl Cancel {
    fn cancel(&self) {
        let notify = {
            let mut state = self.state.lock().expect("cancel state");
            state.cancelled = true;
            std::mem::take(&mut state.notify)
        };
        self.changed.notify_all();
        for (callback, context) in notify {
            // Safety: the module registered this pair for this run.
            unsafe { callback(context.0) };
        }
    }
}

/// A module's pointer. What it may be used from is the module's contract.
#[derive(Clone, Copy)]
pub(crate) struct SendPtr(pub *mut c_void);
// Safety: a context is only ever handed back to the module that gave it, which
// declares it usable from the threads its table is called on.
unsafe impl Send for SendPtr {}
unsafe impl Sync for SendPtr {}

pub(crate) unsafe extern "C" fn cancelled(cancel: *const abi::Cancel) -> u32 {
    contained(|| {
        if cancel.is_null() {
            return abi::INVALID_ARGUMENT;
        }
        // Safety: a module holds this for the run it was handed to.
        let cancel = unsafe { &*cancel.cast::<Cancel>() };
        u32::from(cancel.state.lock().expect("cancel state").cancelled)
    })
}

pub(crate) unsafe extern "C" fn cancel_wait(cancel: *const abi::Cancel) -> u32 {
    contained(|| {
        if cancel.is_null() {
            return abi::INVALID_ARGUMENT;
        }
        // Safety: as in `cancelled`.
        let cancel = unsafe { &*cancel.cast::<Cancel>() };
        let mut state = cancel.state.lock().expect("cancel state");
        while !state.cancelled {
            state = cancel.changed.wait(state).expect("cancel state");
        }
        abi::OK
    })
}

pub(crate) unsafe extern "C" fn cancel_notify(
    cancel: *mut abi::Cancel,
    callback: Option<unsafe extern "C" fn(*mut c_void)>,
    context: *mut c_void,
) -> u32 {
    contained(|| {
        let Some(callback) = callback else {
            return abi::INVALID_ARGUMENT;
        };
        if cancel.is_null() {
            return abi::INVALID_ARGUMENT;
        }
        // Safety: as in `cancelled`.
        let cancel = unsafe { &*cancel.cast::<Cancel>() };
        let mut state = cancel.state.lock().expect("cancel state");
        if state.cancelled {
            drop(state);
            // Safety: the module gives this pair for this call.
            unsafe { callback(context) };
        } else {
            state.notify.push((callback, SendPtr(context)));
        }
        abi::OK
    })
}

/// The table's parts that are read from any thread.
#[derive(Clone, Copy)]
struct Table {
    run: unsafe extern "C" fn(*mut abi::Cancel, *mut c_void) -> u32,
    context: SendPtr,
}

pub(crate) struct ModuleService {
    name: String,
    table: Table,
}

impl ModuleService {
    pub(crate) fn new(
        name: String,
        service: Service,
    ) -> Option<Self> {
        Some(Self {
            name,
            table: Table {
                run: service.run?,
                context: SendPtr(service.context),
            },
        })
    }
}

#[async_trait]
impl Runnable<AppState> for ModuleService {
    async fn run(
        self: Arc<Self>,
        _: (),
        _: AppState,
        context: RunContext,
    ) -> registry::Result<()> {
        let cancel = Arc::new(Cancel::default());
        let (finished, status) = oneshot::channel();
        let thread_cancel = Arc::clone(&cancel);
        let table = self.table;
        std::thread::Builder::new()
            .name(format!("module:{}", self.name))
            .spawn(move || {
                let table = table;
                let handle = Arc::as_ptr(&thread_cancel).cast_mut().cast();
                let status = contained(|| unsafe {
                    (table.run)(handle, table.context.0)
                });
                let _ = finished.send(status);
            })
            .map_err(|error| registry::Error::msg(error.to_string()))?;
        tokio::pin!(status);
        let status = tokio::select! {
            status = &mut status => status,
            () = context.cancelled() => {
                cancel.cancel();
                // A native call cannot be interrupted: drain waits for it.
                status.await
            }
        };
        // One module's service failing is its own: the service reads failed and
        // the server carries on. Restarting it is a command, not an implicit retry.
        let failure = match status {
            Ok(abi::OK) => return Ok(()),
            Ok(status) => format!("returned status {status}"),
            Err(_) => "ended without a status".to_owned(),
        };
        Err(registry::Error::recoverable(format!(
            "module service '{}' {failure}",
            self.name
        )))
    }
}
