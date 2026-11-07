//! The OS threads an application's handlers run on. A queue, a condvar and a
//! fixed set of threads: they park until there is work and leave when it closes.

use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;

type Task = Box<dyn FnOnce() + Send>;

#[derive(Default)]
struct Queue {
    tasks: VecDeque<Task>,
    closed: bool,
}

#[derive(Default)]
struct Shared {
    queue: Mutex<Queue>,
    ready: Condvar,
}

pub(crate) struct Pool {
    shared: Arc<Shared>,
    threads: Mutex<Vec<JoinHandle<()>>>,
}

impl Pool {
    pub(crate) fn start(
        name: &str,
        instances: u16,
    ) -> std::io::Result<Self> {
        let shared = Arc::new(Shared::default());
        let mut threads = Vec::with_capacity(instances.into());
        for instance in 0..instances {
            let shared = Arc::clone(&shared);
            threads.push(
                std::thread::Builder::new()
                    .name(format!("module:{name}:{instance}"))
                    .spawn(move || work(&shared))?,
            );
        }
        Ok(Self { shared, threads: Mutex::new(threads) })
    }

    /// Queue work; `false` once closed. What bounds the queue is the caller's
    /// admission: at most `max_waiting` new requests, and a piece of work per
    /// exchange otherwise.
    pub(crate) fn submit(
        &self,
        task: impl FnOnce() + Send + 'static,
    ) -> bool {
        let mut queue = self.shared.queue.lock().expect("pool queue");
        if queue.closed {
            return false;
        }
        queue.tasks.push_back(Box::new(task));
        drop(queue);
        self.shared.ready.notify_one();
        true
    }

    /// Finish what is queued and join the threads. A native call that hangs
    /// holds this up: it cannot be interrupted.
    pub(crate) fn shutdown(&self) {
        self.shared.queue.lock().expect("pool queue").closed = true;
        self.shared.ready.notify_all();
        let threads =
            std::mem::take(&mut *self.threads.lock().expect("threads"));
        for thread in threads {
            let _ = thread.join();
        }
    }
}

fn work(shared: &Shared) {
    loop {
        let task = {
            let mut queue = shared.queue.lock().expect("pool queue");
            loop {
                if let Some(task) = queue.tasks.pop_front() {
                    break task;
                }
                if queue.closed {
                    return;
                }
                queue = shared.ready.wait(queue).expect("pool queue");
            }
        };
        task();
    }
}
