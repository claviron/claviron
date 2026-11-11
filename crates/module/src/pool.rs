//! The OS threads an application's handlers run on. A queue, a condvar and a
//! fixed set of threads: they park until there is work and leave when it closes.

use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;

type Task = Box<dyn FnOnce() + Send>;

#[derive(Default)]
struct Queue {
    tasks: VecDeque<Task>,
    running: usize,
    closed: bool,
}

#[derive(Default)]
struct Shared {
    queue: Mutex<Queue>,
    ready: Condvar,
}

pub(crate) struct Pool {
    shared: Arc<Shared>,
    instances: usize,
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
        Ok(Self {
            shared,
            instances: usize::from(instances),
            threads: Mutex::new(threads),
        })
    }

    /// Submit continuation or cleanup work for an already accepted exchange.
    pub(crate) fn submit(
        &self,
        task: impl FnOnce() + Send + 'static,
    ) -> bool {
        self.enqueue(task, |_| true).is_some()
    }

    /// Reserve an idle thread or ask for one waiting place before enqueueing.
    /// The return value says whether the new exchange had to wait.
    pub(crate) fn submit_exchange(
        &self,
        task: impl FnOnce() + Send + 'static,
        wait: impl FnOnce() -> bool,
    ) -> Option<bool> {
        self.enqueue(task, |waiting| !waiting || wait())
    }

    fn enqueue(
        &self,
        task: impl FnOnce() + Send + 'static,
        admit: impl FnOnce(bool) -> bool,
    ) -> Option<bool> {
        let mut queue = self.shared.queue.lock().expect("pool queue");
        if queue.closed {
            return None;
        }
        let waiting = queue.running + queue.tasks.len() >= self.instances;
        if !admit(waiting) {
            return None;
        }
        queue.tasks.push_back(Box::new(task));
        drop(queue);
        self.shared.ready.notify_one();
        Some(waiting)
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
    let mut queue = shared.queue.lock().expect("pool queue");
    loop {
        if let Some(task) = queue.tasks.pop_front() {
            queue.running += 1;
            drop(queue);
            task();
            queue = shared.queue.lock().expect("pool queue");
            queue.running -= 1;
        } else if queue.closed {
            return;
        } else {
            queue = shared.ready.wait(queue).expect("pool queue");
        }
    }
}
