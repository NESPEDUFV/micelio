//! Reference: https://michaelhelvey.dev/posts/rust_async_runtime
use crate::ffi;
use std::{
    collections::HashMap,
    pin::Pin,
    sync::{
        Arc, LazyLock, Mutex,
        mpsc::{Receiver, Sender, channel},
    },
    task::{Context, Poll, RawWaker, RawWakerVTable, Waker},
    time::Instant,
};

pub(crate) struct TaskFuture<'a> {
    future: Pin<Box<dyn Future<Output = ()> + Send + 'a>>,
    poll: Poll<()>,
}

impl<'a> TaskFuture<'a> {
    pub(crate) fn new(future: impl Future<Output = ()> + Send + 'a) -> Self {
        Self {
            future: Box::pin(future),
            poll: Poll::Pending,
        }
    }

    pub(crate) fn poll(&mut self, cx: &mut Context<'_>) {
        if self.poll.is_pending() {
            self.poll = self.future.as_mut().poll(cx);
        }
    }
}

pub(crate) struct Task<'a> {
    queue: Sender<Arc<Task<'a>>>,
    future: Arc<Mutex<TaskFuture<'a>>>,
    context: u32,
}

impl<'a> Task<'a> {
    pub(crate) fn poll(self: Arc<Self>) {
        let self_ptr = Arc::into_raw(self.clone()).cast::<()>();
        let waker = unsafe { Waker::from_raw(RawWaker::new(self_ptr, create_arc_task_vtable())) };
        let mut context = Context::from_waker(&waker);
        let Ok(mut task) = self.future.lock() else {
            return;
        };
        task.poll(&mut context);
    }

    pub(crate) fn spawn(
        future: impl Future<Output = ()> + Send + 'a,
        queue: &Sender<Arc<Task<'a>>>,
        context: u32,
    ) {
        let task = Arc::new(Task {
            queue: queue.clone(),
            future: Arc::new(Mutex::new(TaskFuture::new(future))),
            context,
        });
        queue
            .send(task)
            .inspect_err(|e| eprintln!("error in Task::spawn: {e}"))
            .expect("should send task");
    }

    pub(crate) fn schedule(self: &Arc<Self>) {
        self.queue
            .send(self.clone())
            .inspect_err(|e| eprintln!("error in Task::spawn: {e}"))
            .expect("should send task");
    }
}

fn create_arc_task_vtable() -> &'static RawWakerVTable {
    &RawWakerVTable::new(
        clone_arc_task_raw,
        wake_arc_task_raw,
        wake_arc_task_by_ref_raw,
        drop_arc_task_raw,
    )
}

unsafe fn clone_arc_task_raw(data: *const ()) -> RawWaker {
    let _arc = std::mem::ManuallyDrop::new(unsafe { Arc::from_raw(data.cast::<Task>()) });
    let _arc_clone: std::mem::ManuallyDrop<_> = _arc.clone();
    RawWaker::new(data, create_arc_task_vtable())
}

unsafe fn drop_arc_task_raw(data: *const ()) {
    drop(unsafe { Arc::from_raw(data.cast::<Task>()) });
}

unsafe fn wake_arc_task_raw(data: *const ()) {
    let arc = unsafe { Arc::from_raw(data.cast::<Task>()) };
    Task::schedule(&arc);
}

unsafe fn wake_arc_task_by_ref_raw(data: *const ()) {
    let arc = std::mem::ManuallyDrop::new(unsafe { Arc::from_raw(data.cast::<Task>()) });
    Task::schedule(&arc);
}

#[derive(Default)]
pub(crate) struct Reactor {
    waker_map: HashMap<usize, Waker>,
    current_key: usize,
}

impl Reactor {
    pub(crate) fn register_waker(&mut self, key: usize, waker: Waker) {
        self.waker_map.insert(key, waker);
    }

    pub(crate) fn deregister_waker(&mut self, key: usize) {
        self.waker_map.remove(&key);
    }

    pub(crate) fn wake(&self, key: usize) {
        if let Some(waker) = self.waker_map.get(&key) {
            waker.wake_by_ref();
        }
    }

    pub(crate) fn next_key(&mut self) -> usize {
        self.current_key = self.current_key.wrapping_add(1).max(1);
        self.current_key
    }
}

pub(crate) static REACTOR: LazyLock<Mutex<Reactor>> =
    LazyLock::new(|| Mutex::new(Reactor::default()));

thread_local! {
    pub(crate) static RUNTIME: Runtime<'static> = Runtime::new();
}

/// An async runtime intended to work with ns3 single-thread execution model.
pub struct Runtime<'a> {
    queue_tx: Sender<Arc<Task<'a>>>,
    queue_rx: Receiver<Arc<Task<'a>>>,
    run_t0: Mutex<Instant>,
    context: Mutex<Option<u32>>,
}

impl<'a> Runtime<'a> {
    pub fn new() -> Self {
        let (queue_tx, queue_rx) = channel();
        let run_t0 = Mutex::new(Instant::now());
        let context = Mutex::new(None);
        Self {
            queue_tx,
            queue_rx,
            run_t0,
            context,
        }
    }

    /// Spawns a new task to be run. Execution will only begin after [`Runtime::run`] is called.
    pub fn spawn(&self, future: impl Future<Output = ()> + Send + 'a) {
        Task::spawn(future, &self.queue_tx, self.context());
    }

    /// Spawns a new task to be run on a given context.
    /// Execution will only begin after [`Runtime::run`] is called.
    pub fn spawn_on_context(&self, context: u32, future: impl Future<Output = ()> + Send + 'a) {
        Task::spawn(future, &self.queue_tx, context);
    }

    /// Executes all tasks that can be processed in the current simulated time.
    /// Different from conventional async runtimes, if an actual block were to happen and
    /// could be simulated, future events will be scheduled in NS3 to resume execution at a
    /// later simulated time.
    pub fn run(&self) {
        {
            let mut t0 = self.run_t0.lock().expect("should get the lock to t0");
            *t0 = Instant::now();
        }
        while let Ok(task) = self.queue_rx.try_recv() {
            {
                let mut ctx = self.context.lock().expect("should get the lock to context");
                *ctx = Some(task.context);
            }
            task.poll();
        }
    }

    pub fn context(&self) -> u32 {
        match *self
            .context
            .lock()
            .expect("should get the lock to read context")
        {
            Some(ctx) => ctx,
            None => ffi::get_context(),
        }
    }

    /// The real time instant the current async execution started.
    ///
    /// This can be used to determine how much real time has elapsed since
    /// the async runtime started running.
    pub(crate) fn t0(&self) -> Instant {
        *self.run_t0.lock().expect("should get the lock to read t0")
    }
}
