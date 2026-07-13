use std::cell::Cell;
use std::future::Future;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{mpsc as std_mpsc, Mutex, OnceLock};
use std::thread::JoinHandle as ThreadJoinHandle;
use std::time::{Duration, Instant};

use tokio::runtime::Runtime;
use tokio::task::{AbortHandle, JoinHandle};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::error::SpannerError;

type RuntimeJob = Box<dyn FnOnce(&mut RuntimeContext) + Send + 'static>;

enum RuntimeRequest {
    Run(RuntimeJob),
    Shutdown {
        deadline: Instant,
        result: std_mpsc::SyncSender<Result<(), String>>,
    },
}

struct RuntimeOwner {
    requests: std_mpsc::Sender<RuntimeRequest>,
    thread: Mutex<Option<ThreadJoinHandle<()>>>,
}

struct RuntimeContext {
    runtime: Runtime,
    tasks: TaskTracker,
    abort_handles: Vec<AbortHandle>,
}

static RUNTIME_OWNER: OnceLock<Result<RuntimeOwner, String>> = OnceLock::new();

// Tokio task-locals are not inherited by spawned child tasks. Marking the
// dedicated runtime's worker threads rejects reentry from all descendants too.
thread_local! {
    static EXTENSION_RUNTIME_THREAD: Cell<bool> = const { Cell::new(false) };
}

// Cap at 4 threads — we don't need many since this is mostly I/O (gRPC).
// Shared with query.rs/scan.rs so concurrent partition execution doesn't
// oversubscribe the runtime's worker threads.
const MAX_WORKER_THREADS: usize = 4;
const OWNER_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);
const OWNER_SHUTDOWN_POLL_INTERVAL: Duration = Duration::from_millis(1);

/// Bound foreground metadata work so a bind callback cannot wait indefinitely
/// for an unreachable endpoint or a stalled discovery RPC.
pub(crate) const METADATA_DISCOVERY_TIMEOUT: Duration = Duration::from_secs(60);

/// Error returned when a synchronous wait on the runtime owner expires.
#[derive(Debug)]
pub(crate) enum RuntimeWaitError {
    TimedOut {
        operation: &'static str,
        timeout: Duration,
    },
    Runtime(SpannerError),
}

impl RuntimeWaitError {
    pub(crate) fn into_spanner_error(self) -> SpannerError {
        match self {
            Self::TimedOut { operation, timeout } => SpannerError::Other(format!(
                "Timed out waiting for {operation} after {}s; submitted work was cancelled",
                timeout.as_secs_f64()
            )),
            Self::Runtime(error) => error,
        }
    }
}

impl std::fmt::Display for RuntimeWaitError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TimedOut { operation, timeout } => write!(
                formatter,
                "Timed out waiting for {operation} after {}s; submitted work was cancelled",
                timeout.as_secs_f64()
            ),
            Self::Runtime(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for RuntimeWaitError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::TimedOut { .. } => None,
            Self::Runtime(error) => Some(error),
        }
    }
}

// DuckDB's C extension API has no unload hook, so the global owner and cached Spanner
// clients live for the process lifetime. RuntimeOwner still has a complete shutdown
// path for tests and any future host lifecycle hook.

/// Number of worker threads the shared Tokio runtime is (or will be) built with.
pub fn worker_threads() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get().min(MAX_WORKER_THREADS))
        .unwrap_or(2)
}

/// Limit local partition execution independently from Spanner's partition-count hint.
pub fn partition_worker_limit(returned_partitions: usize, max_parallelism: Option<i64>) -> usize {
    let requested_limit = max_parallelism
        .and_then(|max| usize::try_from(max).ok())
        .unwrap_or(usize::MAX);
    worker_threads()
        .min(requested_limit)
        .min(returned_partitions)
}

impl RuntimeContext {
    fn spawn<F>(&mut self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.abort_handles.retain(|handle| !handle.is_finished());
        let task = self.tasks.spawn_on(future, self.runtime.handle());
        self.abort_handles.push(task.abort_handle());
        task
    }

    fn shutdown(mut self, deadline: Instant) -> Result<(), SpannerError> {
        self.tasks.close();
        self.abort_handles.retain(|handle| !handle.is_finished());
        for handle in &self.abort_handles {
            handle.abort();
        }

        // The waiter is deliberately untracked: waiting for the tracker from a
        // tracked task would keep the tracker non-empty forever.
        let tracker = self.tasks.clone();
        let (drained_tx, drained_rx) = std_mpsc::sync_channel(1);
        self.runtime.spawn(async move {
            tracker.wait().await;
            let _ = drained_tx.send(());
        });
        let drained = matches!(drained_rx.recv_timeout(remaining_until(deadline)), Ok(()));
        self.runtime.shutdown_timeout(remaining_until(deadline));

        if drained {
            Ok(())
        } else {
            Err(SpannerError::Other(
                "Runtime owner shutdown timed out while draining tasks".to_string(),
            ))
        }
    }
}

impl RuntimeOwner {
    fn start() -> Result<Self, SpannerError> {
        Self::start_with_worker_threads(worker_threads())
    }

    fn start_with_worker_threads(worker_threads: usize) -> Result<Self, SpannerError> {
        let (request_tx, request_rx) = std_mpsc::channel();
        let (startup_tx, startup_rx) = std_mpsc::sync_channel(1);
        let thread = std::thread::Builder::new()
            .name("duckdb-spanner-runtime-owner".to_string())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(worker_threads)
                    .on_thread_start(|| EXTENSION_RUNTIME_THREAD.set(true))
                    .on_thread_stop(|| EXTENSION_RUNTIME_THREAD.set(false))
                    .enable_all()
                    .build();
                let runtime = match runtime {
                    Ok(runtime) => {
                        let _ = startup_tx.send(Ok::<(), String>(()));
                        runtime
                    }
                    Err(error) => {
                        let _ = startup_tx
                            .send(Err(format!("Failed to create Tokio runtime: {error}")));
                        return;
                    }
                };

                let mut context = RuntimeContext {
                    runtime,
                    tasks: TaskTracker::new(),
                    abort_handles: Vec::new(),
                };
                let mut shutdown_request = None;
                while let Ok(request) = request_rx.recv() {
                    match request {
                        RuntimeRequest::Run(job) => {
                            if catch_unwind(AssertUnwindSafe(|| job(&mut context))).is_err() {
                                // Each job owns its response sender, so unwinding drops
                                // it and wakes the caller with an explicit bridge error.
                                eprintln!("[duckdb-spanner] runtime owner request panicked");
                            }
                        }
                        RuntimeRequest::Shutdown { deadline, result } => {
                            shutdown_request = Some((deadline, result));
                            break;
                        }
                    }
                }

                let (deadline, result_tx) = shutdown_request
                    .map(|(deadline, result)| (deadline, Some(result)))
                    .unwrap_or_else(|| (Instant::now() + OWNER_SHUTDOWN_TIMEOUT, None));
                let result = context
                    .shutdown(deadline)
                    .map_err(|error| error.to_string());
                if let Some(result_tx) = result_tx {
                    let _ = result_tx.send(result);
                }
            })
            .map_err(|error| {
                SpannerError::Other(format!("Failed to start runtime owner thread: {error}"))
            })?;

        match startup_rx.recv() {
            Ok(Ok(())) => Ok(Self {
                requests: request_tx,
                thread: Mutex::new(Some(thread)),
            }),
            Ok(Err(error)) => {
                let _ = thread.join();
                Err(SpannerError::Other(error))
            }
            Err(error) => {
                let _ = thread.join();
                Err(SpannerError::Other(format!(
                    "Runtime owner stopped during startup: {error}"
                )))
            }
        }
    }

    fn submit(&self, request: RuntimeRequest) -> Result<(), SpannerError> {
        self.requests
            .send(request)
            .map_err(|_| SpannerError::Other("Runtime owner is not available".to_string()))
    }

    fn run<F>(&self, future: F) -> Result<F::Output, SpannerError>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.run_with_timeout(None, future)
            .map_err(RuntimeWaitError::into_spanner_error)
    }

    fn run_bounded<F>(
        &self,
        operation: &'static str,
        timeout: Duration,
        future: F,
    ) -> Result<F::Output, RuntimeWaitError>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.run_with_timeout(Some((operation, timeout)), future)
    }

    fn run_with_timeout<F>(
        &self,
        timeout: Option<(&'static str, Duration)>,
        future: F,
    ) -> Result<F::Output, RuntimeWaitError>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        if EXTENSION_RUNTIME_THREAD.get() {
            return Err(RuntimeWaitError::Runtime(SpannerError::Other(
                "runtime::run cannot be called from an extension-owned runtime task".to_string(),
            )));
        }

        let (response_tx, response_rx) = std_mpsc::sync_channel(1);
        let (abort_tx, abort_rx) = std_mpsc::sync_channel(1);
        let cancellation = CancellationToken::new();
        let request_cancellation = cancellation.clone();
        let request_cancellation_before_spawn = request_cancellation.clone();
        self.submit(RuntimeRequest::Run(Box::new(move |context| {
            if request_cancellation_before_spawn.is_cancelled() {
                let _ = response_tx.send(Err(SpannerError::Other(
                    "Runtime request was cancelled".to_string(),
                )));
                return;
            }
            let mut task = context.spawn(future);
            let fallback_abort = task.abort_handle();
            let _ = abort_tx.send(fallback_abort.clone());
            let reporter = async move {
                let result = tokio::select! {
                    biased;
                    _ = request_cancellation.cancelled() => {
                        task.abort();
                        let _ = task.await;
                        Err(SpannerError::Other("Runtime request was cancelled".to_string()))
                    }
                    result = &mut task => join_result(result),
                };
                let _ = response_tx.send(result);
            };
            if catch_unwind(AssertUnwindSafe(|| context.spawn(reporter))).is_err() {
                fallback_abort.abort();
            }
        })))
        .map_err(RuntimeWaitError::Runtime)?;

        let response = match timeout {
            Some((operation, timeout)) => {
                let deadline = Instant::now()
                    .checked_add(timeout)
                    .unwrap_or_else(Instant::now);
                let abort = match abort_rx.recv_timeout(remaining_until(deadline)) {
                    Ok(abort) => abort,
                    Err(std_mpsc::RecvTimeoutError::Timeout) => {
                        cancellation.cancel();
                        return Err(RuntimeWaitError::TimedOut { operation, timeout });
                    }
                    Err(std_mpsc::RecvTimeoutError::Disconnected) => {
                        cancellation.cancel();
                        return Err(RuntimeWaitError::Runtime(SpannerError::Other(
                            "Runtime request ended before its task was submitted".to_string(),
                        )));
                    }
                };
                match response_rx.recv_timeout(remaining_until(deadline)) {
                    Ok(result) => result,
                    Err(std_mpsc::RecvTimeoutError::Timeout) => {
                        cancellation.cancel();
                        abort.abort();
                        return Err(RuntimeWaitError::TimedOut { operation, timeout });
                    }
                    Err(std_mpsc::RecvTimeoutError::Disconnected) => {
                        cancellation.cancel();
                        abort.abort();
                        return Err(RuntimeWaitError::Runtime(SpannerError::Other(
                            "Runtime request ended without a response".to_string(),
                        )));
                    }
                }
            }
            None => match response_rx.recv() {
                Ok(result) => result,
                Err(_) => {
                    cancellation.cancel();
                    return Err(RuntimeWaitError::Runtime(SpannerError::Other(
                        "Runtime request ended without a response".to_string(),
                    )));
                }
            },
        };

        response.map_err(RuntimeWaitError::Runtime)
    }

    fn spawn<F>(&self, future: F) -> Result<JoinHandle<F::Output>, SpannerError>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let (response_tx, response_rx) = std_mpsc::sync_channel(1);
        self.submit(RuntimeRequest::Run(Box::new(move |context| {
            let result = catch_unwind(AssertUnwindSafe(|| context.spawn(future))).map_err(|_| {
                SpannerError::Other("Runtime owner panicked while spawning a task".to_string())
            });
            let _ = response_tx.send(result);
        })))?;
        response_rx.recv().map_err(|_| {
            SpannerError::Other("Runtime spawn request ended without a response".to_string())
        })?
    }

    fn shutdown(&self) -> Result<(), SpannerError> {
        self.shutdown_with_timeout(OWNER_SHUTDOWN_TIMEOUT)
    }

    fn shutdown_with_timeout(&self, timeout: Duration) -> Result<(), SpannerError> {
        let thread = self
            .thread
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take();
        let Some(thread) = thread else {
            return Ok(());
        };

        let deadline = Instant::now()
            .checked_add(timeout)
            .unwrap_or_else(Instant::now);
        let (result_tx, result_rx) = std_mpsc::sync_channel(1);
        self.submit(RuntimeRequest::Shutdown {
            deadline,
            result: result_tx,
        })?;

        while !thread.is_finished() {
            let remaining = remaining_until(deadline);
            if remaining.is_zero() {
                if thread.is_finished() {
                    break;
                }
                return Err(SpannerError::Other(format!(
                    "Runtime owner shutdown timed out after {}ms",
                    timeout.as_millis()
                )));
            }
            std::thread::sleep(remaining.min(OWNER_SHUTDOWN_POLL_INTERVAL));
        }

        // Joining is non-blocking now: is_finished was observed within the
        // shared deadline. A timed-out path returns above and drops the handle.
        thread.join().map_err(|_| {
            SpannerError::Other("Runtime owner thread panicked during shutdown".to_string())
        })?;
        match result_rx.try_recv() {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => Err(SpannerError::Other(error)),
            Err(std_mpsc::TryRecvError::Empty | std_mpsc::TryRecvError::Disconnected) => {
                Err(SpannerError::Other(
                    "Runtime owner stopped without reporting shutdown status".to_string(),
                ))
            }
        }
    }
}

impl Drop for RuntimeOwner {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

fn get_or_init_owner() -> Result<&'static RuntimeOwner, SpannerError> {
    match RUNTIME_OWNER.get_or_init(|| RuntimeOwner::start().map_err(|error| error.to_string())) {
        Ok(owner) => Ok(owner),
        Err(error) => Err(SpannerError::Other(error.clone())),
    }
}

fn join_result<T>(result: Result<T, tokio::task::JoinError>) -> Result<T, SpannerError> {
    match result {
        Ok(output) => Ok(output),
        Err(error) if error.is_panic() => Err(SpannerError::Other(format!(
            "Runtime task panicked: {error}"
        ))),
        Err(error) => Err(SpannerError::Other(format!(
            "Runtime task was cancelled: {error}"
        ))),
    }
}

fn remaining_until(deadline: Instant) -> Duration {
    deadline.saturating_duration_since(Instant::now())
}

/// Run async work on the extension-owned runtime and wait through std synchronization.
pub fn run<F>(future: F) -> Result<F::Output, SpannerError>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    get_or_init_owner()?.run(future)
}

/// Run async foreground work with a finite synchronous wait.
///
/// On expiry, the runtime request's cancellation token is signalled. The
/// reporter aborts the submitted task rather than allowing the caller to treat
/// a stalled request as a clean end of stream.
pub(crate) fn run_bounded<F>(
    operation: &'static str,
    timeout: Duration,
    future: F,
) -> Result<F::Output, RuntimeWaitError>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    get_or_init_owner()
        .map_err(RuntimeWaitError::Runtime)?
        .run_bounded(operation, timeout, future)
}

pub fn spawn<F>(future: F) -> Result<JoinHandle<F::Output>, SpannerError>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    get_or_init_owner()?.spawn(future)
}

/// Await an operation unless stream cancellation has already been requested.
///
/// Cancellation is biased so it wins when the operation becomes ready at the
/// same time, preventing new RPC or row-delivery work after teardown starts.
pub async fn await_or_cancel<F>(cancellation: &CancellationToken, future: F) -> Option<F::Output>
where
    F: std::future::Future,
{
    tokio::select! {
        biased;
        _ = cancellation.cancelled() => None,
        output = future => Some(output),
    }
}

/// Send a row unless cancellation or receiver closure wins first.
pub async fn send_or_cancel<T>(
    cancellation: &CancellationToken,
    tx: &tokio::sync::mpsc::Sender<T>,
    value: T,
) -> bool {
    matches!(
        await_or_cancel(cancellation, tx.send(value)).await,
        Some(Ok(()))
    )
}

/// Run partition jobs without allocating a Tokio task for every returned partition.
///
/// At most `max_concurrency` tasks exist at once. The first error or cancellation
/// stops new work, aborts in-flight tasks, and drains them before returning.
pub async fn run_bounded_partitions<I, T, F, Fut>(
    partitions: I,
    max_concurrency: usize,
    cancellation: CancellationToken,
    mut run: F,
) -> Result<(), SpannerError>
where
    I: IntoIterator<Item = T>,
    I::IntoIter: Send,
    T: Send + 'static,
    F: FnMut(T, CancellationToken) -> Fut + Send,
    Fut: std::future::Future<Output = Result<(), SpannerError>> + Send + 'static,
{
    let mut partitions = partitions.into_iter();
    if cancellation.is_cancelled() {
        return Ok(());
    }
    if max_concurrency == 0 {
        return if partitions.next().is_none() {
            Ok(())
        } else {
            Err(SpannerError::Other(
                "Partition worker limit must be greater than zero".to_string(),
            ))
        };
    }

    let mut join_set = tokio::task::JoinSet::new();
    for _ in 0..max_concurrency {
        let Some(partition) = partitions.next() else {
            break;
        };
        join_set.spawn(run(partition, cancellation.clone()));
    }

    loop {
        let joined = tokio::select! {
            biased;
            _ = cancellation.cancelled() => {
                join_set.shutdown().await;
                return Ok(());
            }
            joined = join_set.join_next() => joined,
        };
        let Some(joined) = joined else {
            return Ok(());
        };
        let result = match joined {
            Ok(result) => result,
            Err(error) if error.is_cancelled() => Ok(()),
            Err(error) => Err(SpannerError::Other(format!(
                "Partition task join error: {error}"
            ))),
        };

        if let Err(error) = result {
            join_set.shutdown().await;
            return Err(error);
        }

        if cancellation.is_cancelled() {
            join_set.shutdown().await;
            return Ok(());
        }
        if let Some(partition) = partitions.next() {
            join_set.spawn(run(partition, cancellation.clone()));
        }
    }
}

#[cfg(test)]
pub(crate) mod test_support {
    use std::future::Future;
    use std::time::{Duration, Instant};

    use tokio::task::AbortHandle;

    use super::RuntimeOwner;
    use crate::error::SpannerError;

    const CHILD_TEST: &str = "DUCKDB_SPANNER_RUNTIME_CHILD_TEST";
    const CHILD_MARKER: &str = "DUCKDB_SPANNER_RUNTIME_CHILD_MARKER";

    pub struct TestRuntimeOwner(RuntimeOwner);

    impl TestRuntimeOwner {
        pub fn start(worker_threads: usize) -> Result<Self, SpannerError> {
            RuntimeOwner::start_with_worker_threads(worker_threads).map(Self)
        }

        pub fn spawn_detached<F>(&self, future: F) -> Result<AbortHandle, SpannerError>
        where
            F: Future<Output = ()> + Send + 'static,
        {
            let task = self.0.spawn(future)?;
            let abort_handle = task.abort_handle();
            drop(task);
            Ok(abort_handle)
        }

        pub fn shutdown(&self, timeout: Duration) -> Result<(), SpannerError> {
            self.0.shutdown_with_timeout(timeout)
        }
    }

    pub fn is_child(test_name: &str) -> bool {
        std::env::var(CHILD_TEST).is_ok_and(|value| value == test_name)
    }

    pub fn finish_child() {
        std::fs::write(std::env::var(CHILD_MARKER).unwrap(), "ok").unwrap();
    }

    pub fn run(test_name: &str, timeout: Duration) {
        let marker = std::env::temp_dir().join(format!(
            "duckdb-spanner-runtime-test-{}-{}",
            std::process::id(),
            test_name.replace("::", "-")
        ));
        let _ = std::fs::remove_file(&marker);
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg(test_name)
            .arg("--nocapture")
            .env(CHILD_TEST, test_name)
            .env(CHILD_MARKER, &marker)
            .spawn()
            .expect("failed to start runtime regression subprocess");

        let deadline = Instant::now() + timeout;
        let status = loop {
            if let Some(status) = child
                .try_wait()
                .expect("failed to poll runtime regression subprocess")
            {
                break status;
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                panic!("runtime regression subprocess timed out: {test_name}");
            }
            std::thread::sleep(Duration::from_millis(10));
        };

        assert!(
            status.success(),
            "runtime regression subprocess failed: {test_name} ({status})"
        );
        let marker_contents = std::fs::read_to_string(&marker)
            .expect("runtime regression child did not write its completion marker");
        let _ = std::fs::remove_file(marker);
        assert_eq!(marker_contents, "ok");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::mpsc::{sync_channel, SyncSender};
    use std::sync::Arc;
    use std::task::Poll;
    use std::time::Duration;
    use tokio::sync::oneshot;

    struct DropSignal(Option<oneshot::Sender<()>>);

    struct StdDropSignal(Option<SyncSender<()>>);

    struct SlowDrop(Duration);

    impl Drop for DropSignal {
        fn drop(&mut self) {
            if let Some(sender) = self.0.take() {
                let _ = sender.send(());
            }
        }
    }

    impl Drop for StdDropSignal {
        fn drop(&mut self) {
            if let Some(sender) = self.0.take() {
                let _ = sender.send(());
            }
        }
    }

    impl Drop for SlowDrop {
        fn drop(&mut self) {
            std::thread::sleep(self.0);
        }
    }

    #[test]
    fn worker_threads_is_at_least_one_and_capped_at_max() {
        let n = worker_threads();
        // available_parallelism() can legitimately return 1 on single-core or
        // cgroup-limited runners, so the floor is 1, not 2.
        assert!(n >= 1, "expected at least 1 worker thread, got {n}");
        assert!(
            n <= MAX_WORKER_THREADS,
            "expected at most {MAX_WORKER_THREADS} worker threads, got {n}"
        );
    }

    #[test]
    fn partition_worker_limit_is_bounded_by_returned_partitions() {
        assert_eq!(partition_worker_limit(0, Some(1)), 0);
        assert_eq!(partition_worker_limit(1, Some(1)), 1);
        assert!(partition_worker_limit(10, Some(2)) <= 2);
    }

    fn assert_run_from_tokio_worker() {
        let caller = std::thread::current().id();
        let (worker, value) = run(async move {
            tokio::time::sleep(Duration::from_millis(1)).await;
            (std::thread::current().id(), 42)
        })
        .unwrap();

        assert_ne!(
            worker, caller,
            "async work ran on the caller's Tokio thread"
        );
        assert_eq!(value, 42);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn run_is_safe_from_a_current_thread_runtime() {
        tokio::task::yield_now().await;
        assert_run_from_tokio_worker();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn run_is_safe_from_a_multithread_runtime_worker() {
        tokio::task::yield_now().await;
        assert_run_from_tokio_worker();
    }

    #[test]
    fn task_panic_is_reported_and_owner_remains_usable() {
        let error = run(async move {
            panic!("runtime bridge panic probe");
        })
        .unwrap_err();
        assert!(error.to_string().contains("Runtime task panicked"));
        assert_eq!(run(async move { 7 }).unwrap(), 7);
    }

    #[test]
    fn bounded_run_times_out_and_aborts_submitted_work() {
        let owner = RuntimeOwner::start().unwrap();
        let (started_tx, started_rx) = sync_channel(1);
        let (dropped_tx, dropped_rx) = sync_channel(1);

        let error = owner
            .run_bounded(
                "test foreground request",
                Duration::from_millis(10),
                async move {
                    let _drop_signal = StdDropSignal(Some(dropped_tx));
                    started_tx.send(()).unwrap();
                    std::future::pending::<usize>().await
                },
            )
            .unwrap_err();
        assert!(matches!(
            error,
            RuntimeWaitError::TimedOut {
                operation: "test foreground request",
                ..
            }
        ));
        started_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("bounded request did not start");
        dropped_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("bounded request was not aborted");
        owner.shutdown().unwrap();
    }

    #[test]
    fn runtime_wait_error_preserves_runtime_error_source() {
        let error = RuntimeWaitError::Runtime(SpannerError::Other("inner error".to_string()));
        let source = std::error::Error::source(&error).expect("runtime source");
        assert_eq!(source.to_string(), "inner error");
    }

    #[test]
    fn run_reentry_is_rejected_on_a_single_worker_owner_subprocess() {
        test_support::run(
            "runtime::tests::run_reentry_is_rejected_on_a_single_worker_owner_child",
            Duration::from_secs(5),
        );
    }

    #[test]
    fn run_reentry_is_rejected_on_a_single_worker_owner_child() {
        const TEST_NAME: &str =
            "runtime::tests::run_reentry_is_rejected_on_a_single_worker_owner_child";
        if !test_support::is_child(TEST_NAME) {
            return;
        }

        let owner = Arc::new(RuntimeOwner::start_with_worker_threads(1).unwrap());
        let reentrant_owner = Arc::clone(&owner);
        let error = owner
            .run(async move { reentrant_owner.run(async move { 42 }) })
            .unwrap()
            .unwrap_err();
        assert!(error.to_string().contains("extension-owned runtime task"));
        owner.shutdown().unwrap();
        test_support::finish_child();
    }

    #[test]
    fn owner_shutdown_aborts_and_drains_tracked_tasks() {
        let owner = RuntimeOwner::start().unwrap();
        let (started_tx, started_rx) = sync_channel(1);
        let (dropped_tx, dropped_rx) = sync_channel(1);
        let _task = owner
            .spawn(async move {
                let _drop_signal = StdDropSignal(Some(dropped_tx));
                started_tx.send(()).unwrap();
                std::future::pending::<()>().await;
            })
            .unwrap();

        started_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("tracked task did not start");
        owner.shutdown().unwrap();
        dropped_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("owner shutdown returned before the tracked task drained");
    }

    #[test]
    fn owner_shutdown_reports_success_after_a_slow_bounded_drain() {
        let owner = RuntimeOwner::start_with_worker_threads(1).unwrap();
        let (started_tx, started_rx) = sync_channel(1);
        let _task = owner
            .spawn(async move {
                let _slow_drop = SlowDrop(Duration::from_millis(50));
                started_tx.send(()).unwrap();
                std::future::pending::<()>().await;
            })
            .unwrap();
        started_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("slow task did not start");

        let started = Instant::now();
        owner
            .shutdown_with_timeout(Duration::from_millis(500))
            .unwrap();
        let elapsed = started.elapsed();
        assert!(elapsed >= Duration::from_millis(40), "elapsed={elapsed:?}");
        assert!(elapsed < Duration::from_millis(500), "elapsed={elapsed:?}");
    }

    #[test]
    fn stuck_owner_shutdown_respects_deadline_subprocess() {
        test_support::run(
            "runtime::tests::stuck_owner_shutdown_respects_deadline_child",
            Duration::from_secs(5),
        );
    }

    #[test]
    fn stuck_owner_shutdown_respects_deadline_child() {
        const TEST_NAME: &str = "runtime::tests::stuck_owner_shutdown_respects_deadline_child";
        if !test_support::is_child(TEST_NAME) {
            return;
        }

        let owner = RuntimeOwner::start_with_worker_threads(1).unwrap();
        let (started_tx, started_rx) = sync_channel(1);
        let _task = owner
            .spawn(async move {
                started_tx.send(()).unwrap();
                loop {
                    std::hint::spin_loop();
                }
            })
            .unwrap();
        started_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("stuck task did not start");

        let started = Instant::now();
        let error = owner
            .shutdown_with_timeout(Duration::from_millis(50))
            .unwrap_err();
        assert!(error.to_string().contains("shutdown timed out"));
        assert!(started.elapsed() < Duration::from_secs(1));
        test_support::finish_child();
    }

    #[tokio::test]
    async fn pending_operation_stops_after_cancellation() {
        let cancellation = CancellationToken::new();
        let wait_cancellation = cancellation.clone();
        let (polled_tx, polled_rx) = oneshot::channel();
        let waiter = tokio::spawn(async move {
            let mut polled_tx = Some(polled_tx);
            await_or_cancel(
                &wait_cancellation,
                std::future::poll_fn(move |_context| {
                    if let Some(sender) = polled_tx.take() {
                        let _ = sender.send(());
                    }
                    Poll::<()>::Pending
                }),
            )
            .await
        });

        polled_rx.await.expect("operation future was not polled");
        cancellation.cancel();
        let result = tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("operation did not stop after cancellation")
            .expect("operation task failed");
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn cancellation_wins_over_a_ready_send() {
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);

        assert!(!send_or_cancel(&cancellation, &tx, 1).await);
        assert!(matches!(
            rx.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn cancelling_bounded_runner_quiesces_remaining_tasks() {
        let (started_tx, started_rx) = oneshot::channel();
        let (dropped_tx, dropped_rx) = oneshot::channel();
        let started_tx = Arc::new(std::sync::Mutex::new(Some(started_tx)));
        let dropped_tx = Arc::new(std::sync::Mutex::new(Some(dropped_tx)));
        let cancellation = CancellationToken::new();
        let runner_cancellation = cancellation.clone();
        let outer = tokio::spawn(run_bounded_partitions(
            [()],
            1,
            runner_cancellation,
            move |_, _| {
                let started_tx = started_tx.clone();
                let dropped_tx = dropped_tx.clone();
                async move {
                    let _drop_signal = DropSignal(dropped_tx.lock().unwrap().take());
                    if let Some(sender) = started_tx.lock().unwrap().take() {
                        let _ = sender.send(());
                    }
                    std::future::pending::<Result<(), SpannerError>>().await
                }
            },
        ));
        started_rx.await.expect("partition task did not start");
        cancellation.cancel();
        outer.await.unwrap().unwrap();
        tokio::time::timeout(Duration::from_secs(1), dropped_rx)
            .await
            .expect("timed out waiting for the cancelled partition task to drain")
            .expect("partition task drop signal was not delivered");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn bounded_partition_runner_limits_live_tasks() {
        let active = Arc::new(AtomicUsize::new(0));
        let max_observed = Arc::new(AtomicUsize::new(0));
        let completed = Arc::new(AtomicUsize::new(0));

        run_bounded_partitions(0..20, 2, CancellationToken::new(), {
            let active = active.clone();
            let max_observed = max_observed.clone();
            let completed = completed.clone();
            move |_, _| {
                let active = active.clone();
                let max_observed = max_observed.clone();
                let completed = completed.clone();
                async move {
                    let current = active.fetch_add(1, Ordering::SeqCst) + 1;
                    max_observed.fetch_max(current, Ordering::SeqCst);
                    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                    active.fetch_sub(1, Ordering::SeqCst);
                    completed.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            }
        })
        .await
        .unwrap();

        assert_eq!(completed.load(Ordering::SeqCst), 20);
        assert!(max_observed.load(Ordering::SeqCst) <= 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn bounded_partition_runner_stops_scheduling_and_drains_on_error() {
        let started = Arc::new(AtomicUsize::new(0));
        let (dropped_tx, dropped_rx) = oneshot::channel();
        let dropped_tx = Arc::new(std::sync::Mutex::new(Some(dropped_tx)));
        let barrier = Arc::new(tokio::sync::Barrier::new(2));

        let result = run_bounded_partitions(0..100, 2, CancellationToken::new(), {
            let started = started.clone();
            let dropped_tx = dropped_tx.clone();
            let barrier = barrier.clone();
            move |partition, _| {
                let started = started.clone();
                let dropped_tx = dropped_tx.clone();
                let barrier = barrier.clone();
                async move {
                    started.fetch_add(1, Ordering::SeqCst);
                    let _drop_signal = if partition == 0 {
                        None
                    } else {
                        Some(DropSignal(dropped_tx.lock().unwrap().take()))
                    };
                    barrier.wait().await;
                    if partition == 0 {
                        return Err(SpannerError::Other("first failure".to_string()));
                    }
                    std::future::pending::<Result<(), SpannerError>>().await
                }
            }
        })
        .await;

        assert!(result.unwrap_err().to_string().contains("first failure"));
        assert!(started.load(Ordering::SeqCst) <= 2);
        tokio::time::timeout(std::time::Duration::from_secs(1), dropped_rx)
            .await
            .expect("in-flight partition was not drained")
            .expect("partition drop signal was not delivered");
    }
}
