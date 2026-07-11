use std::sync::OnceLock;
use tokio::runtime::Runtime;
use tokio_util::sync::CancellationToken;

use crate::error::SpannerError;

static TOKIO_RUNTIME: OnceLock<Runtime> = OnceLock::new();

// Cap at 4 threads — we don't need many since this is mostly I/O (gRPC).
// Shared with query.rs/scan.rs so concurrent partition execution doesn't
// oversubscribe the runtime's worker threads.
const MAX_WORKER_THREADS: usize = 4;

// DuckDB's C extension API has no unload hook, so the Tokio runtime and cached Spanner
// clients are intentionally leaked for the process lifetime.

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

fn get_or_init_runtime() -> Result<&'static Runtime, SpannerError> {
    if let Some(rt) = TOKIO_RUNTIME.get() {
        return Ok(rt);
    }

    let rt = {
        let mut builder = tokio::runtime::Builder::new_multi_thread();
        builder.worker_threads(worker_threads());
        builder.enable_all();
        builder
            .build()
            .map_err(|e| SpannerError::Other(format!("Failed to create Tokio runtime: {e}")))?
    };

    // Another thread may have initialized it between our get() and here;
    // OnceLock::set returns Err(value) if already set, which is fine.
    let _ = TOKIO_RUNTIME.set(rt);
    Ok(TOKIO_RUNTIME.get().unwrap())
}

pub fn block_on<F: std::future::Future>(future: F) -> Result<F::Output, SpannerError> {
    let rt = get_or_init_runtime()?;
    Ok(rt.block_on(future))
}

pub fn spawn<F>(future: F) -> Result<tokio::task::JoinHandle<F::Output>, SpannerError>
where
    F: std::future::Future + Send + 'static,
    F::Output: Send + 'static,
{
    let rt = get_or_init_runtime()?;
    Ok(rt.spawn(future))
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
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::task::Poll;
    use std::time::Duration;
    use tokio::sync::oneshot;

    struct DropSignal(Option<oneshot::Sender<()>>);

    impl Drop for DropSignal {
        fn drop(&mut self) {
            if let Some(sender) = self.0.take() {
                let _ = sender.send(());
            }
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
