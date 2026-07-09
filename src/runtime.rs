use std::sync::OnceLock;
use tokio::runtime::Runtime;

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

/// Await every task in `join_set`, propagating the first task error (or join
/// failure). On any error the remaining tasks are aborted and then *drained to
/// completion* before returning, so no task is still running once this returns.
///
/// The quiescence guarantee is load-bearing for concurrent partition execution
/// (query.rs / scan.rs): client eviction close relies on `Arc` uniqueness =>
/// no in-flight users, so a partition task still holding a `Client` clone and
/// `ManagedSession` must not outlive its streamer. It also removes a duplicate-
/// row window — an aborted-but-not-yet-stopped partition can no longer emit a
/// straggler row after the caller's post-return `rows_delivered` fallback check.
/// Cancellations surfaced during the drain are ignored; the first real error
/// wins.
pub async fn join_partitions(
    mut join_set: tokio::task::JoinSet<Result<(), SpannerError>>,
) -> Result<(), SpannerError> {
    let mut result = Ok(());
    while let Some(joined) = join_set.join_next().await {
        match joined {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                result = Err(e);
                break;
            }
            Err(e) if e.is_cancelled() => {}
            Err(e) => {
                result = Err(SpannerError::Other(format!("Task join error: {e}")));
                break;
            }
        }
    }
    if result.is_err() {
        join_set.abort_all();
        // Drain remaining tasks (ignoring their now-irrelevant results, including
        // cancellations) so none are still running when we return.
        while join_set.join_next().await.is_some() {}
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::sync::Semaphore;

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

    // Mirrors the pattern used in query.rs/scan.rs: a semaphore sized to
    // `worker_threads()` bounds how many spawned tasks run concurrently,
    // regardless of how many tasks are spawned up front.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn semaphore_caps_concurrent_partition_tasks() {
        let permits = 2;
        let semaphore = Arc::new(Semaphore::new(permits));
        let concurrent = Arc::new(AtomicUsize::new(0));
        let max_observed = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::new();
        for _ in 0..10 {
            let semaphore = semaphore.clone();
            let concurrent = concurrent.clone();
            let max_observed = max_observed.clone();
            handles.push(tokio::spawn(async move {
                let _permit = semaphore.acquire().await.unwrap();
                let now = concurrent.fetch_add(1, Ordering::SeqCst) + 1;
                max_observed.fetch_max(now, Ordering::SeqCst);
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                concurrent.fetch_sub(1, Ordering::SeqCst);
            }));
        }
        for h in handles {
            h.await.unwrap();
        }

        assert!(
            max_observed.load(Ordering::SeqCst) <= permits,
            "observed more concurrent tasks than permits allow"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn join_partitions_propagates_first_error() {
        let mut join_set: tokio::task::JoinSet<Result<(), SpannerError>> =
            tokio::task::JoinSet::new();
        join_set.spawn(async { Ok(()) });
        join_set.spawn(async { Err(SpannerError::Other("boom".to_string())) });
        join_set.spawn(async { Ok(()) });

        let result = join_partitions(join_set).await;
        match result {
            Err(SpannerError::Other(msg)) => assert_eq!(msg, "boom"),
            other => panic!("expected the task error to propagate, got {other:?}"),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn join_partitions_all_ok_returns_ok() {
        let mut join_set: tokio::task::JoinSet<Result<(), SpannerError>> =
            tokio::task::JoinSet::new();
        for _ in 0..5 {
            join_set.spawn(async { Ok(()) });
        }
        assert!(join_partitions(join_set).await.is_ok());
    }

    // On error, `join_partitions` must abort and drain the remaining tasks before
    // returning, so a not-yet-completed task can never produce a side effect after
    // the function has returned. A straggler that sleeps then sends into a channel
    // must be cancelled mid-sleep; no message may arrive.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn join_partitions_drains_stragglers_before_returning() {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<()>();

        let mut join_set: tokio::task::JoinSet<Result<(), SpannerError>> =
            tokio::task::JoinSet::new();
        // Fails immediately.
        join_set.spawn(async { Err(SpannerError::Other("boom".to_string())) });
        // Stragglers: would send after a long sleep, but should be cancelled.
        for _ in 0..3 {
            let tx = tx.clone();
            join_set.spawn(async move {
                tokio::time::sleep(std::time::Duration::from_secs(30)).await;
                let _ = tx.send(());
                Ok(())
            });
        }
        drop(tx); // drop our own sender so the channel closes once tasks are gone

        let result = join_partitions(join_set).await;
        assert!(result.is_err(), "the immediate error must propagate");

        // After the drain, every straggler is terminated: the channel is closed
        // and empty, so no straggler send can ever arrive.
        assert!(
            matches!(
                rx.try_recv(),
                Err(tokio::sync::mpsc::error::TryRecvError::Disconnected)
            ),
            "a straggler delivered a message after join_partitions returned"
        );
    }
}
