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
}
