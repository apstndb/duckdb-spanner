use std::sync::OnceLock;
use tokio::runtime::Runtime;

use crate::error::SpannerError;

static TOKIO_RUNTIME: OnceLock<Runtime> = OnceLock::new();

fn get_or_init_runtime() -> Result<&'static Runtime, SpannerError> {
    if let Some(rt) = TOKIO_RUNTIME.get() {
        return Ok(rt);
    }

    let rt = {
        let mut builder = tokio::runtime::Builder::new_multi_thread();
        // Cap at 4 threads — we don't need many since this is mostly I/O (gRPC)
        let threads = std::thread::available_parallelism()
            .map(|n| n.get().min(4))
            .unwrap_or(2);
        builder.worker_threads(threads);
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
