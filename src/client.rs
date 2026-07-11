use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};

use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
use google_cloud_gax::options::RequestOptions;
use google_cloud_gax::retry_policy::{Aip194Strict, RetryPolicyExt};
use google_cloud_spanner::client::{DatabaseClient, Spanner};
use tokio::sync::OnceCell;

use crate::cache::LruCache;
use crate::connection::{AuthenticationMode, ConnectionIdentity, ConnectionProfile};
use crate::error::SpannerError;
use crate::runtime;

/// Maximum number of Spanner clients kept alive at once. Connecting to more
/// distinct `(database, endpoint)` targets than this evicts the least-recently
/// used client. A small fixed bound is fine: most processes talk to a handful
/// of databases, and each client owns session maintenance state worth reclaiming.
const CACHE_CAPACITY: usize = 8;

/// How long an eviction task waits, per reclaim stage, for in-flight work to
/// drop its references so the client can be reclaimed by consuming sole ownership.
/// The official google-cloud-rust `DatabaseClient` has no public close method;
/// dropping the final clone lets the weakly-held maintenance task exit.
const EVICT_CLOSE_TIMEOUT: Duration = Duration::from_secs(60);

/// Poll interval used while waiting for an `Arc` to become uniquely owned.
const RECLAIM_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// Upper bound for building a Spanner client (channel + multiplexed session).
/// Without this, unreachable endpoints retry gRPC indefinitely and hang CI
/// (e.g. `test_copy_to_registration` against a closed local port).
const CLIENT_CONNECT_TIMEOUT: Duration = Duration::from_secs(15);

/// Per-attempt RPC timeout while creating the multiplexed session.
const SESSION_CREATE_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(5);

/// Each cache slot holds a shared [`OnceCell`] so that concurrent binds for the
/// same key create the underlying [`Client`] exactly once (single-flight),
/// instead of each racing bind building its own client and leaking sessions.
type ClientSlot = Arc<OnceCell<Arc<DatabaseClient>>>;

static CLIENT_CACHE: LazyLock<Mutex<LruCache<ConnectionIdentity, ClientSlot>>> =
    LazyLock::new(|| Mutex::new(LruCache::new(CACHE_CAPACITY)));

/// Outcome of waiting for an `Arc` to become uniquely owned.
enum Reclaim<T> {
    /// The wait succeeded: we now own the value outright.
    Sole(T),
    /// The grace period elapsed while other holders still exist.
    StillShared,
}

/// Wait (bounded by `grace`) for `arc` to become uniquely owned. Returns
/// [`Reclaim::Sole`] once every other holder has dropped its reference, or
/// [`Reclaim::StillShared`] if the grace period elapses first.
async fn take_when_unique<T>(mut arc: Arc<T>, grace: Duration) -> Reclaim<T> {
    let start = Instant::now();
    loop {
        match Arc::try_unwrap(arc) {
            Ok(value) => return Reclaim::Sole(value),
            Err(shared) => {
                if start.elapsed() >= grace {
                    return Reclaim::StillShared;
                }
                arc = shared;
                tokio::time::sleep(RECLAIM_POLL_INTERVAL).await;
            }
        }
    }
}

/// Drop an evicted client once nothing is using it anymore.
///
/// An evicted slot may still be referenced by a bind mid-`get_or_try_init`
/// (holding an `Arc<OnceCell>` clone) or by in-flight queries (holding
/// `Arc<DatabaseClient>` clones), so we cannot reclaim immediately. We wait a
/// bounded grace period for those to drain; if they overrun, dropping this
/// eviction task's clone is still the only public lifecycle action available in
/// the official client. Runs as a detached background task.
async fn close_evicted(slot: ClientSlot, grace: Duration) {
    match take_when_unique(slot, grace).await {
        Reclaim::Sole(cell) => {
            let _ = cell.into_inner();
        }
        Reclaim::StillShared => {}
    };
}

/// Get or create a Spanner client.
///
/// Clients are cached by the complete resolved identity so targets sharing an
/// endpoint cannot accidentally share credentials or transport semantics.
/// The cache is bounded ([`CACHE_CAPACITY`]) with LRU eviction; evicted clients are closed in the
/// background once no in-flight work references them.
pub async fn get_or_create_client(
    profile: &ConnectionProfile,
) -> Result<Arc<DatabaseClient>, SpannerError> {
    let cache_key = profile.identity();

    // Look up (or create) the slot for this key under the std mutex. The lock is
    // released before any `.await` below — bind runs on `block_on`, so holding a
    // std mutex across an await point could deadlock the runtime.
    let (slot, evicted) = {
        let mut cache = CLIENT_CACHE.lock().unwrap_or_else(|e| e.into_inner());
        cache.get_or_insert_with(cache_key.clone(), || Arc::new(OnceCell::new()))
    };

    // Close any client the insertion evicted, off the hot path.
    if let Some(evicted) = evicted {
        let _ = runtime::spawn(close_evicted(evicted, EVICT_CLOSE_TIMEOUT));
    }

    // Keep a failed cell in the bounded LRU. Removing an empty cell here can
    // race a waiting caller that has already started retrying initialization,
    // creating two clients for one key. Tokio OnceCell retries failures on the
    // same slot, and normal LRU eviction bounds unreachable identities.
    let client = slot.get_or_try_init(|| create_client(profile)).await?;
    Ok(Arc::clone(client))
}

/// Build a brand-new Spanner client for the given target.
async fn create_client(profile: &ConnectionProfile) -> Result<Arc<DatabaseClient>, SpannerError> {
    match tokio::time::timeout(CLIENT_CONNECT_TIMEOUT, create_client_inner(profile)).await {
        Ok(result) => result,
        Err(_) => Err(SpannerError::Other(format!(
            "Timed out connecting to Spanner after {}s (database={}, endpoint={}, mode={:?})",
            CLIENT_CONNECT_TIMEOUT.as_secs(),
            profile.database_path(),
            profile.data_endpoint().unwrap_or("<default>"),
            profile.endpoint_mode(),
        ))),
    }
}

async fn create_client_inner(
    profile: &ConnectionProfile,
) -> Result<Arc<DatabaseClient>, SpannerError> {
    let mut builder = Spanner::builder();
    if let Some(endpoint) = profile.data_endpoint() {
        builder = builder.with_endpoint(endpoint);
    }
    if profile.authentication() == AuthenticationMode::Anonymous {
        builder = builder.with_credentials(Anonymous::new().build());
    }

    let spanner = builder.build().await?;

    // Bound session-create retries so connection refused / unreachable hosts
    // fail within CLIENT_CONNECT_TIMEOUT instead of retrying for minutes.
    let mut session_options = RequestOptions::default();
    session_options.set_attempt_timeout(SESSION_CREATE_ATTEMPT_TIMEOUT);
    session_options.set_retry_policy(
        Aip194Strict
            .with_time_limit(CLIENT_CONNECT_TIMEOUT)
            .with_attempt_limit(3),
    );

    let client = spanner
        .database_client(profile.database_path())
        .with_request_options(session_options)
        .build()
        .await?;
    Ok(Arc::new(client))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn take_when_unique_reclaims_sole_owner() {
        let arc = Arc::new(42);
        match take_when_unique(arc, Duration::from_secs(1)).await {
            Reclaim::Sole(v) => assert_eq!(v, 42),
            Reclaim::StillShared => panic!("should have reclaimed sole ownership"),
        }
    }

    #[tokio::test]
    async fn take_when_unique_hands_back_shared_after_grace() {
        let arc = Arc::new(42);
        let _keepalive = Arc::clone(&arc);
        // A second reference persists, so ownership can never be reclaimed.
        match take_when_unique(arc, Duration::from_millis(100)).await {
            Reclaim::StillShared => assert_eq!(*_keepalive, 42),
            Reclaim::Sole(_) => panic!("should not have reclaimed while shared"),
        }
    }
}
