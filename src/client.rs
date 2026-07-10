use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};

use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
use google_cloud_gax::options::RequestOptions;
use google_cloud_gax::retry_policy::{Aip194Strict, RetryPolicyExt};
use google_cloud_spanner::client::{DatabaseClient, Spanner};
use tokio::sync::OnceCell;

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

/// A tiny hand-rolled LRU map. Capacity is small (see [`CACHE_CAPACITY`]), so an
/// O(n) scan for the least-recently-used entry on eviction is cheap and avoids
/// pulling in an external crate. Recency is tracked with a monotonic counter.
///
/// This type is deliberately free of any Spanner/async concerns so the eviction
/// policy can be unit-tested on its own (see the tests module below).
struct LruCache<K, V> {
    capacity: usize,
    tick: u64,
    map: HashMap<K, (V, u64)>,
}

impl<K: Eq + Hash + Clone, V: Clone> LruCache<K, V> {
    fn new(capacity: usize) -> Self {
        assert!(capacity >= 1, "LRU capacity must be at least 1");
        Self {
            capacity,
            tick: 0,
            map: HashMap::new(),
        }
    }

    fn next_tick(&mut self) -> u64 {
        self.tick += 1;
        self.tick
    }

    /// Return the value for `key`, marking it most-recently-used, or insert one
    /// produced by `make` if absent. When inserting pushes the map over
    /// capacity, the least-recently-used entry is evicted and returned as the
    /// second tuple element so the caller can dispose of it.
    fn get_or_insert_with<F: FnOnce() -> V>(&mut self, key: K, make: F) -> (V, Option<V>) {
        // Compute the tick before the `get_mut` borrow so the hit path is a
        // single lookup (calling `next_tick(&mut self)` inside the `if let` would
        // conflict with the `&mut` borrow held by `entry`).
        let tick = self.next_tick();
        if let Some(entry) = self.map.get_mut(&key) {
            entry.1 = tick;
            return (entry.0.clone(), None);
        }

        let value = make();
        self.map.insert(key, (value.clone(), tick));

        let evicted = if self.map.len() > self.capacity {
            // The entry we just inserted has the newest tick, so the minimum is
            // always some other, older entry — never the one we just added.
            let lru_key = self
                .map
                .iter()
                .min_by_key(|(_, (_, t))| *t)
                .map(|(k, _)| k.clone());
            lru_key.and_then(|k| self.map.remove(&k)).map(|(v, _)| v)
        } else {
            None
        };

        (value, evicted)
    }

    /// Remove `key` only if it is currently mapped to a value satisfying `pred`.
    /// Returns whether an entry was removed. Used to drop a slot whose client
    /// creation failed without clobbering an entry a concurrent caller may have
    /// since replaced or successfully initialized.
    fn remove_if<F: FnOnce(&V) -> bool>(&mut self, key: &K, pred: F) -> bool {
        let matches = self.map.get(key).is_some_and(|(v, _)| pred(v));
        if matches {
            self.map.remove(key);
        }
        matches
    }
}

/// Each cache slot holds a shared [`OnceCell`] so that concurrent binds for the
/// same key create the underlying [`Client`] exactly once (single-flight),
/// instead of each racing bind building its own client and leaking sessions.
type ClientSlot = Arc<OnceCell<Arc<DatabaseClient>>>;

static CLIENT_CACHE: LazyLock<Mutex<LruCache<String, ClientSlot>>> =
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

/// Build the cache key used to identify a `(database, endpoint)` pair.
///
/// Shared by the client cache below and the dialect cache in `schema.rs` so a
/// database's detected dialect can be looked up under the same identity as
/// its cached client.
pub fn cache_key(database: &str, endpoint: Option<&str>) -> String {
    match endpoint {
        Some(ep) => format!("{database}@{ep}"),
        None => database.to_string(),
    }
}

/// Get or create a Spanner client.
///
/// - `endpoint: None` → default behavior (uses `SPANNER_EMULATOR_HOST` env var if set, otherwise real Spanner with auth)
/// - `endpoint: Some("host:port")` → connects to the given endpoint without auth (emulator mode)
///
/// Clients are cached by `(database, endpoint)` so both emulator and real connections can coexist.
/// The cache is bounded ([`CACHE_CAPACITY`]) with LRU eviction; evicted clients are closed in the
/// background once no in-flight work references them.
pub async fn get_or_create_client(
    database: &str,
    endpoint: Option<&str>,
) -> Result<Arc<DatabaseClient>, SpannerError> {
    let cache_key = cache_key(database, endpoint);

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

    // Single-flight: concurrent callers share this slot's OnceCell, so the client
    // is built at most once.
    match slot
        .get_or_try_init(|| create_client(database, endpoint))
        .await
    {
        Ok(client) => Ok(Arc::clone(client)),
        Err(e) => {
            // Creation failed: the cell stays empty. Drop the empty slot from the
            // cache so repeated failures against an unreachable target don't
            // occupy LRU capacity and evict healthy clients. Remove only if it is
            // still *this* slot and still empty — a concurrent caller may have
            // retried and successfully initialized it in the meantime.
            let mut cache = CLIENT_CACHE.lock().unwrap_or_else(|e| e.into_inner());
            cache.remove_if(&cache_key, |s| Arc::ptr_eq(s, &slot) && s.get().is_none());
            Err(e)
        }
    }
}

/// Build a brand-new Spanner client for the given target.
async fn create_client(
    database: &str,
    endpoint: Option<&str>,
) -> Result<Arc<DatabaseClient>, SpannerError> {
    match tokio::time::timeout(CLIENT_CONNECT_TIMEOUT, create_client_inner(database, endpoint))
        .await
    {
        Ok(result) => result,
        Err(_) => Err(SpannerError::Other(format!(
            "Timed out connecting to Spanner after {}s (database={database}, endpoint={})",
            CLIENT_CONNECT_TIMEOUT.as_secs(),
            endpoint.unwrap_or("<default>")
        ))),
    }
}

async fn create_client_inner(
    database: &str,
    endpoint: Option<&str>,
) -> Result<Arc<DatabaseClient>, SpannerError> {
    let mut builder = Spanner::builder();
    if let Some(ep) = endpoint {
        builder = builder
            .with_endpoint(endpoint_url(ep))
            .with_credentials(Anonymous::new().build());
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
        .database_client(database)
        .with_request_options(session_options)
        .build()
        .await?;
    Ok(Arc::new(client))
}

pub(crate) fn endpoint_url(endpoint: &str) -> String {
    if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        endpoint.to_string()
    } else {
        format!("http://{endpoint}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn returns_existing_without_eviction() {
        let mut cache: LruCache<&str, i32> = LruCache::new(2);
        let (v, evicted) = cache.get_or_insert_with("a", || 1);
        assert_eq!(v, 1);
        assert!(evicted.is_none());

        // Second lookup of the same key reuses the value and evicts nothing.
        let (v, evicted) = cache.get_or_insert_with("a", || panic!("should not build"));
        assert_eq!(v, 1);
        assert!(evicted.is_none());
    }

    #[test]
    fn evicts_least_recently_used() {
        let mut cache: LruCache<&str, i32> = LruCache::new(2);
        cache.get_or_insert_with("a", || 1);
        cache.get_or_insert_with("b", || 2);

        // Inserting a third key over capacity evicts the LRU ("a").
        let (_, evicted) = cache.get_or_insert_with("c", || 3);
        assert_eq!(evicted, Some(1));
        assert!(!cache.map.contains_key("a"));
        assert!(cache.map.contains_key("b"));
        assert!(cache.map.contains_key("c"));
    }

    #[test]
    fn access_refreshes_recency() {
        let mut cache: LruCache<&str, i32> = LruCache::new(2);
        cache.get_or_insert_with("a", || 1);
        cache.get_or_insert_with("b", || 2);

        // Touch "a" so "b" becomes the least-recently-used.
        cache.get_or_insert_with("a", || panic!("should not build"));

        let (_, evicted) = cache.get_or_insert_with("c", || 3);
        assert_eq!(evicted, Some(2));
        assert!(cache.map.contains_key("a"));
        assert!(!cache.map.contains_key("b"));
        assert!(cache.map.contains_key("c"));
    }

    #[test]
    fn never_evicts_the_just_inserted_entry() {
        let mut cache: LruCache<i32, i32> = LruCache::new(1);
        let (_, evicted) = cache.get_or_insert_with(1, || 10);
        assert!(evicted.is_none());
        // With capacity 1, inserting a new key evicts the previous one, never
        // the entry just added.
        let (v, evicted) = cache.get_or_insert_with(2, || 20);
        assert_eq!(v, 20);
        assert_eq!(evicted, Some(10));
        assert!(cache.map.contains_key(&2));
        assert!(!cache.map.contains_key(&1));
    }

    #[test]
    fn remove_if_respects_predicate() {
        let mut cache: LruCache<&str, i32> = LruCache::new(2);
        cache.get_or_insert_with("a", || 1);

        // Predicate false: entry stays.
        assert!(!cache.remove_if(&"a", |v| *v == 999));
        assert!(cache.map.contains_key("a"));

        // Missing key: nothing removed.
        assert!(!cache.remove_if(&"missing", |_| true));

        // Predicate true: entry removed.
        assert!(cache.remove_if(&"a", |v| *v == 1));
        assert!(!cache.map.contains_key("a"));
    }

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
