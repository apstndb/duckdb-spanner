use std::collections::HashMap;
use std::future::Future;
use std::hash::Hash;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};

use google_cloud_gax::conn::Environment;
use google_cloud_spanner::client::{Client, ClientConfig};
use tokio::sync::OnceCell;

use crate::error::SpannerError;
use crate::runtime;

/// Maximum number of Spanner clients kept alive at once. Connecting to more
/// distinct `(database, endpoint)` targets than this evicts the least-recently
/// used client (and closes it). A small fixed bound is fine: most processes talk
/// to a handful of databases, and each client owns a session pool worth reclaiming.
const CACHE_CAPACITY: usize = 8;

/// How long an eviction task waits, per reclaim stage, for in-flight work to
/// drop its references so the client can be closed by consuming sole ownership.
/// This is a politeness grace period, not a deadline for closing: if it elapses
/// while the client is still shared, the eviction task force-closes through a
/// clone rather than leaking the client (see [`close_evicted`]).
const EVICT_CLOSE_TIMEOUT: Duration = Duration::from_secs(60);

/// Poll interval used while waiting for an `Arc` to become uniquely owned.
const RECLAIM_POLL_INTERVAL: Duration = Duration::from_millis(50);

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
type ClientSlot = Arc<OnceCell<Arc<Client>>>;

static CLIENT_CACHE: LazyLock<Mutex<LruCache<String, ClientSlot>>> =
    LazyLock::new(|| Mutex::new(LruCache::new(CACHE_CAPACITY)));

/// Outcome of waiting for an `Arc` to become uniquely owned.
enum Reclaim<T> {
    /// The wait succeeded: we now own the value outright.
    Sole(T),
    /// The grace period elapsed while other holders still exist; the `Arc` is
    /// handed back so the caller can still act on a clone of the value.
    StillShared(Arc<T>),
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
                    return Reclaim::StillShared(shared);
                }
                arc = shared;
                tokio::time::sleep(RECLAIM_POLL_INTERVAL).await;
            }
        }
    }
}

/// Run `close` on the value behind `arc` exactly once, after at most `grace`.
///
/// Prefers to consume sole ownership (a clean hand-off). If the value is still
/// shared after the grace period, it force-runs `close` on a *clone* rather than
/// leaking the value: for a Spanner `Client` this is safe and desirable because
/// `Client` is `Clone` and its `SessionManager` is shared behind an `Arc`, so
/// closing any clone cancels the shared cancellation token — deleting the pooled
/// sessions server-side and stopping the two detached background tasks
/// (health-check pinger + session creator) that the fork's `SessionManager`
/// otherwise stops only via `close()` (it has no `Drop`). Straggling holders of
/// an old clone will get session errors on their next use, which is acceptable
/// for an already-evicted client.
async fn close_when_reclaimed<T, F, Fut>(arc: Arc<T>, grace: Duration, close: F)
where
    T: Clone,
    F: FnOnce(T) -> Fut,
    Fut: Future<Output = ()>,
{
    match take_when_unique(arc, grace).await {
        Reclaim::Sole(value) => close(value).await,
        Reclaim::StillShared(shared) => close((*shared).clone()).await,
    }
}

/// Close an evicted client once nothing is using it anymore, or force it closed
/// after a grace period if in-flight work overruns.
///
/// An evicted slot may still be referenced by a bind mid-`get_or_try_init`
/// (holding an `Arc<OnceCell>` clone) or by in-flight queries (holding
/// `Arc<Client>` clones), so we cannot close immediately. We wait a bounded
/// grace period for those to drain, but do **not** give up if they overrun:
/// a partitioned scan can stream for minutes and a COPY holds the client for the
/// whole operation, and simply dropping the client would leak the fork's
/// `SessionManager` background tasks and keep its pooled sessions alive
/// server-side indefinitely. Instead [`close_when_reclaimed`] force-closes
/// through a clone. Runs as a detached background task.
async fn close_evicted(slot: ClientSlot, grace: Duration) {
    // Stage 1: reclaim the client out of the slot's OnceCell.
    let client = match take_when_unique(slot, grace).await {
        // Sole owner: take the client out of the cell (if creation completed).
        Reclaim::Sole(cell) => match cell.into_inner() {
            Some(client) => client,
            // Creation never completed (e.g. it errored) — nothing to close.
            None => return,
        },
        // Still shared past the grace period (a bind is stuck awaiting creation):
        // borrow a clone of the client so we can still force it closed.
        Reclaim::StillShared(slot) => match slot.get() {
            Some(client) => Arc::clone(client),
            // Not yet initialized — nothing to close.
            None => return,
        },
    };
    // Stage 2: close the client, force-closing through a clone if in-flight
    // queries still hold it past the grace period.
    close_when_reclaimed(client, grace, |c| c.close()).await;
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
) -> Result<Arc<Client>, SpannerError> {
    let cache_key = match endpoint {
        Some(ep) => format!("{database}@{ep}"),
        None => database.to_string(),
    };

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
) -> Result<Arc<Client>, SpannerError> {
    let config = match endpoint {
        Some(ep) => {
            // Explicit endpoint: emulator mode (no auth, plain HTTP)
            ClientConfig {
                environment: Environment::Emulator(ep.to_string()),
                ..Default::default()
            }
        }
        None => {
            // Default: respect SPANNER_EMULATOR_HOST env var, or use real auth
            if std::env::var("SPANNER_EMULATOR_HOST").is_ok() {
                ClientConfig::default()
            } else {
                ClientConfig::default()
                    .with_auth()
                    .await
                    .map_err(|e| SpannerError::Other(format!("Auth error: {e}")))?
            }
        }
    };

    let client = Client::new(database, config).await?;
    Ok(Arc::new(client))
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
            Reclaim::StillShared(_) => panic!("should have reclaimed sole ownership"),
        }
    }

    #[tokio::test]
    async fn take_when_unique_hands_back_shared_after_grace() {
        let arc = Arc::new(42);
        let _keepalive = Arc::clone(&arc);
        // A second reference persists, so ownership can never be reclaimed.
        match take_when_unique(arc, Duration::from_millis(100)).await {
            Reclaim::StillShared(shared) => assert_eq!(*shared, 42),
            Reclaim::Sole(_) => panic!("should not have reclaimed while shared"),
        }
    }

    /// A clonable stand-in for `Client`: `close` flips a shared flag, standing in
    /// for the real client's `close().await`.
    #[derive(Clone)]
    struct FakeClient {
        closed: Arc<std::sync::atomic::AtomicBool>,
    }

    impl FakeClient {
        async fn close(self) {
            self.closed.store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn close_when_reclaimed_closes_sole_owner() {
        let closed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let arc = Arc::new(FakeClient {
            closed: closed.clone(),
        });
        close_when_reclaimed(arc, Duration::from_secs(1), |c| c.close()).await;
        assert!(closed.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[tokio::test]
    async fn close_when_reclaimed_force_closes_shared_after_grace() {
        let closed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let arc = Arc::new(FakeClient {
            closed: closed.clone(),
        });
        // A straggler holds a clone that never drops, so the value stays shared
        // past the grace period. It must still be force-closed (through a clone),
        // not leaked.
        let _straggler = Arc::clone(&arc);
        close_when_reclaimed(arc, Duration::from_millis(100), |c| c.close()).await;
        assert!(
            closed.load(std::sync::atomic::Ordering::SeqCst),
            "shared client must be force-closed after the grace period"
        );
    }
}
