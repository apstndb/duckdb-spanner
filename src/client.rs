use std::collections::HashMap;
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

/// Upper bound on how long an eviction task waits for in-flight work to finish
/// before giving up on a graceful close (per reclaim stage). If exceeded, the
/// client is simply dropped: its sessions are not gracefully closed but will
/// expire server-side, so this only trades tidiness for a bounded wait.
const EVICT_CLOSE_TIMEOUT: Duration = Duration::from_secs(60);

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
        if self.map.contains_key(&key) {
            let tick = self.next_tick();
            let entry = self.map.get_mut(&key).unwrap();
            entry.1 = tick;
            return (entry.0.clone(), None);
        }

        let tick = self.next_tick();
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
}

/// Each cache slot holds a shared [`OnceCell`] so that concurrent binds for the
/// same key create the underlying [`Client`] exactly once (single-flight),
/// instead of each racing bind building its own client and leaking sessions.
type ClientSlot = Arc<OnceCell<Arc<Client>>>;

static CLIENT_CACHE: LazyLock<Mutex<LruCache<String, ClientSlot>>> =
    LazyLock::new(|| Mutex::new(LruCache::new(CACHE_CAPACITY)));

/// Reclaim sole ownership of `arc`, waiting (bounded) for other holders to drop
/// their references. Returns `None` if the timeout elapses while still shared.
async fn take_when_unique<T>(mut arc: Arc<T>, timeout: Duration) -> Option<T> {
    let start = Instant::now();
    loop {
        match Arc::try_unwrap(arc) {
            Ok(value) => return Some(value),
            Err(shared) => {
                if start.elapsed() >= timeout {
                    return None;
                }
                arc = shared;
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
}

/// Gracefully close an evicted client once nothing is using it anymore.
///
/// `Client::close` takes `self`, so we must reclaim ownership of the `Client`
/// before closing. An evicted slot may still be referenced by in-flight queries
/// (which hold `Arc<Client>` clones) or by a bind that is mid-`get_or_try_init`
/// (which holds an `Arc<OnceCell>` clone), so we cannot close immediately.
/// Instead we wait — first for sole ownership of the slot, then of the client —
/// before closing. Runs as a detached background task.
async fn close_evicted(slot: ClientSlot) {
    // Stage 1: no bind is still awaiting creation on this slot.
    let Some(cell) = take_when_unique(slot, EVICT_CLOSE_TIMEOUT).await else {
        return;
    };
    // `None` means creation never completed (e.g. it errored) — nothing to close.
    let Some(client) = cell.into_inner() else {
        return;
    };
    // Stage 2: no query is still holding the client.
    let Some(client) = take_when_unique(client, EVICT_CLOSE_TIMEOUT).await else {
        return;
    };
    client.close().await;
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
        cache.get_or_insert_with(cache_key, || Arc::new(OnceCell::new()))
    };

    // Close any client the insertion evicted, off the hot path.
    if let Some(evicted) = evicted {
        let _ = runtime::spawn(close_evicted(evicted));
    }

    // Single-flight: concurrent callers share this slot's OnceCell, so the client
    // is built at most once. On creation error the cell stays empty and a later
    // call will retry.
    let client = slot
        .get_or_try_init(|| create_client(database, endpoint))
        .await?;
    Ok(Arc::clone(client))
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

    #[tokio::test]
    async fn take_when_unique_reclaims_sole_owner() {
        let arc = Arc::new(42);
        let taken = take_when_unique(arc, Duration::from_secs(1)).await;
        assert_eq!(taken, Some(42));
    }

    #[tokio::test]
    async fn take_when_unique_times_out_while_shared() {
        let arc = Arc::new(42);
        let _keepalive = Arc::clone(&arc);
        // A second reference persists, so ownership can never be reclaimed.
        let taken = take_when_unique(arc, Duration::from_millis(100)).await;
        assert!(taken.is_none());
    }
}
