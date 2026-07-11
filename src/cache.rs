use std::collections::HashMap;
use std::hash::Hash;

/// A tiny hand-rolled LRU map. Capacity is small, so an O(n) scan for the
/// least-recently-used entry on eviction avoids an extra dependency. Recency is
/// tracked with a monotonic counter.
pub(crate) struct LruCache<K, V> {
    capacity: usize,
    tick: u64,
    map: HashMap<K, (V, u64)>,
}

impl<K: Eq + Hash + Clone, V: Clone> LruCache<K, V> {
    pub(crate) fn new(capacity: usize) -> Self {
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

    /// Return the value for `key`, marking it most-recently-used.
    pub(crate) fn get(&mut self, key: &K) -> Option<V> {
        let tick = self.next_tick();
        let entry = self.map.get_mut(key)?;
        entry.1 = tick;
        Some(entry.0.clone())
    }

    /// Insert or replace `key`, returning the least-recently-used value when
    /// the insertion exceeds capacity.
    pub(crate) fn insert(&mut self, key: K, value: V) -> Option<V> {
        let tick = self.next_tick();
        self.map.insert(key.clone(), (value, tick));

        if self.map.len() <= self.capacity {
            return None;
        }

        let lru_key = self
            .map
            .iter()
            .filter(|(candidate, _)| *candidate != &key)
            .min_by_key(|(_, (_, entry_tick))| *entry_tick)
            .map(|(candidate, _)| candidate.clone());
        lru_key
            .and_then(|candidate| self.map.remove(&candidate))
            .map(|(evicted, _)| evicted)
    }

    /// Return the value for `key`, marking it most-recently-used, or insert one
    /// produced by `make` if absent. When inserting pushes the map over
    /// capacity, the least-recently-used entry is evicted and returned as the
    /// second tuple element so the caller can dispose of it.
    pub(crate) fn get_or_insert_with<F: FnOnce() -> V>(
        &mut self,
        key: K,
        make: F,
    ) -> (V, Option<V>) {
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
            // always some other, older entry -- never the one we just added.
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

#[cfg(test)]
mod tests {
    use super::LruCache;

    #[test]
    fn returns_existing_without_eviction() {
        let mut cache: LruCache<&str, i32> = LruCache::new(2);
        let (v, evicted) = cache.get_or_insert_with("a", || 1);
        assert_eq!(v, 1);
        assert!(evicted.is_none());

        let (v, evicted) = cache.get_or_insert_with("a", || panic!("should not build"));
        assert_eq!(v, 1);
        assert!(evicted.is_none());
    }

    #[test]
    fn evicts_least_recently_used() {
        let mut cache: LruCache<&str, i32> = LruCache::new(2);
        cache.get_or_insert_with("a", || 1);
        cache.get_or_insert_with("b", || 2);

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

        let (v, evicted) = cache.get_or_insert_with(2, || 20);
        assert_eq!(v, 20);
        assert_eq!(evicted, Some(10));
        assert!(cache.map.contains_key(&2));
        assert!(!cache.map.contains_key(&1));
    }

    #[test]
    fn get_and_insert_support_completed_value_caches() {
        let mut cache: LruCache<&str, i32> = LruCache::new(2);
        assert!(cache.insert("a", 1).is_none());
        assert!(cache.insert("b", 2).is_none());

        assert_eq!(cache.get(&"a"), Some(1));
        assert_eq!(cache.insert("c", 3), Some(2));
        assert_eq!(cache.get(&"a"), Some(1));
        assert_eq!(cache.get(&"b"), None);
        assert_eq!(cache.get(&"c"), Some(3));
    }
}
