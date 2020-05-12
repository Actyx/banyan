use futures::future::BoxFuture;
use futures::future::Shared;
use futures::{future, Future, FutureExt};
use lru_cache::LruCache;
use parking_lot::{Mutex, MutexGuard};
use std::fmt::Debug;
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::Arc;
use tracing::warn;

type CacheFuture<T> = BoxFuture<'static, T>;

#[derive(Debug, Clone)]
pub enum Weight {
    Weighted(usize),
    NoCache,
}

pub trait Weigher<T> {
    fn weigh(element: &T) -> Weight;
}

#[derive(Debug)]
pub struct WeightedLruCache<T, K, W>
where
    // .shared() enables Clone on futures and forces Clone + Sync on both T and E
    // inspect_err forces Sync on K
    T: Clone,
    K: Clone + Eq + Hash,
    W: Weigher<T>,
{
    cache: Arc<Mutex<LruCacheWithWeight<T, K>>>,
    _phantom_weigher: PhantomData<W>,
}

impl<T, K, W> Clone for WeightedLruCache<T, K, W>
where
    T: Clone,
    K: Clone + Eq + Hash,
    W: Weigher<T>,
{
    fn clone(&self) -> Self {
        WeightedLruCache {
            cache: self.cache.clone(),
            _phantom_weigher: PhantomData,
        }
    }
}

impl<T, K, W> WeightedLruCache<T, K, W>
where
    T: Clone + Sync + Send + 'static,
    K: Clone + Eq + Hash + Sync + Send + Debug + 'static,
    W: Weigher<T> + 'static,
{
    pub fn new(size: usize) -> Self {
        // We use the size for both the weight and the elements
        WeightedLruCache::with_weight_and_elements(size, size)
    }

    pub fn with_weight_and_elements(max_weight: usize, max_elements: usize) -> Self {
        assert!(
            max_weight > 0 && max_elements > 0,
            "Max weight ({}), and max elements ({}), must be greater than 0",
            max_weight,
            max_elements
        );
        let cache = Arc::new(Mutex::new(LruCacheWithWeight::new(
            max_weight,
            max_elements,
        )));

        WeightedLruCache {
            cache,
            _phantom_weigher: PhantomData,
        }
    }

    /// Tries to locate the value in the cache and return it, or if not present, resolves the
    /// value (and stores it back into the cache).
    pub fn fetch_or_resolve<F>(&self, key: &K, resolve: impl Fn(&K) -> F) -> impl Future<Output = T>
    where
        F: Future<Output = T> + Send + 'static,
    {
        let cache = &self.cache;
        // The call to `lock()` returns a guard that holds the lock until the guard goes out of scope,
        // therefore we need to explicitly unlock in some cases, as the function being called tries
        // to take the lock, which would then deadlock (parking_lot mutex is not reentrant).
        let mut locked_cache = cache.lock();
        if let Some(value) = locked_cache.get(key) {
            // cache hit from the value cache, no work to be done
            future::ready(value).left_future()
        } else {
            let from_future = if let Some(result) = locked_cache.get_future(key) {
                // cache hit from the future cache, we are already doing the work
                result.clone().left_future()
            } else {
                MutexGuard::unlock_fair(locked_cache);
                // cache miss from both caches, we have to actually start doing the work
                self.resolve_and_store_internal(key, resolve).right_future()
            };
            from_future.right_future()
        }
    }

    /// Directly resolves the value bypassing the cache. It will still store back the value to
    /// the cache.
    pub fn force_resolve<F>(&self, key: &K, resolve: impl Fn(&K) -> F) -> impl Future<Output = T>
    where
        F: Future<Output = T> + Send + 'static,
    {
        self.resolve_and_store_internal(key, resolve)
    }

    fn resolve_and_store_internal<F>(
        &self,
        key: &K,
        resolve: impl Fn(&K) -> F,
    ) -> impl Future<Output = T>
    where
        F: Future<Output = T> + Send + 'static,
    {
        let cache = self.cache.clone();
        let cache2 = self.cache.clone();
        let key = key.clone();
        let key2 = key.clone();
        let resolve_and_store = resolve(&key).inspect(move |value| {
            // the cache stays in scope for the entire block
            let mut cache = cache.lock();
            // remove the future in any case
            if cache.fut_cache.remove(&key).is_some() {
                // only add the value for the first inspect
                let value = value.clone();
                // store the value only when the weigher tells us to do so!
                if let Weight::Weighted(weight) = W::weigh(&value) {
                    cache.put(key, value, weight);
                }
            }
        });
        let ras: CacheFuture<T> = resolve_and_store.boxed();
        let resolved = ras.shared();
        let resolved2 = resolved.clone();
        (*cache2.lock()).put_future(key2, resolved2);
        // Offload the future in order to guarantee the result being inserted into the cache,
        // even if the actual downstream requester stops polling.
        tokio::spawn(resolved).map(|result| result.expect("offloaded Future has panicked"))
    }
}

#[derive(Debug)]
pub struct LruCacheWithWeight<T, K>
where
    K: Eq + Hash,
{
    max_weight: usize,
    current_weight: usize,
    max_elements: usize,
    current_elements: usize,
    cache: LruCache<K, CacheEntryWithWeight<T>>,
    fut_cache: LruCache<K, Shared<CacheFuture<T>>>,
}

impl<T, K> LruCacheWithWeight<T, K>
where
    T: Clone,
    K: Clone + Eq + Hash + Debug,
{
    pub fn new(max_weight: usize, max_elements: usize) -> Self {
        LruCacheWithWeight {
            max_weight,
            current_weight: 0,
            max_elements,
            current_elements: 0,
            // Always have one extra element in the LRU cache to make insertion logic simpler
            cache: LruCache::new(max_elements + 1),
            fut_cache: LruCache::new(max_elements + 1),
        }
    }

    pub fn get(&mut self, key: &K) -> Option<T> {
        self.cache.get_mut(key).map(|x| x.value.clone())
    }

    pub fn put(&mut self, key: K, value: T, weight: usize) {
        self.fut_cache.remove(&key);
        if weight > self.max_weight {
            warn!(
                "Trying to store element for key '{:?}' with bigger weight {} than the max weight {}",
                key, weight, self.max_weight
            );
            return;
        }

        // Since we always have one extra element in the underlying LRU cache, we can insert the
        // element and add the weight first, and do the checks after
        self.current_weight += weight;
        self.current_elements += 1;
        let entry = CacheEntryWithWeight { weight, value };
        if let Some(CacheEntryWithWeight { weight, .. }) = self.cache.insert(key, entry) {
            // We replaced an entry, so subtract that weight
            self.current_weight -= weight;
            self.current_elements -= 1;
        }

        // If we went over max elements capacity, we need to remove the LRU element
        if self.current_elements > self.max_elements {
            if let Some((_key, entry)) = self.cache.remove_lru() {
                self.current_weight -= entry.weight;
                self.current_elements -= 1;
            }
        }

        // If we've gone over the limit, then remove as many elements as needed in LRU order
        if self.current_weight > self.max_weight {
            while let Some((_key, entry)) = self.cache.remove_lru() {
                self.current_weight -= entry.weight;
                self.current_elements -= 1;
                if self.current_weight <= self.max_weight {
                    break;
                }
            }
        }
    }

    pub fn contains_key(&mut self, key: &K) -> bool {
        self.cache.contains_key(key)
    }

    pub fn remove(&mut self, key: &K) -> Option<T> {
        self.cache.remove(key).map(|w| w.value)
    }

    pub fn iter(&mut self) -> Iter<K, T> {
        Iter(self.cache.iter())
    }

    fn get_future(&mut self, key: &K) -> Option<Shared<CacheFuture<T>>> {
        self.fut_cache.get_mut(key).cloned()
    }

    fn put_future(&mut self, key: K, fut: Shared<CacheFuture<T>>) {
        self.fut_cache.insert(key, fut);
    }
}

pub struct Iter<'a, K, T>(lru_cache::Iter<'a, K, CacheEntryWithWeight<T>>);

impl<'a, K, V> Iterator for Iter<'a, K, V> {
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<(&'a K, &'a V)> {
        let next = self.0.next();
        if let Some((k, cv)) = next {
            Some((k, &cv.value))
        } else {
            None
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}

#[derive(Debug)]
struct CacheEntryWithWeight<T> {
    weight: usize,
    value: T,
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::FutureExt;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    fn create_lru_cache_with_weight_with_entries(
        max_weight: usize,
        max_elements: usize,
        entries: i32,
        weight: usize,
    ) -> Box<LruCacheWithWeight<i32, i32>> {
        let mut cache = LruCacheWithWeight::new(max_weight, max_elements);
        for i in 1..=entries {
            cache.put(i, i, weight)
        }
        Box::new(cache)
    }

    fn create_wlru_cache() -> WeightedLruCache<i32, i32, TestIntWeigher> {
        WeightedLruCache::new(100)
    }

    #[test]
    fn lcww_oldest_is_dropped_at_max_elements() {
        let mut cache = create_lru_cache_with_weight_with_entries(20, 10, 10, 1);
        for i in 1..=10 {
            assert_eq!(Some(i), cache.get(&i))
        }
        cache.put(11, 11, 1);
        assert_eq!(10, cache.current_weight);
        assert_eq!(10, cache.current_elements);
        assert_eq!(None, cache.get(&1));
        for i in 2..=11 {
            assert_eq!(Some(i), cache.get(&i))
        }
    }

    #[test]
    fn lcww_oldest_is_not_dropped_at_max_elements_when_replacing() {
        let mut cache = create_lru_cache_with_weight_with_entries(20, 10, 10, 1);
        for i in 1..=10 {
            assert_eq!(Some(i), cache.get(&i))
        }
        cache.put(2, 22, 1);
        assert_eq!(10, cache.current_weight);
        assert_eq!(10, cache.current_elements);
        assert_eq!(Some(1), cache.get(&1));
        assert_eq!(Some(22), cache.get(&2));
        for i in 3..=10 {
            assert_eq!(Some(i), cache.get(&i))
        }
    }

    #[test]
    fn lcww_oldest_is_dropped_at_max_weight() {
        let mut cache = create_lru_cache_with_weight_with_entries(10, 20, 10, 1);
        for i in 1..=10 {
            assert_eq!(Some(i), cache.get(&i))
        }
        cache.put(11, 11, 1);
        assert_eq!(10, cache.current_weight);
        assert_eq!(10, cache.current_elements);
        assert_eq!(None, cache.get(&1));
        for i in 2..=11 {
            assert_eq!(Some(i), cache.get(&i))
        }
    }

    #[test]
    fn lcww_oldest_is_dropped_until_element_fits() {
        let mut cache = create_lru_cache_with_weight_with_entries(10, 20, 10, 1);
        for i in 1..=10 {
            assert_eq!(Some(i), cache.get(&i))
        }
        cache.put(11, 11, 6);
        assert_eq!(10, cache.current_weight);
        assert_eq!(5, cache.current_elements);
        for i in 1..=6 {
            assert_eq!(None, cache.get(&i));
        }
        for i in 7..=11 {
            assert_eq!(Some(i), cache.get(&i))
        }
    }

    #[test]
    fn lcww_oldest_is_not_dropped_at_max_weight_when_replacing_element() {
        let mut cache = create_lru_cache_with_weight_with_entries(10, 20, 10, 1);
        for i in 1..=10 {
            assert_eq!(Some(i), cache.get(&i))
        }
        cache.put(2, 22, 1);
        assert_eq!(10, cache.current_weight);
        assert_eq!(10, cache.current_elements);
        assert_eq!(Some(1), cache.get(&1));
        assert_eq!(Some(22), cache.get(&2));
        for i in 3..=10 {
            assert_eq!(Some(i), cache.get(&i))
        }
    }

    #[test]
    fn lcww_nothing_is_dropped_when_trying_to_add_element_with_weight_over_max_weight() {
        let mut cache = create_lru_cache_with_weight_with_entries(10, 10, 10, 1);
        for i in 1..=10 {
            assert_eq!(Some(i), cache.get(&i))
        }
        cache.put(2, 22, 11);
        assert_eq!(10, cache.current_weight);
        assert_eq!(10, cache.current_elements);
        for i in 1..=10 {
            assert_eq!(Some(i), cache.get(&i))
        }
    }

    struct TestIntWeigher;

    impl Weigher<i32> for TestIntWeigher {
        fn weigh(element: &i32) -> Weight {
            Weight::Weighted(element.abs() as usize)
        }
    }

    struct TestResultWeigher;

    impl Weigher<Result<i32, &str>> for TestResultWeigher {
        fn weigh(element: &Result<i32, &str>) -> Weight {
            match element {
                Ok(value) => Weight::Weighted(value.abs() as usize),
                Err(_) => Weight::NoCache,
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn check_resolve_and_cache<F, R, W, RF>(
        key: i32,
        force: bool,
        expected: R,
        cache_misses: usize,
        cache: &WeightedLruCache<R, i32, W>,
        cache_miss_ctr: &Arc<AtomicUsize>,
        resolve: RF,
    ) where
        F: Future<Output = R> + Send + Unpin + 'static,
        R: Eq + Hash + Clone + Send + Sync + Debug + 'static,
        W: Weigher<R> + 'static,
        RF: Fn(&i32) -> F + 'static,
    {
        let result = if force {
            cache
                .force_resolve(&key, resolve)
                .unit_error()
                .await
                .unwrap()
        } else {
            cache
                .fetch_or_resolve(&key, resolve)
                .unit_error()
                .await
                .unwrap()
        };
        assert_eq!(expected, result);
        assert_eq!(cache_misses, cache_miss_ctr.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn wlc_second_non_concurrent_read_comes_from_cache() {
        let cache_miss_ctr = Arc::new(AtomicUsize::new(0));
        let ctr_clone = cache_miss_ctr.clone();
        let resolve = move |k: &i32| {
            let value = *k;
            ctr_clone.fetch_add(1, Ordering::SeqCst);
            future::ready(value)
        };
        let cache = create_wlru_cache();
        check_resolve_and_cache(42, false, 42, 1, &cache, &cache_miss_ctr, resolve.clone()).await;
        check_resolve_and_cache(42, false, 42, 1, &cache, &cache_miss_ctr, resolve.clone()).await;
    }

    #[tokio::test]
    async fn wlc_force_resolve_always_stores_in_cache() {
        let cache_miss_ctr = Arc::new(AtomicUsize::new(0));
        let ctr_clone = cache_miss_ctr.clone();
        let resolve = move |k: &i32| {
            let value = *k + ctr_clone.fetch_add(1, Ordering::SeqCst) as i32;
            future::ready(value)
        };
        let cache = create_wlru_cache();
        check_resolve_and_cache(42, false, 42, 1, &cache, &cache_miss_ctr, resolve.clone()).await;

        // The value is cached now, but we bypass the cache
        check_resolve_and_cache(42, true, 43, 2, &cache, &cache_miss_ctr, resolve.clone()).await;
        check_resolve_and_cache(42, true, 44, 3, &cache, &cache_miss_ctr, resolve.clone()).await;

        // Now use the cache again
        check_resolve_and_cache(42, false, 44, 3, &cache, &cache_miss_ctr, resolve.clone()).await;
    }

    #[tokio::test]
    async fn wlc_no_caching_if_resolve_fails() {
        let cache_miss_ctr = Arc::new(AtomicUsize::new(0));
        let should_fail = Arc::new(AtomicBool::new(true));
        let ctr_clone = cache_miss_ctr.clone();
        let should_fail_clone = should_fail.clone();

        let resolve = move |value: &i32| {
            let ctr = ctr_clone.fetch_add(1, Ordering::SeqCst) as i32;
            if should_fail_clone.load(Ordering::SeqCst) {
                future::err::<i32, &str>("foo")
            } else {
                future::ok(*value + ctr)
            }
        };

        let cache: WeightedLruCache<Result<i32, &str>, i32, TestResultWeigher> =
            WeightedLruCache::new(100);

        // Keep failing a few times
        check_resolve_and_cache(
            42,
            false,
            Err("foo"),
            1,
            &cache,
            &cache_miss_ctr,
            resolve.clone(),
        )
        .await;
        check_resolve_and_cache(
            42,
            false,
            Err("foo"),
            2,
            &cache,
            &cache_miss_ctr,
            resolve.clone(),
        )
        .await;
        check_resolve_and_cache(
            42,
            false,
            Err("foo"),
            3,
            &cache,
            &cache_miss_ctr,
            resolve.clone(),
        )
        .await;

        // Stop failing
        should_fail.store(false, Ordering::SeqCst);

        // Now we will resolve and cache correctly
        check_resolve_and_cache(
            42,
            false,
            Ok(45),
            4,
            &cache,
            &cache_miss_ctr,
            resolve.clone(),
        )
        .await;
        check_resolve_and_cache(
            42,
            false,
            Ok(45),
            4,
            &cache,
            &cache_miss_ctr,
            resolve.clone(),
        )
        .await;

        // Start failing again
        should_fail.store(true, Ordering::SeqCst);

        // Cache protects us from failure
        check_resolve_and_cache(
            42,
            false,
            Ok(45),
            4,
            &cache,
            &cache_miss_ctr,
            resolve.clone(),
        )
        .await;

        // But forcing the resolve returns an error
        check_resolve_and_cache(
            42,
            true,
            Err("foo"),
            5,
            &cache,
            &cache_miss_ctr,
            resolve.clone(),
        )
        .await;
    }

    #[tokio::test]
    async fn wlc_drops_elements_based_on_weight() {
        let cache_miss_ctr = Arc::new(AtomicUsize::new(0));
        let ctr_clone = cache_miss_ctr.clone();
        let resolve = move |k: &i32| {
            let value = *k;
            ctr_clone.fetch_add(1, Ordering::SeqCst);
            future::ready(value)
        };
        let cache = create_wlru_cache();

        // First element fits in the cache and is cached
        check_resolve_and_cache(47, false, 47, 1, &cache, &cache_miss_ctr, resolve.clone()).await;

        // Second element fits in the cache
        check_resolve_and_cache(11, false, 11, 2, &cache, &cache_miss_ctr, resolve.clone()).await;

        // Third element fits in the cache
        check_resolve_and_cache(17, false, 17, 3, &cache, &cache_miss_ctr, resolve.clone()).await;

        // Check that all elements are still cached
        check_resolve_and_cache(11, false, 11, 3, &cache, &cache_miss_ctr, resolve.clone()).await;
        check_resolve_and_cache(47, false, 47, 3, &cache, &cache_miss_ctr, resolve.clone()).await;
        check_resolve_and_cache(17, false, 17, 3, &cache, &cache_miss_ctr, resolve.clone()).await;

        // Fourth element does not fit in the cache and should evict the LRU, which is 11
        check_resolve_and_cache(29, false, 29, 4, &cache, &cache_miss_ctr, resolve.clone()).await;

        // Check that all the other elements are in the cache
        check_resolve_and_cache(47, false, 47, 4, &cache, &cache_miss_ctr, resolve.clone()).await;
        check_resolve_and_cache(17, false, 17, 4, &cache, &cache_miss_ctr, resolve.clone()).await;
        check_resolve_and_cache(29, false, 29, 4, &cache, &cache_miss_ctr, resolve.clone()).await;

        // Check that 11 was evicted and will trigger a resolve
        check_resolve_and_cache(11, false, 11, 5, &cache, &cache_miss_ctr, resolve.clone()).await;
    }
}
