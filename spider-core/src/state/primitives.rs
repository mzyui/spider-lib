//! # State Primitives Module
//!
//! Provides ready-to-use thread-safe primitives for building Spider state structures.
//!
//! ## Overview
//!
//! This module offers a collection of thread-safe types that can be used to build
//! custom Spider state without worrying about concurrency issues. All types are
//! designed for high-performance concurrent access with minimal locking overhead.
//!
//! ## Key Types
//!
//! - [`Counter`]: Thread-safe atomic counter
//! - [`Counter64`]: 64-bit thread-safe counter for large counts
//! - [`Flag`]: Thread-safe boolean flag
//! - [`VisitedUrls`]: Thread-safe URL tracking with DashMap
//! - [`ConcurrentMap<K, V>`]: Thread-safe key-value map
//! - [`ConcurrentVec<T>`]: Thread-safe dynamic vector
//! - [`StateAccessMetrics`]: Metrics for tracking state access patterns
//!
//! ## Example
//!
//! ```rust
//! use spider_core::state::{Counter, VisitedUrls};
//! use std::sync::Arc;
//!
//! #[derive(Clone, Default)]
//! struct MySpiderState {
//!     page_count: Counter,
//!     visited_urls: VisitedUrls,
//! }
//!
//! impl MySpiderState {
//!     fn increment_page_count(&self) {
//!         self.page_count.inc();
//!     }
//!
//!     fn mark_url_visited(&self, url: String) {
//!         self.visited_urls.mark(url);
//!     }
//! }
//! ```

use dashmap::DashMap;
use parking_lot::RwLock;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

// ============================================================================
// Counter - Thread-safe atomic counter
// ============================================================================

/// A thread-safe counter using atomic operations.
///
/// This is a wrapper around `AtomicUsize` that provides a more ergonomic API
/// for counting operations in concurrent environments.
///
/// ## Example
///
/// ```rust
/// use spider_core::state::Counter;
///
/// let counter = Counter::new();
/// counter.inc();
/// counter.add(5);
/// assert_eq!(counter.get(), 6);
/// ```
#[derive(Debug, Default)]
pub struct Counter(AtomicUsize);

impl Counter {
    /// Creates a new counter initialized to 0.
    pub fn new() -> Self {
        Self(AtomicUsize::new(0))
    }

    /// Creates a new counter with the specified initial value.
    pub fn with_value(value: usize) -> Self {
        Self(AtomicUsize::new(value))
    }

    /// Increments the counter by 1.
    pub fn inc(&self) {
        self.0.fetch_add(1, Ordering::AcqRel);
    }

    /// Decrements the counter by 1.
    pub fn dec(&self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }

    /// Adds a value to the counter.
    pub fn add(&self, value: usize) {
        self.0.fetch_add(value, Ordering::AcqRel);
    }

    /// Subtracts a value from the counter.
    pub fn sub(&self, value: usize) {
        self.0.fetch_sub(value, Ordering::AcqRel);
    }

    /// Gets the current value of the counter.
    pub fn get(&self) -> usize {
        self.0.load(Ordering::Acquire)
    }

    /// Sets the counter to a specific value.
    pub fn set(&self, value: usize) {
        self.0.store(value, Ordering::Release);
    }

    /// Atomically swaps the counter value and returns the old value.
    pub fn swap(&self, value: usize) -> usize {
        self.0.swap(value, Ordering::AcqRel)
    }

    /// Atomically compares and swaps the value.
    pub fn compare_and_swap(&self, current: usize, new: usize) -> usize {
        self.0
            .compare_exchange(current, new, Ordering::AcqRel, Ordering::Acquire)
            .unwrap_or(current)
    }
}

impl Clone for Counter {
    fn clone(&self) -> Self {
        Self::with_value(self.get())
    }
}

// ============================================================================
// Counter64 - 64-bit thread-safe counter
// ============================================================================

/// A 64-bit thread-safe counter for large counts.
///
/// Similar to [`Counter`] but uses AtomicU64 for larger values.
///
/// ## Example
///
/// ```rust
/// use spider_core::state::Counter64;
///
/// let counter = Counter64::new();
/// counter.add(1_000_000_000);
/// assert_eq!(counter.get(), 1_000_000_000);
/// ```
#[derive(Debug, Default)]
pub struct Counter64(AtomicU64);

impl Counter64 {
    /// Creates a new counter initialized to 0.
    pub fn new() -> Self {
        Self(AtomicU64::new(0))
    }

    /// Creates a new counter with the specified initial value.
    pub fn with_value(value: u64) -> Self {
        Self(AtomicU64::new(value))
    }

    /// Increments the counter by 1.
    pub fn inc(&self) {
        self.0.fetch_add(1, Ordering::AcqRel);
    }

    /// Decrements the counter by 1.
    pub fn dec(&self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }

    /// Adds a value to the counter.
    pub fn add(&self, value: u64) {
        self.0.fetch_add(value, Ordering::AcqRel);
    }

    /// Subtracts a value from the counter.
    pub fn sub(&self, value: u64) {
        self.0.fetch_sub(value, Ordering::AcqRel);
    }

    /// Gets the current value of the counter.
    pub fn get(&self) -> u64 {
        self.0.load(Ordering::Acquire)
    }

    /// Sets the counter to a specific value.
    pub fn set(&self, value: u64) {
        self.0.store(value, Ordering::Release);
    }
}

impl Clone for Counter64 {
    fn clone(&self) -> Self {
        Self::with_value(self.get())
    }
}

// ============================================================================
// Flag - Thread-safe boolean flag
// ============================================================================

/// A thread-safe boolean flag.
///
/// Useful for tracking state flags that need to be accessed from multiple threads.
///
/// ## Example
///
/// ```rust
/// use spider_core::state::Flag;
///
/// let flag = Flag::new(false);
/// flag.set(true);
/// assert!(flag.get());
/// ```
#[derive(Debug, Default)]
pub struct Flag(AtomicBool);

impl Flag {
    /// Creates a new flag with the specified initial value.
    pub fn new(value: bool) -> Self {
        Self(AtomicBool::new(value))
    }

    /// Gets the current value of the flag.
    pub fn get(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }

    /// Sets the flag to the specified value.
    pub fn set(&self, value: bool) {
        self.0.store(value, Ordering::Release);
    }

    /// Atomically swaps the flag value and returns the old value.
    pub fn swap(&self, value: bool) -> bool {
        self.0.swap(value, Ordering::AcqRel)
    }

    /// Atomically compares and swaps the value.
    pub fn compare_and_swap(&self, current: bool, new: bool) -> bool {
        self.0
            .compare_exchange(current, new, Ordering::AcqRel, Ordering::Acquire)
            .unwrap_or(current)
    }
}

impl Clone for Flag {
    fn clone(&self) -> Self {
        Self::new(self.get())
    }
}

// ============================================================================
// VisitedUrls - Thread-safe URL tracker
// ============================================================================

/// A thread-safe URL tracker using DashMap.
///
/// This provides efficient concurrent access for tracking visited URLs
/// without requiring explicit locks.
///
/// ## Example
///
/// ```rust
/// use spider_core::state::VisitedUrls;
///
/// let visited = VisitedUrls::new();
/// visited.mark("https://example.com".to_string());
/// assert!(visited.is_visited("https://example.com"));
/// ```
#[derive(Debug, Default)]
pub struct VisitedUrls {
    urls: DashMap<String, bool>,
}

impl VisitedUrls {
    /// Creates a new empty URL tracker.
    pub fn new() -> Self {
        Self {
            urls: DashMap::new(),
        }
    }

    /// Creates a URL tracker with the specified capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            urls: DashMap::with_capacity(capacity),
        }
    }

    /// Marks a URL as visited.
    pub fn mark(&self, url: String) {
        self.urls.insert(url, true);
    }

    /// Checks if a URL has been visited.
    pub fn is_visited(&self, url: &str) -> bool {
        self.urls.contains_key(url)
    }

    /// Removes a URL from the visited set.
    pub fn remove(&self, url: &str) {
        self.urls.remove(url);
    }

    /// Returns the number of visited URLs.
    pub fn len(&self) -> usize {
        self.urls.len()
    }

    /// Returns true if no URLs have been visited.
    pub fn is_empty(&self) -> bool {
        self.urls.is_empty()
    }

    /// Clears all visited URLs.
    pub fn clear(&self) {
        self.urls.clear();
    }
}

impl Clone for VisitedUrls {
    fn clone(&self) -> Self {
        Self {
            urls: self.urls.clone(),
        }
    }
}

// ============================================================================
// ConcurrentMap - Thread-safe key-value map
// ============================================================================

/// A thread-safe key-value map using DashMap.
///
/// Provides concurrent read/write access without explicit locking.
///
/// ## Example
///
/// ```rust
/// use spider_core::state::ConcurrentMap;
///
/// let map = ConcurrentMap::new();
/// map.insert("key".to_string(), 42);
/// assert_eq!(map.get(&"key".to_string()), Some(42));
/// ```
pub struct ConcurrentMap<K, V> {
    map: DashMap<K, V>,
}

impl<K, V> Default for ConcurrentMap<K, V>
where
    K: Eq + std::hash::Hash + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> std::fmt::Debug for ConcurrentMap<K, V>
where
    K: Eq + std::hash::Hash + Clone + Send + Sync + 'static + std::fmt::Debug,
    V: Clone + Send + Sync + std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConcurrentMap")
            .field("count", &self.map.len())
            .finish()
    }
}

impl<K, V> ConcurrentMap<K, V>
where
    K: Eq + std::hash::Hash + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync,
{
    /// Creates a new empty map.
    pub fn new() -> Self {
        Self {
            map: DashMap::new(),
        }
    }

    /// Creates a map with the specified capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            map: DashMap::with_capacity(capacity),
        }
    }

    /// Inserts a key-value pair.
    pub fn insert(&self, key: K, value: V) -> Option<V> {
        self.map.insert(key, value)
    }

    /// Gets a reference to a value.
    pub fn get(&self, key: &K) -> Option<V> {
        self.map.get(key).map(|ref_multi| ref_multi.clone())
    }

    /// Removes a key from the map.
    pub fn remove(&self, key: &K) -> Option<V> {
        self.map.remove(key).map(|(_, v)| v)
    }

    /// Returns the number of entries in the map.
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Returns true if the map is empty.
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Clears all entries.
    pub fn clear(&self) {
        self.map.clear();
    }

    /// Returns true if the map contains the key.
    pub fn contains_key(&self, key: &K) -> bool {
        self.map.contains_key(key)
    }
}

impl<K, V> Clone for ConcurrentMap<K, V>
where
    K: Eq + std::hash::Hash + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync,
{
    fn clone(&self) -> Self {
        Self {
            map: self.map.clone(),
        }
    }
}

// ============================================================================
// ConcurrentVec - Thread-safe vector
// ============================================================================

/// A thread-safe vector using RwLock.
///
/// Provides concurrent read access with exclusive write access.
///
/// ## Example
///
/// ```rust
/// use spider_core::state::ConcurrentVec;
///
/// let vec = ConcurrentVec::new();
/// vec.push(1);
/// vec.push(2);
/// assert_eq!(vec.len(), 2);
/// ```
#[derive(Debug, Default)]
pub struct ConcurrentVec<T> {
    vec: RwLock<Vec<T>>,
}

impl<T> ConcurrentVec<T>
where
    T: Clone + Send + Sync + 'static,
{
    /// Creates a new empty vector.
    pub fn new() -> Self {
        Self {
            vec: RwLock::new(Vec::new()),
        }
    }

    /// Creates a vector with the specified capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            vec: RwLock::new(Vec::with_capacity(capacity)),
        }
    }

    /// Pushes an element to the vector.
    pub fn push(&self, value: T) {
        self.vec.write().push(value);
    }

    /// Returns the number of elements.
    pub fn len(&self) -> usize {
        self.vec.read().len()
    }

    /// Returns true if the vector is empty.
    pub fn is_empty(&self) -> bool {
        self.vec.read().is_empty()
    }

    /// Clears all elements.
    pub fn clear(&self) {
        self.vec.write().clear();
    }

    /// Returns a copy of all elements.
    pub fn to_vec(&self) -> Vec<T> {
        self.vec.read().clone()
    }
}

impl<T> Clone for ConcurrentVec<T>
where
    T: Clone + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            vec: RwLock::new(self.vec.read().clone()),
        }
    }
}

// ============================================================================
// StateAccessMetrics - Metrics for tracking state access patterns
// ============================================================================

/// Metrics for tracking state access patterns.
///
/// Useful for debugging and performance monitoring of state access.
///
/// ## Example
///
/// ```rust
/// use spider_core::state::StateAccessMetrics;
///
/// let metrics = StateAccessMetrics::new();
/// metrics.record_read();
/// metrics.record_write();
/// println!("Reads: {}, Writes: {}", metrics.read_count(), metrics.write_count());
/// ```
#[derive(Debug, Default)]
pub struct StateAccessMetrics {
    read_count: AtomicUsize,
    write_count: AtomicUsize,
    concurrent_access_peak: AtomicUsize,
    current_concurrent: AtomicUsize,
}

impl StateAccessMetrics {
    /// Creates a new metrics tracker.
    pub fn new() -> Self {
        Self::default()
    }

    /// Records a read access.
    pub fn record_read(&self) {
        self.read_count.fetch_add(1, Ordering::AcqRel);
    }

    /// Records a write access.
    pub fn record_write(&self) {
        self.write_count.fetch_add(1, Ordering::AcqRel);
    }

    /// Records the start of an access (read or write).
    pub fn record_access_start(&self) {
        let current = self.current_concurrent.fetch_add(1, Ordering::AcqRel);
        let peak = self.concurrent_access_peak.load(Ordering::Acquire);
        if current + 1 > peak {
            self.concurrent_access_peak
                .compare_exchange(peak, current + 1, Ordering::AcqRel, Ordering::Acquire)
                .ok();
        }
    }

    /// Records the end of an access.
    pub fn record_access_end(&self) {
        self.current_concurrent.fetch_sub(1, Ordering::AcqRel);
    }

    /// Returns the total number of read accesses.
    pub fn read_count(&self) -> usize {
        self.read_count.load(Ordering::Acquire)
    }

    /// Returns the total number of write accesses.
    pub fn write_count(&self) -> usize {
        self.write_count.load(Ordering::Acquire)
    }

    /// Returns the peak concurrent access count.
    pub fn concurrent_access_peak(&self) -> usize {
        self.concurrent_access_peak.load(Ordering::Acquire)
    }

    /// Returns the current concurrent access count.
    pub fn current_concurrent(&self) -> usize {
        self.current_concurrent.load(Ordering::Acquire)
    }

    /// Resets all counters.
    pub fn reset(&self) {
        self.read_count.store(0, Ordering::Release);
        self.write_count.store(0, Ordering::Release);
        self.concurrent_access_peak.store(0, Ordering::Release);
        self.current_concurrent.store(0, Ordering::Release);
    }
}

impl Clone for StateAccessMetrics {
    fn clone(&self) -> Self {
        Self {
            read_count: AtomicUsize::new(self.read_count.load(Ordering::Acquire)),
            write_count: AtomicUsize::new(self.write_count.load(Ordering::Acquire)),
            concurrent_access_peak: AtomicUsize::new(
                self.concurrent_access_peak.load(Ordering::Acquire),
            ),
            current_concurrent: AtomicUsize::new(self.current_concurrent.load(Ordering::Acquire)),
        }
    }
}
