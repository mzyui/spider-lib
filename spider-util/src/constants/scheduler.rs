//! Defaults used by the scheduler and duplicate-detection path.

/// Capacity of the visited URL cache in the scheduler.
pub const VISITED_URL_CACHE_CAPACITY: u64 = 500_000;

/// Default capacity for the visited URL cache when not using checkpoint.
pub const DEFAULT_VISITED_CACHE_SIZE: u64 = 200_000;

/// Maximum number of pending requests before applying backpressure.
pub const MAX_PENDING_REQUESTS: usize = 30_000;

/// Time-to-idle for visited URL cache entries (1 hour).
pub const VISITED_URL_CACHE_TTL_SECS: u64 = 3600;

/// Capacity of the Bloom filter for duplicate detection.
pub const BLOOM_FILTER_CAPACITY: u64 = 5_000_000;

/// Number of hash functions used by the Bloom filter.
pub const BLOOM_FILTER_HASH_FUNCTIONS: usize = 5;

/// Buffer size before flushing to Bloom filter.
pub const BLOOM_BUFFER_FLUSH_SIZE: usize = 100;

/// Interval in milliseconds for periodic Bloom filter flush.
pub const BLOOM_FLUSH_INTERVAL_MS: u64 = 100;
