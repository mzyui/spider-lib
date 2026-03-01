//! Global constants used across the spider-lib workspace.
//!
//! This module centralizes all magic numbers and configuration values
//! to ensure consistency and ease of maintenance.

// ============================================================================
// Scheduler Constants
// ============================================================================

/// Capacity of the visited URL cache in the scheduler.
pub const VISITED_URL_CACHE_CAPACITY: u64 = 500_000;

/// Default capacity for the visited URL cache when not using checkpoint.
pub const DEFAULT_VISITED_CACHE_SIZE: u64 = 200_000;

/// Maximum number of pending requests before applying backpressure.
pub const MAX_PENDING_REQUESTS: usize = 30_000;

/// Time-to-idle for visited URL cache entries (1 hour).
pub const VISITED_URL_CACHE_TTL_SECS: u64 = 3600;

// ============================================================================
// Bloom Filter Constants
// ============================================================================

/// Capacity of the Bloom filter for duplicate detection.
pub const BLOOM_FILTER_CAPACITY: u64 = 5_000_000;

/// Number of hash functions used by the Bloom filter.
pub const BLOOM_FILTER_HASH_FUNCTIONS: usize = 5;

/// Buffer size before flushing to Bloom filter.
pub const BLOOM_BUFFER_FLUSH_SIZE: usize = 100;

/// Interval in milliseconds for periodic Bloom filter flush.
pub const BLOOM_FLUSH_INTERVAL_MS: u64 = 100;

// ============================================================================
// Rate Limit Constants
// ============================================================================

/// Initial delay for adaptive rate limiting (500ms).
pub const RATE_LIMIT_INITIAL_DELAY_MS: u64 = 500;

/// Minimum delay for rate limiting (50ms).
pub const RATE_LIMIT_MIN_DELAY_MS: u64 = 50;

/// Maximum delay for rate limiting (60 seconds).
pub const RATE_LIMIT_MAX_DELAY_MS: u64 = 60_000;

/// Maximum jitter for rate limiting (500ms).
pub const RATE_LIMIT_MAX_JITTER_MS: u64 = 500;

/// Error penalty multiplier for adaptive rate limiting.
pub const RATE_LIMIT_ERROR_PENALTY_MULTIPLIER: f64 = 1.5;

/// Success decay multiplier for adaptive rate limiting.
pub const RATE_LIMIT_SUCCESS_DECAY_MULTIPLIER: f64 = 0.95;

/// Forbidden penalty multiplier for adaptive rate limiting.
pub const RATE_LIMIT_FORBIDDEN_PENALTY_MULTIPLIER: f64 = 1.2;

// ============================================================================
// Middleware Constants
// ============================================================================

/// Default cache TTL for middleware (1 hour).
pub const MIDDLEWARE_CACHE_TTL_SECS: u64 = 3600;

/// Default cache capacity for middleware.
pub const MIDDLEWARE_CACHE_CAPACITY: u64 = 10_000;

/// Default retry attempts for retry middleware.
pub const RETRY_DEFAULT_MAX_RETRIES: u32 = 3;

/// Default backoff factor for retry middleware.
pub const RETRY_DEFAULT_BACKOFF_FACTOR: f64 = 1.0;

/// Default maximum delay for retry middleware (3 minutes).
pub const RETRY_DEFAULT_MAX_DELAY_MS: u64 = 180_000;

/// Default HTTP status codes to retry.
pub const RETRY_DEFAULT_HTTP_CODES: &[u16] = &[500, 502, 503, 504, 408, 429];

// ============================================================================
// Pipeline Constants
// ============================================================================

/// Buffer size for CSV export pipeline.
pub const CSV_BUFFER_SIZE: usize = 8192;

/// Channel capacity for SQLite pipeline.
pub const SQLITE_CHANNEL_CAPACITY: usize = 100;

/// Default batch size for stream JSON pipeline.
pub const STREAM_JSON_DEFAULT_BATCH_SIZE: usize = 100;

// ============================================================================
// Downloader Constants
// ============================================================================

/// Default request timeout in seconds.
pub const DEFAULT_REQUEST_TIMEOUT_SECS: u64 = 30;

/// Connection pool idle timeout in seconds.
pub const CONNECTION_POOL_IDLE_TIMEOUT_SECS: u64 = 120;

/// TCP keepalive in seconds.
pub const TCP_KEEPALIVE_SECS: u64 = 60;

/// Connect timeout in seconds.
pub const CONNECT_TIMEOUT_SECS: u64 = 10;

/// Maximum idle connections per host for default client.
pub const DEFAULT_POOL_MAX_IDLE_PER_HOST: usize = 200;

/// Maximum idle connections per host for host-specific clients.
pub const HOST_SPECIFIC_POOL_MAX_IDLE_PER_HOST: usize = 50;

// ============================================================================
// Crawler Constants
// ============================================================================

/// Default channel capacity for crawler communication.
pub const CRAWLER_DEFAULT_CHANNEL_CAPACITY: usize = 1000;

/// Default grace period for crawler shutdown in seconds.
pub const CRAWLER_SHUTDOWN_GRACE_PERIOD_SECS: u64 = 30;

/// Idle check interval in milliseconds.
pub const CRAWLER_IDLE_CHECK_INTERVAL_MS: u64 = 100;
