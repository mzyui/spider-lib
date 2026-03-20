//! Default values used by built-in middleware.

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
