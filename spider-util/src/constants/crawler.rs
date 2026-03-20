//! Defaults used by crawler orchestration internals.

/// Default channel capacity for crawler communication.
pub const CRAWLER_DEFAULT_CHANNEL_CAPACITY: usize = 1000;

/// Default grace period for crawler shutdown in seconds.
pub const CRAWLER_SHUTDOWN_GRACE_PERIOD_SECS: u64 = 30;

/// Idle check interval in milliseconds.
pub const CRAWLER_IDLE_CHECK_INTERVAL_MS: u64 = 100;
