//! Downloader/network constants.

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
