//! Global constants used across the spider-lib workspace.
//!
//! This module is a facade that re-exports domain-specific constant groups.
//! Keep cross-crate and publicly tunable values here; keep local implementation
//! details near their usage sites.

mod crawler;
mod downloader;
mod middleware;
mod pipeline;
mod scheduler;

pub use crawler::*;
pub use downloader::*;
pub use middleware::*;
pub use pipeline::*;
pub use scheduler::*;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scheduler_invariants() {
        assert!(DEFAULT_VISITED_CACHE_SIZE <= VISITED_URL_CACHE_CAPACITY);
        assert!(MAX_PENDING_REQUESTS > 0);
        assert!(BLOOM_FILTER_HASH_FUNCTIONS > 0);
        assert!(BLOOM_BUFFER_FLUSH_SIZE > 0);
    }

    #[test]
    fn rate_limit_invariants() {
        assert!(RATE_LIMIT_MIN_DELAY_MS <= RATE_LIMIT_INITIAL_DELAY_MS);
        assert!(RATE_LIMIT_INITIAL_DELAY_MS <= RATE_LIMIT_MAX_DELAY_MS);
        assert!(RATE_LIMIT_MAX_JITTER_MS <= RATE_LIMIT_MAX_DELAY_MS);
    }

    #[test]
    fn downloader_and_pipeline_invariants() {
        assert!(CONNECT_TIMEOUT_SECS <= DEFAULT_REQUEST_TIMEOUT_SECS);
        assert!(HOST_SPECIFIC_POOL_MAX_IDLE_PER_HOST <= DEFAULT_POOL_MAX_IDLE_PER_HOST);
        assert!(SQLITE_CHANNEL_CAPACITY > 0);
        assert!(STREAM_JSON_DEFAULT_BATCH_SIZE > 0);
    }
}
