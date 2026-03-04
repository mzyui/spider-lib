//! # State Module
//!
//! Provides state tracking primitives for the spider-lib framework.
//!
//! ## Overview
//!
//! This module offers two categories of state management:
//!
//! 1. **Crawler Internal State**: [`CrawlerState`] for tracking operational metrics
//! 2. **Thread-Safe Primitives**: Ready-to-use types for building custom Spider state
//!
//! ## Thread-Safe Primitives
//!
//! The following types are designed for building custom Spider state structures
//! with safe concurrent access:
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
//! use spider_core::{Counter, VisitedUrls, CrawlerState};
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

mod primitives;

pub use primitives::{
    Counter, Counter64, Flag, VisitedUrls,
    ConcurrentMap, ConcurrentVec,
    StateAccessMetrics,
};

// ============================================================================
// Crawler Internal State
// ============================================================================

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Represents the shared state of the crawler's various actors.
///
/// This struct provides a centralized mechanism for monitoring the real-time
/// activity of the web crawler. It utilizes atomic counters to keep track of:
/// - The number of HTTP requests currently in flight (being downloaded).
/// - The number of responses actively being parsed by spiders.
/// - The number of scraped items currently being processed by pipelines.
///
/// This state information is crucial for determining when the crawler is idle
/// and can be gracefully shut down, or when to trigger checkpointing.
#[derive(Debug, Default)]
pub struct CrawlerState {
    /// The number of requests currently being downloaded.
    pub in_flight_requests: AtomicUsize,
    /// The number of responses currently being parsed.
    pub parsing_responses: AtomicUsize,
    /// The number of items currently being processed by pipelines.
    pub processing_items: AtomicUsize,
}

impl CrawlerState {
    /// Creates a new, atomically reference-counted `CrawlerState`.
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Checks if all crawler activities are idle.
    pub fn is_idle(&self) -> bool {
        self.in_flight_requests.load(Ordering::Acquire) == 0
            && self.parsing_responses.load(Ordering::Acquire) == 0
            && self.processing_items.load(Ordering::Acquire) == 0
    }
}
