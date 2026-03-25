//! Runtime state helpers.
//!
//! This module exposes the internal crawler state plus a small set of
//! thread-safe primitives that are useful in user-defined spider state.
//!
//! ## Example
//!
//! ```rust,ignore
//! use spider_core::{Counter, VisitedUrls};
//! use spider_core::state::CrawlerState;
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
    ConcurrentMap, ConcurrentVec, Counter, Counter64, Flag, StateAccessMetrics, VisitedUrls,
};

// ============================================================================
// Crawler Internal State
// ============================================================================

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

/// Internal shared state used by the runtime.
#[derive(Debug, Default)]
pub struct CrawlerState {
    /// The number of requests currently being downloaded.
    pub in_flight_requests: AtomicUsize,
    /// The number of responses currently being parsed.
    pub parsing_responses: AtomicUsize,
    /// The number of items currently being processed by pipelines.
    pub processing_items: AtomicUsize,
    /// The number of scraped items admitted into the processing pipeline.
    pub admitted_items: AtomicUsize,
    /// Indicates that the crawl is shutting down because the item limit was reached.
    pub item_limit_reached: AtomicBool,
    /// Number of follow-up requests skipped because item-limit shutdown was in progress.
    pub shutdown_skipped_requests: AtomicUsize,
    /// Number of scraped items dropped because item-limit shutdown was in progress.
    pub shutdown_dropped_items: AtomicUsize,
    /// Number of visited-mark updates skipped because item-limit shutdown was in progress.
    pub shutdown_skipped_visited_marks: AtomicUsize,
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
