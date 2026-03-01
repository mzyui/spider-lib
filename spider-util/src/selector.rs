//! # Selector Cache Module
//!
//! Provides a global cache for compiled CSS selectors to improve parsing performance.
//!
//! ## Overview
//!
//! The selector cache module implements a global caching mechanism for compiled
//! CSS selectors used in HTML parsing. Since selector compilation can be expensive,
//! especially when the same selectors are used repeatedly during crawling,
//! this module caches compiled selectors to avoid repeated compilation overhead.
//! The cache uses a thread-safe approach to allow concurrent access from multiple
//! crawler threads.
//!
//! ## Key Components
//!
//! - **SELECTOR_CACHE**: Global static cache using Lazy initialization
//! - **get_cached_selector**: Main function to retrieve or compile selectors
//! - **prewarm_cache**: Function to pre-populate the cache with common selectors
//! - **Thread Safety**: Uses RwLock for concurrent read/write access
//!
//! ## Performance Benefits
//!
//! The selector cache provides significant performance improvements when processing
//! many pages with similar HTML structures. By caching compiled selectors,
//! the system avoids the computational cost of parsing the same CSS selector
//! expressions repeatedly. The cache uses a read-write lock to allow multiple
//! concurrent readers while ensuring thread safety during cache updates.
//!
//! ## Example
//!
//! ```rust,ignore
//! use spider_util::selector_cache::get_cached_selector;
//!
//! // Get a cached selector (compiles and caches if not already present)
//! if let Some(selector) = get_cached_selector("div.content > p") {
//!     // Use the selector for parsing HTML
//!     // The selector is now cached for future use
//! }
//!
//! // Pre-warm the cache with commonly used selectors
//! spider_util::selector_cache::prewarm_cache();
//! ```

use once_cell::sync::Lazy;
use parking_lot::RwLock;
use scraper::Selector;
use std::collections::HashMap;

// Global selector cache to avoid repeated compilation
static SELECTOR_CACHE: Lazy<RwLock<HashMap<String, Selector>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));

/// Get a compiled selector from the cache or compile and store it if not present
pub fn get_cached_selector(selector_str: &str) -> Option<Selector> {
    {
        let cache = SELECTOR_CACHE.read();
        if let Some(cached) = cache.get(selector_str) {
            return Some(cached.clone());
        }
    }

    match Selector::parse(selector_str) {
        Ok(selector) => {
            {
                let mut cache = SELECTOR_CACHE.write();
                if let Some(cached) = cache.get(selector_str) {
                    return Some(cached.clone());
                }
                cache.insert(selector_str.to_string(), selector.clone());
            }
            Some(selector)
        }
        Err(_) => None,
    }
}

/// Pre-warm the selector cache with commonly used selectors
pub fn prewarm_cache() {
    let common_selectors = vec![
        "a[href]",
        "link[href]",
        "script[src]",
        "img[src]",
        "audio[src]",
        "video[src]",
        "source[src]",
        "form[action]",
        "iframe[src]",
        "frame[src]",
        "embed[src]",
        "object[data]",
    ];

    for selector_str in common_selectors {
        get_cached_selector(selector_str);
    }
}

