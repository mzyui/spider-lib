//! Item traits and parse results.
//!
//! [`ParseOutput`] is what a spider returns after parsing a page. It carries the
//! emitted items plus any follow-up requests discovered on that page.
//!
//! ## Example
//!
//! ```rust,ignore
//! use spider_util::item::{ScrapedItem, ParseOutput};
//!
//! #[spider_macro::scraped_item]
//! struct Article {
//!     title: String,
//!     content: String,
//! }
//!
//! // In your spider's parse method:
//! // let mut output = ParseOutput::new();
//! // output.add_item(Article { title: "...", content: "..." });
//! // Ok(output)
//! ```
//!
//! `ParseOutput` is intentionally small: it is just the handoff object between
//! parsing and the rest of the runtime. Use it to emit items, schedule new
//! requests, or both from the same page.

use crate::request::Request;
use serde_json::Value;
use std::any::Any;
use std::fmt::Debug;

/// The output returned by a spider's `parse` method.
#[derive(Debug, Clone)]
pub struct ParseOutput<I> {
    items: Vec<I>,
    requests: Vec<Request>,
}

impl<I> ParseOutput<I> {
    /// Creates a new, empty `ParseOutput`.
    ///
    /// Most spiders start each parse call with this and then append items and
    /// follow-up requests as they discover them.
    pub fn new() -> Self {
        Self {
            items: Vec::new(),
            requests: Vec::new(),
        }
    }

    /// Consumes the `ParseOutput` and returns its inner items and requests.
    ///
    /// This is mainly used by the runtime, but can also be handy in isolated
    /// parsing helpers.
    pub fn into_parts(self) -> (Vec<I>, Vec<Request>) {
        (self.items, self.requests)
    }

    /// Adds a scraped item to the output.
    ///
    /// Use this when the current page produced one structured result that
    /// should continue through the configured pipeline chain.
    pub fn add_item(&mut self, item: I) {
        self.items.push(item);
    }

    /// Adds a new request to be crawled.
    ///
    /// Requests emitted here are handed back to the scheduler after the parse
    /// step completes.
    pub fn add_request(&mut self, request: Request) {
        self.requests.push(request);
    }

    /// Adds multiple scraped items to the output.
    pub fn add_items(&mut self, items: impl IntoIterator<Item = I>) {
        self.items.extend(items);
    }

    /// Adds multiple new requests to be crawled.
    pub fn add_requests(&mut self, requests: impl IntoIterator<Item = Request>) {
        self.requests.extend(requests);
    }
}

impl<I> Default for ParseOutput<I> {
    fn default() -> Self {
        Self::new()
    }
}

/// Trait implemented by item types emitted from spiders.
///
/// In normal application code you usually do not implement this trait by hand.
/// Prefer annotating the item struct with `#[scraped_item]`, which wires up the
/// required serialization and cloning behavior automatically.
pub trait ScrapedItem: Debug + Send + Sync + Any + 'static {
    /// Returns the item as a `dyn Any` for downcasting.
    fn as_any(&self) -> &dyn Any;
    /// Clones the item into a `Box<dyn ScrapedItem>`.
    fn box_clone(&self) -> Box<dyn ScrapedItem + Send + Sync>;
    /// Converts the item to a `serde_json::Value`.
    fn to_json_value(&self) -> Value;
}

impl Clone for Box<dyn ScrapedItem + Send + Sync> {
    fn clone(&self) -> Self {
        self.box_clone()
    }
}
