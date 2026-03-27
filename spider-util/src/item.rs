//! Item traits and parse-time output helpers.
//!
//! [`ParseOutput`] is the async sink carried by a spider's parse context.
//! Spiders typically use it indirectly through `ParseContext` helpers such as
//! `cx.add_item(...)` and `cx.add_request(...)`, while the runtime uses it to
//! stream scraped items and follow-up requests as they are discovered.
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
//! // output.add_item(Article { title: "...", content: "..." }).await?;
//! // output.add_request(request).await?;
//! ```
//!
//! `ParseOutput` intentionally hides the runtime transport details. The
//! crawler can backpressure parsing internally while spider code continues to
//! use familiar `add_*` methods.

use crate::request::Request;
use async_trait::async_trait;
use serde_json::Value;
use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use crate::error::SpiderError;

/// Stable field kinds used by typed item schema metadata.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum FieldValueType {
    Bool,
    Integer,
    Float,
    String,
    Json,
    Sequence,
    Map,
    Unknown,
}

/// Static schema metadata for a single item field.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ItemFieldSchema {
    pub name: String,
    pub rust_type: String,
    pub value_type: FieldValueType,
    pub nullable: bool,
}

/// Static schema metadata for a scraped item type.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ItemSchema {
    pub item_name: String,
    pub version: u32,
    pub fields: Vec<ItemFieldSchema>,
}

impl ItemSchema {
    /// Returns the fields in their declared order.
    pub fn fields(&self) -> &[ItemFieldSchema] {
        &self.fields
    }
}

/// Trait for typed item definitions that can expose static schema metadata.
pub trait TypedItemSchema {
    /// Returns the typed schema for the item.
    fn schema() -> ItemSchema;

    /// Returns the schema version used by the item.
    fn schema_version() -> u32 {
        1
    }
}

#[async_trait]
#[doc(hidden)]
pub trait ParseSink<I>: Send + Sync + 'static {
    async fn add_item(&self, item: I) -> Result<(), SpiderError>;
    async fn add_request(&self, request: Request) -> Result<(), SpiderError>;
}

/// Async output sink passed into a spider's `parse` method.
pub struct ParseOutput<I> {
    sink: Arc<dyn ParseSink<I>>,
}

impl<I: 'static> ParseOutput<I> {
    #[doc(hidden)]
    pub fn from_sink(sink: Arc<dyn ParseSink<I>>) -> Self {
        Self { sink }
    }

    /// Emits a scraped item into the runtime.
    ///
    /// Use this when the current page produced one structured result that
    /// should continue through the configured pipeline chain. This call is
    /// async so the runtime can apply backpressure when downstream work is
    /// saturated.
    pub async fn add_item(&self, item: I) -> Result<(), SpiderError> {
        self.sink.add_item(item).await
    }

    /// Emits a new request to be crawled.
    ///
    /// Requests emitted here are forwarded into the scheduler path.
    pub async fn add_request(&self, request: Request) -> Result<(), SpiderError> {
        self.sink.add_request(request).await
    }

    /// Emits multiple scraped items into the runtime.
    pub async fn add_items(&self, items: impl IntoIterator<Item = I>) -> Result<(), SpiderError> {
        for item in items {
            self.add_item(item).await?;
        }
        Ok(())
    }

    /// Emits multiple new requests to be crawled.
    pub async fn add_requests(
        &self,
        requests: impl IntoIterator<Item = Request>,
    ) -> Result<(), SpiderError> {
        for request in requests {
            self.add_request(request).await?;
        }
        Ok(())
    }
}

impl<I: 'static> Debug for ParseOutput<I> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ParseOutput").finish_non_exhaustive()
    }
}

impl<I> Clone for ParseOutput<I> {
    fn clone(&self) -> Self {
        Self {
            sink: Arc::clone(&self.sink),
        }
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
    /// Returns typed schema metadata when the item type exposes it.
    fn item_schema(&self) -> Option<ItemSchema> {
        None
    }
    /// Returns the schema version used by this item.
    fn item_schema_version(&self) -> u32 {
        1
    }
}

impl Clone for Box<dyn ScrapedItem + Send + Sync> {
    fn clone(&self) -> Self {
        self.box_clone()
    }
}
