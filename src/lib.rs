//! # spider-lib
//!
//! `spider-lib` is the easiest way to use this workspace as an application
//! framework. It re-exports the crawler runtime, common middleware and
//! pipelines, shared request and response types, and the `#[scraped_item]`
//! macro behind one crate.
//!
//! If you want the lower-level pieces individually, the workspace also exposes
//! `spider-core`, `spider-middleware`, `spider-pipeline`, `spider-downloader`,
//! `spider-macro`, and `spider-util`. Most users should start here.
//!
//! ## What you get from the facade crate
//!
//! The root crate is optimized for application authors:
//!
//! - [`prelude`] re-exports the common types needed to define and run a spider
//! - [`Spider`] describes crawl behavior
//! - [`CrawlerBuilder`] assembles the runtime
//! - [`Request`], [`Response`], [`ParseContext`], and [`ParseOutput`] are the core runtime types
//! - [`Response::css`](spider_util::response::Response::css) provides Scrapy-like builtin selectors
//! - middleware and pipelines can be enabled with feature flags and then added
//!   through the builder
//!
//! ## Installation
//!
//! ```toml
//! [dependencies]
//! spider-lib = "4.0.0"
//! serde = { version = "1.0", features = ["derive"] }
//! serde_json = "1.0"
//! ```
//!
//! `serde` and `serde_json` are required when you use [`scraped_item`].
//!
//! ## Quick start
//!
//! ```rust,ignore
//! use spider_lib::prelude::*;
//!
//! #[scraped_item]
//! struct Quote {
//!     text: String,
//!     author: String,
//! }
//!
//! struct QuotesSpider;
//!
//! #[async_trait]
//! impl Spider for QuotesSpider {
//!     type Item = Quote;
//!     type State = ();
//!
//!     fn start_requests(&self) -> Result<StartRequests<'_>, SpiderError> {
//!         Ok(StartRequests::Urls(vec!["https://quotes.toscrape.com/"]))
//!     }
//!
//!     async fn parse(&self, cx: ParseContext<'_, Self>) -> Result<(), SpiderError> {
//!         for quote in cx.css(".quote")? {
//!             let text = quote
//!                 .css(".text::text")?
//!                 .get()
//!                 .unwrap_or_default();
//!
//!             let author = quote
//!                 .css(".author::text")?
//!                 .get()
//!                 .unwrap_or_default();
//!
//!             cx.add_item(Quote { text, author }).await?;
//!         }
//!
//!         Ok(())
//!     }
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<(), SpiderError> {
//!     let crawler = CrawlerBuilder::new(QuotesSpider).build().await?;
//!     crawler.start_crawl().await
//! }
//! ```
//!
//! The built-in selector API is the recommended path for HTML extraction:
//! `cx.css(".card")?`, `node.css("a::attr(href)")?.get()`, and
//! `node.css(".title::text")?.get()`.
//!
//! [`Spider::parse`] takes `&self` and a single [`ParseContext`] parameter.
//! That design keeps the spider itself immutable while still giving parse logic
//! access to the current response, shared state, and async output methods.
//!
//! ## Typical next steps
//!
//! After the minimal spider works, the next additions are usually:
//!
//! 1. add one or more middleware with [`CrawlerBuilder::add_middleware`]
//! 2. add one or more pipelines with [`CrawlerBuilder::add_pipeline`]
//! 3. move repeated parse-time state into [`Spider::State`]
//! 4. enable optional features such as `live-stats`, `pipeline-csv`, or
//!    `middleware-robots`
//!
//! If you find yourself needing transport-level customization, custom
//! middleware contracts, or lower-level runtime control, move down to the
//! crate-specific APIs in `spider-core`, `spider-downloader`,
//! `spider-middleware`, or `spider-pipeline`.

extern crate self as spider_lib;

pub mod prelude;
/// Re-export the application-facing prelude.
///
/// Most examples and first integrations start with:
///
/// ```rust
/// use spider_lib::prelude::*;
/// ```
pub use prelude::*;
pub use spider_core::route_by_rule;

// Re-export procedural macros
pub use spider_macro::scraped_item;
