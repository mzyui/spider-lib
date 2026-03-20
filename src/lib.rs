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
//! ## Installation
//!
//! ```toml
//! [dependencies]
//! spider-lib = "3.0.1"
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
//!     async fn parse(
//!         &self,
//!         response: Response,
//!         _state: &Self::State,
//!     ) -> Result<ParseOutput<Self::Item>, SpiderError> {
//!         let html = response.to_html()?;
//!         let mut output = ParseOutput::new();
//!
//!         for quote in html.select(&".quote".to_selector()?) {
//!             let text = quote
//!                 .select(&".text".to_selector()?)
//!                 .next()
//!                 .map(|node| node.text().collect::<String>())
//!                 .unwrap_or_default();
//!
//!             let author = quote
//!                 .select(&".author".to_selector()?)
//!                 .next()
//!                 .map(|node| node.text().collect::<String>())
//!                 .unwrap_or_default();
//!
//!             output.add_item(Quote { text, author });
//!         }
//!
//!         Ok(output)
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
//! [`Spider::parse`] takes `&self` and a separate shared state parameter.
//! That design keeps the spider itself immutable while still allowing
//! concurrent parsing with user-defined shared state.

pub mod prelude;
pub use prelude::*;

// Re-export procedural macros
pub use spider_macro::scraped_item;
