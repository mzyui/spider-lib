//! # spider-core
//!
//! Core engine of the `spider-lib` web scraping framework.
//!
//! Provides the main components: `Crawler`, `Scheduler`, `Spider` trait, and infrastructure.
//!
//! ## Example
//!
//! ```rust,ignore
//! use spider_core::{Crawler, CrawlerBuilder, Spider};
//! use spider_util::{response::Response, error::SpiderError, item::ParseOutput};
//!
//! #[spider_macro::scraped_item]
//! struct MyItem {
//!     title: String,
//!     url: String,
//! }
//!
//! struct MySpider;
//!
//! #[async_trait::async_trait]
//! impl Spider for MySpider {
//!     type Item = MyItem;
//!     type State = ();
//!     fn start_requests(&self) -> Result<spider_core::spider::StartRequests<'_>, SpiderError> {
//!         let req = spider_util::request::Request::new("https://example.com".parse()?);
//!         Ok(spider_core::spider::StartRequests::Iter(Box::new(std::iter::once(Ok(req)))))
//!     }
//!     async fn parse(&self, response: Response, state: &Self::State) -> Result<ParseOutput<Self::Item>, SpiderError> {
//!         let _ = (response, state);
//!         todo!()
//!     }
//! }
//!
//! async fn run_crawler() -> Result<(), SpiderError> {
//!     let crawler = CrawlerBuilder::new(MySpider).build().await?;
//!     crawler.start_crawl().await
//! }
//! ```

pub mod builder;
#[cfg(feature = "checkpoint")]
pub mod checkpoint;
pub mod config;
pub mod engine;
pub mod prelude;
pub mod scheduler;
pub mod spider;
pub mod state;
pub mod stats;

// Re-export SchedulerCheckpoint and Checkpoint (when checkpoint feature is enabled)
#[cfg(feature = "checkpoint")]
pub use checkpoint::{Checkpoint, SchedulerCheckpoint};

pub use spider_downloader::{Downloader, HttpClient, ReqwestClientDownloader};

// Re-export CookieStore (when cookie-store feature is enabled)
#[cfg(feature = "cookie-store")]
pub use cookie_store::CookieStore;

pub use builder::CrawlerBuilder;
pub use engine::Crawler;
pub use scheduler::Scheduler;
pub use spider_macro::scraped_item;

pub use async_trait::async_trait;
pub use dashmap::DashMap;
pub use spider::{Spider, StartRequestIter, StartRequests};
pub use state::{
    ConcurrentMap, ConcurrentVec, Counter, Counter64, Flag, StateAccessMetrics, VisitedUrls,
};
pub use tokio;
