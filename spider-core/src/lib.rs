//! # spider-core
//!
//! `spider-core` is the runtime crate behind the rest of the workspace.
//! It owns the crawler loop, scheduling, shared runtime state, statistics, and
//! the [`Spider`] trait used to describe crawl behavior.
//!
//! If you are building an application, `spider-lib` is usually the easier
//! starting point. Depend on `spider-core` directly when you want the runtime
//! API without the facade crate.
//!
//! ## Example
//!
//! ```rust,ignore
//! use spider_core::{async_trait, CrawlerBuilder, Spider};
//! use spider_util::{response::Response, error::SpiderError, item::ParseOutput};
//!
//! #[spider_macro::scraped_item]
//! struct Item {
//!     title: String,
//! }
//!
//! struct MySpider;
//!
//! #[async_trait]
//! impl Spider for MySpider {
//!     type Item = Item;
//!     type State = ();
//!
//!     fn start_requests(&self) -> Result<spider_core::StartRequests<'_>, SpiderError> {
//!         Ok(spider_core::StartRequests::Urls(vec!["https://example.com"]))
//!     }
//!
//!     async fn parse(
//!         &self,
//!         _response: Response,
//!         _state: &Self::State,
//!     ) -> Result<ParseOutput<Self::Item>, SpiderError> {
//!         Ok(ParseOutput::new())
//!     }
//! }
//!
//! async fn run() -> Result<(), SpiderError> {
//!     let crawler = CrawlerBuilder::new(MySpider).build().await?;
//!     crawler.start_crawl().await
//! }
//! ```

pub mod builder;
#[cfg(feature = "checkpoint")]
pub mod checkpoint;
pub mod config;
pub mod discovery;
pub mod engine;
pub mod prelude;
pub mod scheduler;
pub mod spider;
pub mod state;
pub mod stats;

/// Routes parse logic based on the discovery rule name attached to a response.
///
/// This is a lightweight helper for rule-based crawling. The response is only
/// consumed by the matched branch, so each branch may move it into a dedicated
/// parse helper.
#[macro_export]
macro_rules! route_by_rule {
    ($response:expr, _ => $default:expr $(,)?) => {
        $default
    };
    ($response:expr, $rule:literal => $handler:expr, $($rest:tt)+) => {{
        if $response.matches_discovery_rule($rule) {
            $handler
        } else {
            $crate::route_by_rule!($response, $($rest)+)
        }
    }};
}

// Re-export SchedulerCheckpoint and Checkpoint (when checkpoint feature is enabled)
#[cfg(feature = "checkpoint")]
pub use checkpoint::{Checkpoint, SchedulerCheckpoint};

pub use spider_downloader::{Downloader, HttpClient, ReqwestClientDownloader};

// Re-export CookieStore (when cookie-store feature is enabled)
#[cfg(feature = "cookie-store")]
pub use cookie_store::CookieStore;

pub use builder::CrawlerBuilder;
pub use config::{CrawlerConfig, DiscoveryConfig, DiscoveryMode, DiscoveryRule};
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
