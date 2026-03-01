//! A "prelude" for users of the `spider-lib` crate.
//!
//! This prelude re-exports the most commonly used traits, structs, and macros
//! from the entire spider framework so that they can be easily imported.
//!
//! # Example
//!
//! ```
//! use spider_lib::prelude::*;
//! ```

pub use spider_core::{
    // Core structs
    Crawler,
    CrawlerBuilder,
    // Core traits
    Downloader,
    ReqwestClientDownloader,
    Spider,
    // Essential re-exports for trait implementation
    async_trait,
    // Core modules
    scheduler::Scheduler,
    state::CrawlerState,
    stats::StatCollector,
    tokio,
};

// Re-export ParseOutput and ScrapedItem from spider_util
pub use spider_util::{
    item::{ParseOutput, ScrapedItem},
};

// Re-export Pipeline from spider_pipeline
pub use spider_pipeline::pipeline::Pipeline;

// Import types from other crates
pub use spider_macro::scraped_item;
pub use spider_middleware::middleware::{Middleware, MiddlewareAction};
pub use spider_util::{
    error::{PipelineError, SpiderError},
    request::Request,
    response::Response,
    util::{ToSelector, create_dir, is_same_site, normalize_origin, validate_output_dir},
};

pub use spider_middleware::{
    rate_limit::RateLimitMiddleware, referer::RefererMiddleware, retry::RetryMiddleware,
};

#[cfg(feature = "middleware-cache")]
pub use spider_middleware::http_cache::HttpCacheMiddleware;

#[cfg(feature = "middleware-proxy")]
pub use spider_middleware::proxy::ProxyMiddleware;

#[cfg(feature = "middleware-user-agent")]
pub use spider_middleware::user_agent::UserAgentMiddleware;

#[cfg(feature = "middleware-robots")]
pub use spider_middleware::robots::RobotsTxtMiddleware;

#[cfg(feature = "middleware-cookies")]
pub use spider_middleware::cookies::CookieMiddleware;

pub use spider_pipeline::{
    console::ConsolePipeline, dedup::DeduplicationPipeline,
};

#[cfg(feature = "pipeline-csv")]
pub use spider_pipeline::csv::CsvPipeline;

#[cfg(feature = "pipeline-json")]
pub use spider_pipeline::json::JsonPipeline;

#[cfg(feature = "pipeline-jsonl")]
pub use spider_pipeline::jsonl::JsonlPipeline;

#[cfg(feature = "pipeline-sqlite")]
pub use spider_pipeline::sqlite::SqlitePipeline;

#[cfg(feature = "pipeline-stream-json")]
pub use spider_pipeline::stream_json::StreamJsonPipeline;


#[cfg(feature = "checkpoint")]
pub use spider_core::checkpoint::{Checkpoint, SchedulerCheckpoint};
