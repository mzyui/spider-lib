//! Convenient re-exports for `spider-lib` applications.
//!
//! Most example code in this workspace starts here:
//!
//! ```rust
//! use spider_lib::prelude::*;
//! ```
//!
//! The prelude intentionally groups together the "first spider" surface area:
//! runtime types, the spider trait, common errors, parsing helpers, middleware,
//! and the most common pipelines.

/// Core runtime types and traits used to define and run a crawl.
pub use spider_core::{
    CrawlShapePreset,
    // Core structs
    Crawler,
    CrawlerBuilder,
    CrawlerConfig,
    DiscoveryConfig,
    DiscoveryMode,
    DiscoveryRule,
    // Core traits
    Downloader,
    ReqwestClientDownloader,
    Spider,
    StartRequestIter,
    StartRequests,
    // Essential re-exports for trait implementation
    async_trait,
    // Core modules
    scheduler::Scheduler,
    state::CrawlerState,
    // Thread-safe state primitives
    state::{
        ConcurrentMap, ConcurrentVec, Counter, Counter64, Flag, StateAccessMetrics, VisitedUrls,
    },
    stats::StatCollector,
    tokio,
};

pub use spider_util::item::{FieldValueType, ItemFieldSchema, ItemSchema, TypedItemSchema};
/// Item contracts and parse results returned by [`Spider::parse`].
pub use spider_util::item::{ParseOutput, ScrapedItem};

/// Pipeline trait for item-processing stages.
pub use spider_pipeline::pipeline::Pipeline;

/// Helper macro used to define item structs that satisfy [`ScrapedItem`].
pub use spider_macro::scraped_item;
/// Middleware trait and control-flow type for request/response hooks.
pub use spider_middleware::middleware::{Middleware, MiddlewareAction};
/// Shared runtime data types and convenience helpers.
pub use spider_util::{
    error::{PipelineError, SpiderError},
    request::{Method, Request},
    response::{Link, LinkExtractOptions, LinkSource, LinkType, PageMetadata, Response},
    util::{ToSelector, create_dir, is_same_site, normalize_origin, validate_output_dir},
};

/// Built-in middleware that is available without extra feature flags.
pub use spider_middleware::{
    rate_limit::RateLimitMiddleware, referer::RefererMiddleware, retry::RetryMiddleware,
};

/// File-backed HTTP response cache middleware.
#[cfg(feature = "middleware-cache")]
pub use spider_middleware::http_cache::HttpCacheMiddleware;

/// Adaptive throttling middleware driven by observed response behavior.
#[cfg(feature = "middleware-autothrottle")]
pub use spider_middleware::autothrottle::AutoThrottleMiddleware;

/// Proxy routing middleware.
#[cfg(feature = "middleware-proxy")]
pub use spider_middleware::proxy::ProxyMiddleware;

/// Configurable user-agent selection and rotation middleware.
#[cfg(feature = "middleware-user-agent")]
pub use spider_middleware::user_agent::UserAgentMiddleware;

/// `robots.txt` enforcement middleware.
#[cfg(feature = "middleware-robots")]
pub use spider_middleware::robots::RobotsTxtMiddleware;

/// Shared cookie jar middleware.
#[cfg(feature = "middleware-cookies")]
pub use spider_middleware::cookies::CookieMiddleware;

/// Built-in pipelines that do not require extra feature flags.
pub use spider_pipeline::{
    console::ConsolePipeline,
    dedup::DeduplicationPipeline,
    schema::{
        SchemaExportConfig, SchemaTransformPipeline, SchemaValidationPipeline, SchemaViolation,
    },
    transform::{TransformOperation, TransformPipeline},
    validation::{JsonType, ValidationPipeline, ValidationRule},
};

/// CSV file output pipeline.
#[cfg(feature = "pipeline-csv")]
pub use spider_pipeline::csv::CsvPipeline;

/// JSON array output pipeline.
#[cfg(feature = "pipeline-json")]
pub use spider_pipeline::json::JsonPipeline;

/// JSON Lines output pipeline.
#[cfg(feature = "pipeline-jsonl")]
pub use spider_pipeline::jsonl::JsonlPipeline;

/// SQLite output pipeline.
#[cfg(feature = "pipeline-sqlite")]
pub use spider_pipeline::sqlite::SqlitePipeline;

/// Streaming JSON output pipeline.
#[cfg(feature = "pipeline-stream-json")]
pub use spider_pipeline::stream_json::StreamJsonPipeline;

/// Checkpoint types for save/resume workflows.
#[cfg(feature = "checkpoint")]
pub use spider_core::checkpoint::{Checkpoint, SchedulerCheckpoint};
