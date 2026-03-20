//! Convenient re-exports for code that depends on `spider-core` directly.
//!
//! ```rust
//! use spider_core::prelude::*;
//! ```

pub use crate::{
    // Core structs
    Crawler,
    // Core traits
    Downloader,
    Spider,
    StartRequestIter,
    StartRequests,
    // Essential re-exports for trait implementation
    async_trait,
    // Procedural macro
    scraped_item,
};

// Import types from other crates
pub use spider_middleware::middleware::{Middleware, MiddlewareAction};
pub use spider_util::{
    error::{PipelineError, SpiderError},
    request::Request,
};
