//! Common `spider-middleware` re-exports.
//!
//! Useful when you depend on this crate directly and want the trait plus the
//! built-in middleware types in one import.

pub use spider_util::request::Request;
pub use spider_util::response::Response;

// Core middleware (always available)
pub use crate::rate_limit::RateLimitMiddleware;
pub use crate::referer::RefererMiddleware;
pub use crate::retry::RetryMiddleware;

// Optional middleware (available when features are enabled)
#[cfg(feature = "middleware-autothrottle")]
pub use crate::autothrottle::AutoThrottleMiddleware;

#[cfg(feature = "middleware-user-agent")]
pub use crate::user_agent::UserAgentMiddleware;

#[cfg(feature = "middleware-cookies")]
pub use crate::cookies::CookieMiddleware;

#[cfg(feature = "middleware-cache")]
pub use crate::http_cache::HttpCacheMiddleware;

#[cfg(feature = "middleware-proxy")]
pub use crate::proxy::ProxyMiddleware;

#[cfg(feature = "middleware-robots")]
pub use crate::robots::RobotsTxtMiddleware;

// Re-export the core middleware trait
pub use crate::middleware::{Middleware, MiddlewareAction};
