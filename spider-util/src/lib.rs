//! # spider-util
//!
//! Shared types and helper modules used across the `spider-*` workspace.
//!
//! This crate is where request and response models, error types, selector
//! helpers, formatting helpers, metrics helpers, and other common utilities
//! live.
//!
//! ## Example
//!
//! ```rust,ignore
//! use spider_util::request::Request;
//! use url::Url;
//!
//! let url = Url::parse("https://example.com").unwrap();
//! let request = Request::new(url);
//! let _ = request;
//! ```

pub mod bloom;
pub mod constants;
pub mod error;
pub mod formatters;
pub mod http_client;
pub mod item;
pub mod metrics;
pub mod request;
pub mod response;
pub mod selector;
pub mod util;

pub use constants::*;
pub use formatters::{
    ByteFormatter, DefaultByteFormatter, DefaultDurationFormatter, DefaultRateCalculator,
    DurationFormatter, RateCalculator, format_bytes, format_duration, format_request_time,
};
pub use http_client::HttpClient;
