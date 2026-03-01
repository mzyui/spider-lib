//! # spider-util
//!
//! Utility types and traits for the `spider-lib` framework.
//!
//! ## Example
//!
//! ```rust,ignore
//! use spider_util::{request::Request, item::ScrapedItem};
//! use url::Url;
//!
//! let url = Url::parse("https://example.com").unwrap();
//! let request = Request::new(url);
//!
//! #[spider_macro::scraped_item]
//! struct Article {
//!     title: String,
//!     content: String,
//! }
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
    format_bytes, format_duration, format_request_time, ByteFormatter, DefaultByteFormatter,
    DefaultDurationFormatter, DefaultRateCalculator, DurationFormatter, RateCalculator,
};
pub use http_client::HttpClient;
