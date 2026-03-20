//! Shared error types for the workspace.
//!
//! The runtime keeps transport, parsing, configuration, and pipeline failures in
//! a small set of error enums so applications can match on them consistently.
//!
//! ## Example
//!
//! ```rust,ignore
//! use spider_util::error::{SpiderError, PipelineError};
//! use url::Url;
//!
//! // URL parsing error
//! let result: Result<Url, SpiderError> = Url::parse("not-a-url").map_err(SpiderError::from);
//! if let Err(e) = result {
//!     println!("Error: {}", e);
//! }
//!
//! // Pipeline error
//! let pipeline_err = PipelineError::IoError("File not found".to_string());
//! ```

use http;
use serde_json::Error as SerdeJsonError;
use std::str::Utf8Error;
use thiserror::Error;

/// Simplified wrapper around `reqwest::Error`.
#[derive(Debug, Clone, Error)]
#[error("Reqwest error: {message}")]
pub struct ReqwestError {
    /// A human-readable error message.
    pub message: String,
    /// Whether the error was a connection failure.
    pub is_connect: bool,
    /// Whether the error was a timeout.
    pub is_timeout: bool,
}

impl From<reqwest::Error> for ReqwestError {
    fn from(err: reqwest::Error) -> Self {
        ReqwestError {
            is_connect: err.is_connect(),
            is_timeout: err.is_timeout(),
            message: err.to_string(),
        }
    }
}

/// Main runtime error type used across the crawler stack.
///
/// ## Variants
///
/// - **Network Errors**: [`ReqwestError`](SpiderError::ReqwestError) for HTTP client errors
/// - **URL Errors**: [`UrlParseError`](SpiderError::UrlParseError) for invalid URLs
/// - **Serialization Errors**: [`JsonError`](SpiderError::JsonError) for JSON parsing/serialization
/// - **I/O Errors**: [`IoError`](SpiderError::IoError) for file system operations
/// - **Configuration Errors**: [`ConfigurationError`](SpiderError::ConfigurationError) for invalid settings
/// - **Pipeline Errors**: [`PipelineError`](SpiderError::PipelineError) for item processing failures
/// - **HTML/UTF-8 Errors**: Parse errors for HTML and UTF-8 content
/// - **Robots.txt**: [`BlockedByRobotsTxt`](SpiderError::BlockedByRobotsTxt) for blocked requests
///
/// ## Example
///
/// ```rust,ignore
/// use spider_util::error::SpiderError;
/// use url::Url;
///
/// // Handle different error types
/// match Url::parse("not-a-url").map_err(SpiderError::from) {
///     Ok(url) => println!("Valid URL: {}", url),
///     Err(SpiderError::UrlParseError(e)) => println!("Invalid URL: {}", e),
///     Err(e) => println!("Other error: {}", e),
/// }
/// ```
#[derive(Debug, Clone, Error)]
pub enum SpiderError {
    /// HTTP client error.
    #[error("Reqwest error: {0}")]
    ReqwestError(#[from] ReqwestError),
    /// URL parsing error.
    #[error("Url parsing error: {0}")]
    UrlParseError(#[from] url::ParseError),
    /// JSON parsing or serialization error.
    #[error("Json parsing error: {0}")]
    JsonError(String),
    /// I/O operation error.
    #[error("Io error: {0}")]
    IoError(String),
    /// Invalid configuration error.
    #[error("Configuration error: {0}")]
    ConfigurationError(String),
    /// General unspecified error.
    #[error("General error: {0}")]
    GeneralError(String),
    /// Failed to convert item to string.
    #[error("Failed to convert item to string: {0}")]
    ItemToStringError(String),
    /// Item serialization error.
    #[error("Error during item serialization: {0}")]
    ItemSerializationError(String),
    /// Unknown error.
    #[error("Unknown error")]
    Unknown,
    /// Invalid HTTP header value.
    #[error("Invalid HTTP header value: {0}")]
    InvalidHeaderValue(String),
    /// HTTP header value error.
    #[error("Header value error: {0}")]
    HeaderValueError(String),
    /// HTML parsing error.
    #[error("HTML parsing error: {0}")]
    HtmlParseError(String),
    /// UTF-8 decoding error.
    #[error("UTF-8 parsing error: {0}")]
    Utf8Error(#[from] Utf8Error),
    /// Pipeline processing error.
    #[error("Pipeline error: {0}")]
    PipelineError(#[from] PipelineError),
    /// Request blocked by robots.txt.
    #[error("Request blocked by robots.txt")]
    BlockedByRobotsTxt,
}

impl From<http::header::InvalidHeaderValue> for SpiderError {
    fn from(err: http::header::InvalidHeaderValue) -> Self {
        SpiderError::InvalidHeaderValue(err.to_string())
    }
}

impl From<bincode::Error> for SpiderError {
    fn from(err: bincode::Error) -> Self {
        SpiderError::GeneralError(format!("Bincode error: {}", err))
    }
}

impl From<reqwest::Error> for SpiderError {
    fn from(err: reqwest::Error) -> Self {
        SpiderError::ReqwestError(err.into())
    }
}

impl From<std::io::Error> for SpiderError {
    fn from(err: std::io::Error) -> Self {
        SpiderError::IoError(err.to_string())
    }
}

impl From<SerdeJsonError> for SpiderError {
    fn from(err: SerdeJsonError) -> Self {
        SpiderError::JsonError(err.to_string())
    }
}

/// Error type used by item pipelines.
///
/// ## Variants
///
/// - **[`IoError`](PipelineError::IoError)**: File system or I/O operation failures
/// - **[`ItemError`](PipelineError::ItemError)**: General item processing failures
/// - **[`DatabaseError`](PipelineError::DatabaseError)**: Database operation errors (e.g., SQLite)
/// - **[`SerializationError`](PipelineError::SerializationError)**: JSON/serialization failures
/// - **[`CsvError`](PipelineError::CsvError)**: CSV reading/writing errors
/// - **[`Other`](PipelineError::Other)**: Other unspecified pipeline errors
///
/// ## Example
///
/// ```rust,ignore
/// use spider_util::error::PipelineError;
///
/// let err = PipelineError::IoError("File not found".to_string());
/// println!("Pipeline error: {}", err);
/// ```
#[derive(Error, Debug, Clone)]
pub enum PipelineError {
    /// I/O operation error.
    #[error("I/O error: {0}")]
    IoError(String),
    /// Item processing error.
    #[error("Item processing error: {0}")]
    ItemError(String),
    /// Database operation error.
    #[error("Database error: {0}")]
    DatabaseError(String),
    /// Serialization error.
    #[error("Serialization error: {0}")]
    SerializationError(String),
    /// CSV operation error.
    #[error("CSV error: {0}")]
    CsvError(String),
    /// Other unspecified pipeline error.
    #[error("Other pipeline error: {0}")]
    Other(String),
}

impl From<csv::Error> for PipelineError {
    fn from(err: csv::Error) -> Self {
        PipelineError::CsvError(err.to_string())
    }
}

impl From<std::io::Error> for PipelineError {
    fn from(err: std::io::Error) -> Self {
        PipelineError::IoError(err.to_string())
    }
}

impl From<SerdeJsonError> for PipelineError {
    fn from(err: SerdeJsonError) -> Self {
        PipelineError::SerializationError(err.to_string())
    }
}

impl From<rusqlite::Error> for PipelineError {
    fn from(err: rusqlite::Error) -> Self {
        PipelineError::DatabaseError(err.to_string())
    }
}

impl From<rusqlite::Error> for SpiderError {
    fn from(err: rusqlite::Error) -> Self {
        SpiderError::PipelineError(PipelineError::DatabaseError(err.to_string()))
    }
}
