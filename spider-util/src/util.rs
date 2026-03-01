//! Utility functions for the `spider-lib` framework.
//!
//! This module provides utility functions that are used across different
//! components of the framework.

use psl::{List, Psl};
use scraper::Selector;
use std::fs;
use std::path::Path;
use url::Url;

use crate::error::SpiderError;
use crate::request::Request;

/// Checks if two URLs belong to the same site.
pub fn is_same_site(a: &Url, b: &Url) -> bool {
    a.host_str().and_then(|h| List.domain(h.as_bytes()))
        == b.host_str().and_then(|h| List.domain(h.as_bytes()))
}

/// Normalizes the origin of a request's URL.
pub fn normalize_origin(request: &Request) -> String {
    let url = &request.url;
    let scheme = url.scheme();
    let host = url.host_str().unwrap_or("");
    let port = url.port_or_known_default().unwrap_or(0);

    format!("{scheme}://{host}:{port}")
}

/// Validates that the parent directory of a given file path exists, creating it if necessary.
pub fn validate_output_dir(file_path: impl AsRef<Path>) -> Result<(), SpiderError> {
    let Some(parent_dir) = file_path.as_ref().parent() else {
        return Ok(());
    };

    if !parent_dir.as_os_str().is_empty() && !parent_dir.exists() {
        fs::create_dir_all(parent_dir)?;
    }

    Ok(())
}

/// Creates a directory and all of its parent components if they are missing.
pub fn create_dir(dir_path: impl AsRef<Path>) -> Result<(), SpiderError> {
    fs::create_dir_all(dir_path)?;
    Ok(())
}

pub trait ToSelector {
    /// Parses a string slice into a `scraper::Selector`, returning a `SpiderError` on failure.
    fn to_selector(&self) -> Result<Selector, SpiderError>;
}

impl ToSelector for &str {
    fn to_selector(&self) -> Result<Selector, SpiderError> {
        Selector::parse(self).map_err(|e| SpiderError::HtmlParseError(e.to_string()))
    }
}

impl ToSelector for String {
    fn to_selector(&self) -> Result<Selector, SpiderError> {
        Selector::parse(self).map_err(|e| SpiderError::HtmlParseError(e.to_string()))
    }
}

