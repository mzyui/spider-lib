//! HTTP Client trait for fetching web content.
//!
//! This module provides the `HttpClient` trait, which is a simple abstraction
//! for HTTP clients used throughout the spider framework.

use async_trait::async_trait;
use bytes::Bytes;
use http::StatusCode;
use std::time::Duration;

use crate::error::SpiderError;

/// A simple HTTP client trait for fetching web content.
#[async_trait]
pub trait HttpClient: Send + Sync {
    /// Fetches the content of a URL as text.
    ///
    /// # Errors
    ///
    /// Returns an error when the request fails, times out, or the response body
    /// cannot be read.
    async fn get_text(
        &self,
        url: &str,
        timeout: Duration,
    ) -> Result<(StatusCode, Bytes), SpiderError>;
}

#[async_trait]
impl HttpClient for reqwest::Client {
    async fn get_text(
        &self,
        url: &str,
        timeout: Duration,
    ) -> Result<(StatusCode, Bytes), SpiderError> {
        let resp = self.get(url).timeout(timeout).send().await?;
        let status = resp.status();
        let body = resp.bytes().await?;
        Ok((status, body))
    }
}
