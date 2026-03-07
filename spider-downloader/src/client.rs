//! Reqwest-based Downloader implementation for the `spider-lib` framework.
//!
//! This module provides `ReqwestClientDownloader`, a concrete implementation
//! of the `Downloader` trait that leverages the `reqwest` HTTP client library.
//! It is responsible for executing HTTP requests defined by `Request` objects
//! and converting the received HTTP responses into `Response` objects suitable
//! for further processing by the crawler.
//!
//! This downloader handles various HTTP methods, request bodies (JSON, form data, bytes),
//! and integrates with the framework's error handling.

use crate::Downloader;
use async_trait::async_trait;
use log::{debug, warn};
use moka::sync::Cache;
use reqwest::{Client, Proxy};
use spider_util::error::SpiderError;
use spider_util::request::{Body, Request};
use spider_util::response::Response;
use std::time::Duration;

/// Concrete implementation of Downloader using reqwest client
pub struct ReqwestClientDownloader {
    client: Client,
    timeout: Duration,
    /// Per-proxy clients with TTL/capacity bounds to avoid unbounded growth.
    proxy_clients: Cache<String, Client>,
}

#[async_trait]
impl Downloader for ReqwestClientDownloader {
    type Client = Client;

    /// Returns a reference to the underlying HTTP client.
    fn client(&self) -> &Self::Client {
        &self.client
    }

    async fn download(&self, request: Request) -> Result<Response, SpiderError> {
        debug!(
            "Downloading {} (fingerprint: {})",
            request.url,
            request.fingerprint()
        );

        let url = request.url.clone();
        let body = request.body.clone();
        let client_to_use = self.select_client_for_request(&request);

        let mut req_builder = client_to_use.request(request.method.clone(), url.clone());

        if let Some(body_content) = body {
            req_builder = match body_content {
                Body::Json(json_val) => req_builder.json(&json_val),
                Body::Form(form_val) => {
                    let mut form_map = std::collections::HashMap::new();
                    for entry in form_val.iter() {
                        form_map.insert(entry.key().clone(), entry.value().clone());
                    }
                    req_builder.form(&form_map)
                }
                Body::Bytes(bytes_val) => req_builder.body(bytes_val),
            };
        }

        let res = req_builder.headers(request.headers.clone()).send().await?;

        let response_url = res.url().clone();
        let status = res.status();
        let response_headers = res.headers().clone();
        let response_body = res.bytes().await?;

        Ok(Response {
            url: response_url,
            status,
            headers: response_headers,
            body: response_body,
            request_url: url,
            meta: request.meta_inner().clone(),
            cached: false,
        })
    }
}

impl ReqwestClientDownloader {
    const PROXY_CLIENT_CACHE_MAX_CAPACITY: u64 = 512;
    const PROXY_CLIENT_CACHE_TTL_SECS: u64 = 30 * 60;
    const PROXY_META_KEY: &str = "proxy";

    /// Creates a new `ReqwestClientDownloader` with a default timeout of 30 seconds.
    pub fn new() -> Self {
        Self::new_with_timeout(Duration::from_secs(30))
    }

    /// Creates a new `ReqwestClientDownloader` with a specified request timeout.
    pub fn new_with_timeout(timeout: Duration) -> Self {
        match Self::try_new_with_timeout(timeout) {
            Ok(downloader) => downloader,
            Err(err) => panic!(
                "failed to create reqwest downloader with timeout {:?}: {}",
                timeout, err
            ),
        }
    }

    /// Tries to create a new `ReqwestClientDownloader` with a specified request timeout.
    pub fn try_new_with_timeout(timeout: Duration) -> Result<Self, SpiderError> {
        let base_client = Client::builder()
            .timeout(timeout)
            .pool_max_idle_per_host(200)
            .pool_idle_timeout(Duration::from_secs(120))
            .tcp_keepalive(Duration::from_secs(60))
            .connect_timeout(Duration::from_secs(10))
            .build()
            .map_err(|e| SpiderError::ReqwestError(e.into()))?;

        Ok(ReqwestClientDownloader {
            client: base_client.clone(),
            timeout,
            proxy_clients: Cache::builder()
                .max_capacity(Self::PROXY_CLIENT_CACHE_MAX_CAPACITY)
                .time_to_idle(Duration::from_secs(Self::PROXY_CLIENT_CACHE_TTL_SECS))
                .build(),
        })
    }

    fn proxy_from_request(request: &Request) -> Option<String> {
        request.meta_inner().as_ref().and_then(|meta_map| {
            meta_map
                .get(Self::PROXY_META_KEY)
                .and_then(|proxy_val| proxy_val.as_str().map(str::to_owned))
        })
    }

    fn select_client_for_request(&self, request: &Request) -> Client {
        if let Some(proxy_url) = Self::proxy_from_request(request)
            && let Some(proxy_client) = self.get_or_create_proxy_client(&proxy_url)
        {
            return proxy_client;
        }

        self.client.clone()
    }

    /// Gets or creates a proxy-specific client to preserve connection pooling per proxy endpoint.
    fn get_or_create_proxy_client(&self, proxy_url: &str) -> Option<Client> {
        if let Some(client) = self.proxy_clients.get(proxy_url) {
            return Some(client);
        }

        let proxy = match Proxy::all(proxy_url) {
            Ok(proxy) => proxy,
            Err(err) => {
                warn!(
                    "Invalid proxy URL '{}': {}. Falling back to base client",
                    proxy_url, err
                );
                return None;
            }
        };

        let proxy_client = match Client::builder()
            .timeout(self.timeout)
            .pool_max_idle_per_host(50)
            .pool_idle_timeout(Duration::from_secs(90))
            .tcp_keepalive(Duration::from_secs(30))
            .connect_timeout(Duration::from_secs(5))
            .proxy(proxy)
            .build()
        {
            Ok(client) => client,
            Err(err) => {
                warn!(
                    "Failed to build client for proxy '{}': {}. Falling back to base client",
                    proxy_url, err
                );
                return None;
            }
        };

        if let Some(client) = self.proxy_clients.get(proxy_url) {
            return Some(client);
        }
        self.proxy_clients
            .insert(proxy_url.to_string(), proxy_client.clone());
        Some(proxy_client)
    }
}

impl Default for ReqwestClientDownloader {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::ReqwestClientDownloader;
    use spider_util::request::Request;

    #[test]
    fn proxy_meta_parsing_returns_none_when_missing() {
        let request = Request::new(reqwest::Url::parse("https://example.com").expect("valid url"));
        assert_eq!(ReqwestClientDownloader::proxy_from_request(&request), None);
    }

    #[test]
    fn invalid_proxy_falls_back_without_error() {
        let downloader = ReqwestClientDownloader::new();
        let proxy_client = downloader.get_or_create_proxy_client("://invalid-proxy");
        assert!(proxy_client.is_none());
        assert_eq!(downloader.proxy_clients.entry_count(), 0);
    }

    #[test]
    fn valid_proxy_client_is_cached_and_reused() {
        let downloader = ReqwestClientDownloader::new();
        let proxy = "http://127.0.0.1:8080";

        let first = downloader.get_or_create_proxy_client(proxy);
        assert!(first.is_some());
        assert!(downloader.proxy_clients.get(proxy).is_some());

        let second = downloader.get_or_create_proxy_client(proxy);
        assert!(second.is_some());
        assert!(downloader.proxy_clients.get(proxy).is_some());
    }

    #[test]
    fn request_without_proxy_uses_base_client_path() {
        let downloader = ReqwestClientDownloader::new();
        let request = Request::new(reqwest::Url::parse("https://example.com").expect("valid url"));

        let _ = downloader.select_client_for_request(&request);
        assert_eq!(downloader.proxy_clients.entry_count(), 0);
    }
}
