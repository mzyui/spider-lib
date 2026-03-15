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
use log::{Level, debug, log_enabled, warn};
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
        if log_enabled!(Level::Debug) {
            debug!(
                "Downloading {} (fingerprint: {})",
                request.url,
                request.fingerprint()
            );
        }

        let client_to_use = self.select_client_for_request(&request);
        let mut request = request;
        let meta = request.take_meta();
        let Request {
            url: request_url,
            method,
            headers,
            body,
            ..
        } = request;

        let mut req_builder = client_to_use.request(method, request_url.clone());

        if let Some(body_content) = body {
            req_builder = match body_content {
                Body::Json(json_val) => req_builder.json(&json_val),
                Body::Form(form_val) => req_builder.form(&Self::form_pairs(&form_val)),
                Body::Bytes(bytes_val) => req_builder.body(bytes_val),
            };
        }

        let res = req_builder.headers(headers).send().await?;

        let response_url = res.url().clone();
        let status = res.status();
        let response_headers = res.headers().clone();
        let response_body = res.bytes().await?;

        Ok(Response {
            url: response_url,
            status,
            headers: response_headers,
            body: response_body,
            request_url,
            meta,
            cached: false,
        })
    }
}

impl ReqwestClientDownloader {
    const PROXY_CLIENT_CACHE_MAX_CAPACITY: u64 = 512;
    const PROXY_CLIENT_CACHE_TTL_SECS: u64 = 30 * 60;
    const PROXY_META_KEY: &str = "proxy";
    const DEFAULT_USER_AGENT: &'static str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36";

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
        let base_client = Self::build_client(
            timeout,
            None,
            200,
            Duration::from_secs(120),
            Duration::from_secs(60),
            Duration::from_secs(10),
        )?;

        Ok(ReqwestClientDownloader {
            client: base_client,
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

    fn form_pairs(form: &dashmap::DashMap<String, String>) -> Vec<(String, String)> {
        let mut pairs = Vec::with_capacity(form.len());
        for entry in form.iter() {
            pairs.push((entry.key().clone(), entry.value().clone()));
        }
        pairs
    }

    fn build_client(
        timeout: Duration,
        proxy: Option<Proxy>,
        pool_max_idle_per_host: usize,
        pool_idle_timeout: Duration,
        tcp_keepalive: Duration,
        connect_timeout: Duration,
    ) -> Result<Client, SpiderError> {
        let mut builder = Client::builder()
            .timeout(timeout)
            .pool_max_idle_per_host(pool_max_idle_per_host)
            .pool_idle_timeout(pool_idle_timeout)
            .tcp_keepalive(tcp_keepalive)
            .connect_timeout(connect_timeout)
            .user_agent(Self::DEFAULT_USER_AGENT);

        if let Some(proxy) = proxy {
            builder = builder.proxy(proxy);
        }

        builder
            .build()
            .map_err(|err| SpiderError::ReqwestError(err.into()))
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

        let proxy_client = match Self::build_client(
            self.timeout,
            Some(proxy),
            50,
            Duration::from_secs(90),
            Duration::from_secs(30),
            Duration::from_secs(5),
        ) {
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

    #[cfg(feature = "test-support")]
    pub fn test_proxy_from_request(request: &Request) -> Option<String> {
        Self::proxy_from_request(request)
    }

    #[cfg(feature = "test-support")]
    pub fn test_select_client_for_request(&self, request: &Request) -> Client {
        self.select_client_for_request(request)
    }

    #[cfg(feature = "test-support")]
    pub fn test_get_or_create_proxy_client(&self, proxy_url: &str) -> Option<Client> {
        self.get_or_create_proxy_client(proxy_url)
    }

    #[cfg(feature = "test-support")]
    pub fn test_proxy_client_count(&self) -> u64 {
        self.proxy_clients.entry_count()
    }

    #[cfg(feature = "test-support")]
    pub fn test_has_proxy_client(&self, proxy_url: &str) -> bool {
        self.proxy_clients.get(proxy_url).is_some()
    }
}

impl Default for ReqwestClientDownloader {
    fn default() -> Self {
        Self::new()
    }
}
