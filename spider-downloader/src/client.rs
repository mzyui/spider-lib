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
use dashmap::DashMap;
use log::debug;
use moka::sync::Cache;
use reqwest::{Client, Proxy};
use spider_util::error::SpiderError;
use spider_util::request::{Body, Request};
use spider_util::response::Response;
use std::sync::Arc;
use std::time::Duration;

/// Concrete implementation of Downloader using reqwest client
pub struct ReqwestClientDownloader {
    client: Client,
    timeout: Duration,
    /// Per-host connection pools for better resource management
    host_clients: Arc<DashMap<String, Client>>,
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

        // Get host-specific client if available, otherwise use default
        let host = url.host_str().unwrap_or("").to_string();
        let mut client_to_use = self.get_or_create_host_client(&host).await;

        // Check for proxy in metadata
        if let Some(meta_map) = request.meta_inner().as_ref()
            && let Some(proxy_val) = meta_map.get("proxy")
            && let Some(proxy_str) = proxy_val.as_str()
        {
            client_to_use = self.get_or_create_proxy_client(proxy_str).await?;
        }

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

    /// Creates a new `ReqwestClientDownloader` with a default timeout of 30 seconds.
    pub fn new() -> Self {
        Self::new_with_timeout(Duration::from_secs(30))
    }

    /// Creates a new `ReqwestClientDownloader` with a specified request timeout.
    pub fn new_with_timeout(timeout: Duration) -> Self {
        let base_client = Client::builder()
            .timeout(timeout)
            .pool_max_idle_per_host(200)
            .pool_idle_timeout(Duration::from_secs(120))
            .tcp_keepalive(Duration::from_secs(60))
            .connect_timeout(Duration::from_secs(10))
            .build()
            .unwrap();

        ReqwestClientDownloader {
            client: base_client.clone(),
            timeout,
            host_clients: Arc::new(DashMap::new()),
            proxy_clients: Cache::builder()
                .max_capacity(Self::PROXY_CLIENT_CACHE_MAX_CAPACITY)
                .time_to_idle(Duration::from_secs(Self::PROXY_CLIENT_CACHE_TTL_SECS))
                .build(),
        }
    }

    /// Gets or creates a host-specific client with optimized settings for that host
    async fn get_or_create_host_client(&self, host: &str) -> Client {
        if let Some(client) = self.host_clients.get(host) {
            return client.clone();
        }

        // Create a new client for this host with optimized settings
        let host_specific_client = Client::builder()
            .timeout(self.timeout)
            .pool_max_idle_per_host(50) // Smaller pool per host to distribute connections
            .pool_idle_timeout(Duration::from_secs(90))
            .tcp_keepalive(Duration::from_secs(30))
            .connect_timeout(Duration::from_secs(5))
            .build()
            .unwrap();

        if let Some(existing) = self.host_clients.get(host) {
            return existing.clone();
        }
        self.host_clients
            .insert(host.to_string(), host_specific_client.clone());

        host_specific_client
    }

    /// Gets or creates a proxy-specific client to preserve connection pooling per proxy endpoint.
    async fn get_or_create_proxy_client(&self, proxy_url: &str) -> Result<Client, SpiderError> {
        if let Some(client) = self.proxy_clients.get(proxy_url) {
            return Ok(client);
        }

        let proxy = Proxy::all(proxy_url).map_err(|e| SpiderError::ReqwestError(e.into()))?;
        let proxy_client = Client::builder()
            .timeout(self.timeout)
            .pool_max_idle_per_host(50)
            .pool_idle_timeout(Duration::from_secs(90))
            .tcp_keepalive(Duration::from_secs(30))
            .connect_timeout(Duration::from_secs(5))
            .proxy(proxy)
            .build()
            .map_err(|e| SpiderError::ReqwestError(e.into()))?;

        if let Some(client) = self.proxy_clients.get(proxy_url) {
            return Ok(client);
        }
        self.proxy_clients
            .insert(proxy_url.to_string(), proxy_client.clone());
        Ok(proxy_client)
    }
}

impl Default for ReqwestClientDownloader {
    fn default() -> Self {
        Self::new()
    }
}
