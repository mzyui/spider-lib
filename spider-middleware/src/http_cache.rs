//! On-disk HTTP response cache middleware.
//!
//! This middleware stores successful responses by request fingerprint and can
//! short-circuit later requests by returning cached responses directly.
//! Cache freshness is evaluated per response from HTTP caching headers.

use async_trait::async_trait;
use bytes::Bytes;
use log::{debug, info, trace, warn};
use reqwest::StatusCode;
use reqwest::header::{CACHE_CONTROL, EXPIRES, HeaderMap, HeaderName, HeaderValue};
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc2822;
use tokio::fs;

use crate::middleware::{Middleware, MiddlewareAction};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use spider_util::error::SpiderError;
use spider_util::request::Request;
use spider_util::response::Response;
use url::Url;

fn serialize_headermap<S>(headers: &HeaderMap, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let mut map = std::collections::HashMap::<String, String>::new();
    for (name, value) in headers.iter() {
        map.insert(
            name.to_string(),
            value.to_str().unwrap_or_default().to_string(),
        );
    }
    map.serialize(serializer)
}

fn deserialize_headermap<'de, D>(deserializer: D) -> Result<HeaderMap, D::Error>
where
    D: Deserializer<'de>,
{
    let map = std::collections::HashMap::<String, String>::deserialize(deserializer)?;
    let mut headers = HeaderMap::new();
    for (name, value) in map {
        if let (Ok(header_name), Ok(header_value)) =
            (name.parse::<HeaderName>(), value.parse::<HeaderValue>())
        {
            headers.insert(header_name, header_value);
        } else {
            warn!("Failed to parse header: {} = {}", name, value);
        }
    }
    Ok(headers)
}

fn serialize_statuscode<S>(status: &StatusCode, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    status.as_u16().serialize(serializer)
}

fn deserialize_statuscode<'de, D>(deserializer: D) -> Result<StatusCode, D::Error>
where
    D: Deserializer<'de>,
{
    let status_u16 = u16::deserialize(deserializer)?;
    StatusCode::from_u16(status_u16).map_err(serde::de::Error::custom)
}

fn serialize_url<S>(url: &Url, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    url.to_string().serialize(serializer)
}

fn deserialize_url<'de, D>(deserializer: D) -> Result<Url, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    Url::parse(&s).map_err(serde::de::Error::custom)
}

/// Serialized response data used for cache storage.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CachedResponse {
    #[serde(serialize_with = "serialize_url", deserialize_with = "deserialize_url")]
    url: Url,
    #[serde(
        serialize_with = "serialize_statuscode",
        deserialize_with = "deserialize_statuscode"
    )]
    status: StatusCode,
    #[serde(
        serialize_with = "serialize_headermap",
        deserialize_with = "deserialize_headermap"
    )]
    headers: HeaderMap,
    body: Vec<u8>,
    #[serde(serialize_with = "serialize_url", deserialize_with = "deserialize_url")]
    request_url: Url,
    #[serde(default)]
    cached_at_unix_secs: u64,
    #[serde(default)]
    expires_at_unix_secs: Option<u64>,
}

impl From<Response> for CachedResponse {
    fn from(response: Response) -> Self {
        CachedResponse {
            url: response.url,
            status: response.status,
            headers: response.headers,
            body: response.body.to_vec(),
            request_url: response.request_url,
            cached_at_unix_secs: now_unix_secs(),
            expires_at_unix_secs: None,
        }
    }
}

impl From<CachedResponse> for Response {
    fn from(cached_response: CachedResponse) -> Self {
        Response {
            url: cached_response.url,
            status: cached_response.status,
            headers: cached_response.headers,
            body: Bytes::from(cached_response.body),
            request_url: cached_response.request_url,
            meta: Default::default(),
            cached: true,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CachePolicy {
    DoNotStore,
    Store { expires_at_unix_secs: Option<u64> },
}

impl CachedResponse {
    fn is_fresh_at(&self, now_unix_secs: u64) -> bool {
        match self.expires_at_unix_secs {
            Some(expires_at_unix_secs) => now_unix_secs < expires_at_unix_secs,
            None => true,
        }
    }
}

/// Builder for [`HttpCacheMiddleware`].
#[derive(Default)]
pub struct HttpCacheMiddlewareBuilder {
    cache_dir: Option<PathBuf>,
}

impl HttpCacheMiddlewareBuilder {
    /// Sets the directory where cache files will be stored.
    pub fn cache_dir(mut self, path: PathBuf) -> Self {
        self.cache_dir = Some(path);
        self
    }

    /// Builds the `HttpCacheMiddleware`.
    ///
    /// # Errors
    ///
    /// Returns an error if the cache directory cannot be resolved or created.
    pub fn build(self) -> Result<HttpCacheMiddleware, SpiderError> {
        let cache_dir = if let Some(path) = self.cache_dir {
            path
        } else {
            dirs::cache_dir()
                .ok_or_else(|| {
                    SpiderError::ConfigurationError(
                        "Could not determine cache directory".to_string(),
                    )
                })?
                .join("spider-lib")
                .join("http_cache")
        };

        std::fs::create_dir_all(&cache_dir)?;

        let middleware = HttpCacheMiddleware { cache_dir };
        info!(
            "Initializing HttpCacheMiddleware with config: {:?}",
            middleware
        );

        Ok(middleware)
    }
}

#[derive(Debug)]
/// Middleware that caches successful HTTP responses on disk.
pub struct HttpCacheMiddleware {
    cache_dir: PathBuf,
}

impl HttpCacheMiddleware {
    /// Creates a new `HttpCacheMiddlewareBuilder` to start building an `HttpCacheMiddleware`.
    pub fn builder() -> HttpCacheMiddlewareBuilder {
        HttpCacheMiddlewareBuilder::default()
    }

    fn get_cache_file_path(&self, fingerprint: &str) -> PathBuf {
        self.cache_dir.join(format!("{}.bin", fingerprint))
    }
}

fn now_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_secs()
}

fn parse_cache_policy(headers: &HeaderMap, cached_at_unix_secs: u64) -> CachePolicy {
    if let Some(policy) = parse_cache_control(headers, cached_at_unix_secs) {
        return policy;
    }

    CachePolicy::Store {
        expires_at_unix_secs: parse_expires(headers),
    }
}

fn parse_cache_control(headers: &HeaderMap, cached_at_unix_secs: u64) -> Option<CachePolicy> {
    let cache_control = headers.get(CACHE_CONTROL)?.to_str().ok()?;
    let mut max_age_secs = None;

    for directive in cache_control.split(',') {
        let directive = directive.trim();
        if directive.eq_ignore_ascii_case("no-store") {
            return Some(CachePolicy::DoNotStore);
        }

        let Some((name, value)) = directive.split_once('=') else {
            continue;
        };

        if !name.trim().eq_ignore_ascii_case("max-age") {
            continue;
        }

        let value = value.trim().trim_matches('"');
        if let Ok(parsed) = value.parse::<u64>() {
            max_age_secs = Some(parsed);
        }
    }

    max_age_secs.map(|max_age_secs| CachePolicy::Store {
        expires_at_unix_secs: cached_at_unix_secs.checked_add(max_age_secs),
    })
}

fn parse_expires(headers: &HeaderMap) -> Option<u64> {
    let expires = headers.get(EXPIRES)?.to_str().ok()?;
    let parsed = OffsetDateTime::parse(expires, &Rfc2822).ok()?;
    u64::try_from(parsed.unix_timestamp()).ok()
}

#[async_trait]
impl<C: Send + Sync> Middleware<C> for HttpCacheMiddleware {
    fn name(&self) -> &str {
        "HttpCacheMiddleware"
    }

    async fn process_request(
        &self,
        _client: &C,
        request: Request,
    ) -> Result<MiddlewareAction<Request>, SpiderError> {
        let fingerprint = request.fingerprint();
        let cache_file_path = self.get_cache_file_path(&fingerprint);

        trace!(
            "Checking cache for request: {} (fingerprint: {})",
            request.url, fingerprint
        );
        if fs::metadata(&cache_file_path).await.is_ok() {
            debug!("Cache hit for request: {}", request.url);
            match fs::read(&cache_file_path).await {
                Ok(cached_bytes) => match bincode::deserialize::<CachedResponse>(&cached_bytes) {
                    Ok(cached_resp) => {
                        let now_unix_secs = now_unix_secs();
                        if !cached_resp.is_fresh_at(now_unix_secs) {
                            debug!(
                                "Cached response expired for {} at {:?}, refreshing from network",
                                request.url, cached_resp.expires_at_unix_secs
                            );
                            return Ok(MiddlewareAction::Continue(request));
                        }

                        trace!(
                            "Successfully deserialized cached response for {}",
                            request.url
                        );
                        let mut response: Response = cached_resp.into();
                        response.meta = request.clone_meta();
                        debug!("Returning cached response for {}", response.url);
                        return Ok(MiddlewareAction::ReturnResponse(response));
                    }
                    Err(e) => {
                        warn!(
                            "Failed to deserialize cached response from {}: {}. Deleting invalid cache file.",
                            cache_file_path.display(),
                            e
                        );
                        fs::remove_file(&cache_file_path).await.ok();
                    }
                },
                Err(e) => {
                    warn!(
                        "Failed to read cache file {}: {}. Deleting invalid cache file.",
                        cache_file_path.display(),
                        e
                    );
                    fs::remove_file(&cache_file_path).await.ok();
                }
            }
        } else {
            trace!(
                "Cache miss for request: {} (no cache file found)",
                request.url
            );
        }

        trace!("Continuing request to downloader: {}", request.url);
        Ok(MiddlewareAction::Continue(request))
    }

    async fn process_response(
        &self,
        response: Response,
    ) -> Result<MiddlewareAction<Response>, SpiderError> {
        trace!(
            "Processing response for caching: {} with status: {}",
            response.url, response.status
        );

        // Only cache successful responses (e.g., 200 OK)
        if response.status.is_success() {
            let original_request_fingerprint = response.request_from_response().fingerprint();
            let cache_file_path = self.get_cache_file_path(&original_request_fingerprint);
            let cached_at_unix_secs = now_unix_secs();
            let cache_policy = parse_cache_policy(&response.headers, cached_at_unix_secs);

            if matches!(cache_policy, CachePolicy::DoNotStore) {
                debug!(
                    "Skipping cache storage for {} due to Cache-Control: no-store",
                    response.url
                );
                return Ok(MiddlewareAction::Continue(response));
            }

            trace!(
                "Serializing response for caching to: {}",
                cache_file_path.display()
            );
            let mut cached_response: CachedResponse = response.clone().into();
            cached_response.cached_at_unix_secs = cached_at_unix_secs;
            cached_response.expires_at_unix_secs = match cache_policy {
                CachePolicy::Store {
                    expires_at_unix_secs,
                } => expires_at_unix_secs,
                CachePolicy::DoNotStore => None,
            };
            match bincode::serialize(&cached_response) {
                Ok(serialized_bytes) => {
                    let bytes_count = serialized_bytes.len();
                    trace!(
                        "Writing {} bytes to cache file: {}",
                        bytes_count,
                        cache_file_path.display()
                    );
                    fs::write(&cache_file_path, serialized_bytes)
                        .await
                        .map_err(|e| SpiderError::IoError(e.to_string()))?;
                    debug!(
                        "Cached response for {} ({} bytes)",
                        response.url, bytes_count
                    );
                }
                Err(e) => {
                    warn!(
                        "Failed to serialize response for caching {}: {}",
                        response.url, e
                    );
                }
            }
        } else {
            trace!(
                "Response status {} is not successful, skipping cache for: {}",
                response.status, response.url
            );
        }

        trace!("Continuing response: {}", response.url);
        Ok(MiddlewareAction::Continue(response))
    }
}
