//! Request types used by the crawler runtime.
//!
//! [`Request`] is the runtime's transport-neutral request model. It stores the
//! URL, method, headers, optional body, and a lazily allocated metadata map used
//! by middleware and runtime internals.
//!
//! ## Example
//!
//! ```rust,ignore
//! use spider_util::request::{Request, Body};
//! use url::Url;
//! use serde_json::json;
//!
//! // Create a simple GET request
//! let url = Url::parse("https://example.com").unwrap();
//! let request = Request::new(url);
//!
//! // Create a POST request with JSON body
//! let post_request = Request::new(Url::parse("https://api.example.com/data").unwrap())
//!     .with_method(reqwest::Method::POST)
//!     .with_json(json!({"key": "value"}));
//! ```

use bytes::Bytes;
use dashmap::DashMap;
use http::header::HeaderMap;
use reqwest::{Method, Url};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::hash::Hasher;
use std::str::FromStr;
use std::sync::Arc;
use twox_hash::XxHash64;

use crate::error::SpiderError;

/// Request body variants supported by the default downloader.
///
/// ## Example
///
/// ```rust,ignore
/// use spider_util::request::Body;
/// use serde_json::json;
/// use dashmap::DashMap;
/// use bytes::Bytes;
///
/// // JSON body
/// let json_body = Body::Json(json!({"name": "test"}));
///
/// // Form data
/// let mut form = DashMap::new();
/// form.insert("key".to_string(), "value".to_string());
/// let form_body = Body::Form(form);
///
/// // Raw bytes
/// let bytes_body = Body::Bytes(Bytes::from("raw data"));
/// ```
#[derive(Debug, Clone)]
pub enum Body {
    /// JSON payload.
    Json(serde_json::Value),
    /// Form data (key-value pairs).
    Form(DashMap<String, String>),
    /// Raw binary data.
    Bytes(Bytes),
}

// Custom serialization for Body enum
impl Serialize for Body {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(Some(1))?;

        match self {
            Body::Json(value) => map.serialize_entry("Json", value)?,
            Body::Form(dashmap) => {
                let hmap: HashMap<String, String> = dashmap.clone().into_iter().collect();
                map.serialize_entry("Form", &hmap)?
            }
            Body::Bytes(bytes) => map.serialize_entry("Bytes", bytes)?,
        }

        map.end()
    }
}

impl<'de> Deserialize<'de> for Body {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::{self, MapAccess, Visitor};
        use std::fmt;

        struct BodyVisitor;

        impl<'de> Visitor<'de> for BodyVisitor {
            type Value = Body;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a body object")
            }

            fn visit_map<V>(self, mut map: V) -> Result<Body, V::Error>
            where
                V: MapAccess<'de>,
            {
                let entry = map.next_entry::<String, Value>()?;
                let (key, value) = match entry {
                    Some((k, v)) => (k, v),
                    None => return Err(de::Error::custom("Expected a body variant")),
                };

                match key.as_str() {
                    "Json" => Ok(Body::Json(value)),
                    "Form" => {
                        let form_data: HashMap<String, String> =
                            serde_json::from_value(value).map_err(de::Error::custom)?;
                        let dashmap = DashMap::new();
                        for (k, v) in form_data {
                            dashmap.insert(k, v);
                        }
                        Ok(Body::Form(dashmap))
                    }
                    "Bytes" => {
                        let bytes: Bytes =
                            serde_json::from_value(value).map_err(de::Error::custom)?;
                        Ok(Body::Bytes(bytes))
                    }
                    _ => Err(de::Error::custom(format!("Unknown body variant: {}", key))),
                }
            }
        }

        deserializer.deserialize_map(BodyVisitor)
    }
}

/// Outgoing HTTP request used by the crawler runtime.
///
/// [`Request`] is the handoff type between spiders, middleware, the scheduler,
/// and the downloader. It is transport-neutral enough to be shared across the
/// workspace, but expressive enough for custom methods, headers, bodies, and
/// request-scoped metadata.
///
/// ## Example
///
/// ```rust,ignore
/// use spider_util::request::Request;
/// use url::Url;
///
/// // Create a basic GET request
/// let request = Request::new(Url::parse("https://example.com").unwrap());
///
/// // Build a request with headers and method
/// let post_request = Request::new(Url::parse("https://api.example.com").unwrap())
///     .with_method(reqwest::Method::POST)
///     .with_header("Accept", "application/json")
///     .unwrap();
/// ```
#[derive(Debug, Clone)]
pub struct Request {
    /// The target URL for this request.
    pub url: Url,
    /// The HTTP method (GET, POST, etc.).
    pub method: reqwest::Method,
    /// HTTP headers for the request.
    pub headers: http::header::HeaderMap,
    /// Optional request body.
    pub body: Option<Body>,
    /// Lazy-initialized metadata - only allocated when actually used.
    /// This reduces memory allocation for simple requests without metadata.
    meta: Option<Arc<DashMap<String, Value>>>,
}

// Custom serialization for Request struct
impl Serialize for Request {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        // Convert HeaderMap to a serializable format
        let headers_vec: Vec<(String, String)> = self
            .headers
            .iter()
            .filter_map(|(name, value)| {
                value
                    .to_str()
                    .ok()
                    .map(|val_str| (name.as_str().to_string(), val_str.to_string()))
            })
            .collect();

        let mut s = serializer.serialize_struct("Request", 5)?;
        s.serialize_field("url", &self.url.as_str())?;
        s.serialize_field("method", &self.method.as_str())?;
        s.serialize_field("headers", &headers_vec)?;
        s.serialize_field("body", &self.body)?;
        // Serialize meta as empty HashMap if None (for backward compatibility)
        let meta_map: HashMap<String, Value> = self
            .meta
            .as_ref()
            .map(|m| {
                m.iter()
                    .map(|e| (e.key().clone(), e.value().clone()))
                    .collect()
            })
            .unwrap_or_default();
        s.serialize_field("meta", &meta_map)?;
        s.end()
    }
}

impl<'de> Deserialize<'de> for Request {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::{self, MapAccess, Visitor};
        use std::fmt;

        #[derive(Deserialize)]
        #[serde(field_identifier, rename_all = "lowercase")]
        enum Field {
            Url,
            Method,
            Headers,
            Body,
            Meta,
        }

        struct RequestVisitor;

        impl<'de> Visitor<'de> for RequestVisitor {
            type Value = Request;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("struct Request")
            }

            fn visit_map<V>(self, mut map: V) -> Result<Request, V::Error>
            where
                V: MapAccess<'de>,
            {
                let mut url = None;
                let mut method = None;
                let mut headers = None;
                let mut body = None;
                let mut meta = None;

                while let Some(key) = map.next_key()? {
                    match key {
                        Field::Url => {
                            if url.is_some() {
                                return Err(de::Error::duplicate_field("url"));
                            }
                            let url_str: String = map.next_value()?;
                            let parsed_url = Url::parse(&url_str).map_err(de::Error::custom)?;
                            url = Some(parsed_url);
                        }
                        Field::Method => {
                            if method.is_some() {
                                return Err(de::Error::duplicate_field("method"));
                            }
                            let method_str: String = map.next_value()?;
                            let parsed_method =
                                Method::from_str(&method_str).map_err(de::Error::custom)?;
                            method = Some(parsed_method);
                        }
                        Field::Headers => {
                            if headers.is_some() {
                                return Err(de::Error::duplicate_field("headers"));
                            }
                            // Deserialize headers vector and convert back to HeaderMap
                            let headers_vec: Vec<(String, String)> = map.next_value()?;
                            let mut header_map = HeaderMap::new();
                            for (name, value) in headers_vec {
                                if let Ok(header_name) =
                                    http::header::HeaderName::from_bytes(name.as_bytes())
                                    && let Ok(header_value) =
                                        http::header::HeaderValue::from_str(&value)
                                {
                                    header_map.insert(header_name, header_value);
                                }
                            }
                            headers = Some(header_map);
                        }
                        Field::Body => {
                            if body.is_some() {
                                return Err(de::Error::duplicate_field("body"));
                            }
                            body = Some(map.next_value()?);
                        }
                        Field::Meta => {
                            // Deserialize meta HashMap and convert to DashMap
                            let meta_map: HashMap<String, Value> = map.next_value()?;
                            if !meta_map.is_empty() {
                                let dashmap = DashMap::new();
                                for (k, v) in meta_map {
                                    dashmap.insert(k, v);
                                }
                                meta = Some(Arc::new(dashmap));
                            }
                        }
                    }
                }

                let url = url.ok_or_else(|| de::Error::missing_field("url"))?;
                let method = method.ok_or_else(|| de::Error::missing_field("method"))?;
                let headers = headers.ok_or_else(|| de::Error::missing_field("headers"))?;
                let body = body; // Optional field

                Ok(Request {
                    url,
                    method,
                    headers,
                    body,
                    meta, // May be None if no meta was serialized
                })
            }
        }

        const FIELDS: &[&str] = &["url", "method", "headers", "body", "meta"];
        deserializer.deserialize_struct("Request", FIELDS, RequestVisitor)
    }
}

impl Default for Request {
    fn default() -> Self {
        let default_url = match Url::parse("http://default.invalid") {
            Ok(url) => url,
            Err(err) => panic!("invalid hardcoded default URL: {}", err),
        };
        Self {
            url: default_url,
            method: reqwest::Method::GET,
            headers: http::header::HeaderMap::new(),
            body: None,
            meta: None, // Lazy initialization - no allocation until needed
        }
    }
}

impl Request {
    /// Creates a new [`Request`] with the given URL.
    ///
    /// This is the most common constructor used by spiders when enqueueing
    /// follow-up pages. It does not allocate metadata storage unless
    /// [`with_meta`](Request::with_meta) is called.
    ///
    /// ## Example
    ///
    /// ```rust,ignore
    /// use spider_util::request::Request;
    /// use url::Url;
    ///
    /// let request = Request::new(Url::parse("https://example.com").unwrap());
    /// ```
    pub fn new(url: Url) -> Self {
        Request {
            url,
            method: reqwest::Method::GET,
            headers: http::header::HeaderMap::new(),
            body: None,
            meta: None,
        }
    }

    /// Sets the HTTP method for the request.
    ///
    /// Use this together with one of the body helpers for POST, PUT, or PATCH
    /// workflows.
    ///
    /// ## Example
    ///
    /// ```rust,ignore
    /// use spider_util::request::Request;
    /// use url::Url;
    ///
    /// let request = Request::new(Url::parse("https://example.com").unwrap())
    ///     .with_method(reqwest::Method::POST);
    /// ```
    pub fn with_method(mut self, method: reqwest::Method) -> Self {
        self.method = method;
        self
    }

    /// Adds a header to the request.
    ///
    /// Returns an error if the header name or value is invalid.
    ///
    /// # Errors
    ///
    /// Returns a [`SpiderError::HeaderValueError`] if the header name or value is invalid.
    ///
    /// ## Example
    ///
    /// ```rust,ignore
    /// use spider_util::request::Request;
    /// use url::Url;
    ///
    /// let request = Request::new(Url::parse("https://example.com").unwrap())
    ///     .with_header("Accept", "application/json")
    ///     .unwrap();
    /// ```
    pub fn with_header(mut self, name: &str, value: &str) -> Result<Self, SpiderError> {
        let header_name =
            reqwest::header::HeaderName::from_bytes(name.as_bytes()).map_err(|e| {
                SpiderError::HeaderValueError(format!("Invalid header name '{}': {}", name, e))
            })?;
        let header_value = reqwest::header::HeaderValue::from_str(value).map_err(|e| {
            SpiderError::HeaderValueError(format!("Invalid header value '{}': {}", value, e))
        })?;

        self.headers.insert(header_name, header_value);
        Ok(self)
    }

    /// Sets the body of the request and defaults the method to POST.
    ///
    /// ## Example
    ///
    /// ```rust,ignore
    /// use spider_util::request::{Request, Body};
    /// use url::Url;
    /// use serde_json::json;
    ///
    /// let request = Request::new(Url::parse("https://api.example.com").unwrap())
    ///     .with_body(Body::Json(json!({"key": "value"})));
    /// ```
    pub fn with_body(mut self, body: Body) -> Self {
        self.body = Some(body);
        self.with_method(reqwest::Method::POST)
    }

    /// Sets the body of the request to a JSON value and defaults the method to POST.
    ///
    /// This helper stores the payload body only. Add content-type headers
    /// explicitly when the target service expects them.
    ///
    /// ## Example
    ///
    /// ```rust,ignore
    /// use spider_util::request::Request;
    /// use url::Url;
    /// use serde_json::json;
    ///
    /// let request = Request::new(Url::parse("https://api.example.com").unwrap())
    ///     .with_json(json!({"name": "test"}));
    /// ```
    pub fn with_json(self, json: serde_json::Value) -> Self {
        self.with_body(Body::Json(json))
    }

    /// Sets the body of the request to form data and defaults the method to POST.
    ///
    /// ## Example
    ///
    /// ```rust,ignore
    /// use spider_util::request::Request;
    /// use url::Url;
    /// use dashmap::DashMap;
    ///
    /// let mut form = DashMap::new();
    /// form.insert("key".to_string(), "value".to_string());
    ///
    /// let request = Request::new(Url::parse("https://api.example.com").unwrap())
    ///     .with_form(form);
    /// ```
    pub fn with_form(self, form: DashMap<String, String>) -> Self {
        self.with_body(Body::Form(form))
    }

    /// Sets the body of the request to raw bytes and defaults the method to POST.
    ///
    /// ## Example
    ///
    /// ```rust,ignore
    /// use spider_util::request::Request;
    /// use url::Url;
    /// use bytes::Bytes;
    ///
    /// let data = Bytes::from("binary data");
    /// let request = Request::new(Url::parse("https://api.example.com").unwrap())
    ///     .with_bytes(data);
    /// ```
    pub fn with_bytes(self, bytes: bytes::Bytes) -> Self {
        self.with_body(Body::Bytes(bytes))
    }

    /// Adds a value to the request's metadata.
    ///
    /// Lazily allocates the metadata map on first use. Metadata is commonly
    /// used to carry crawl context such as pagination state, source URLs, or
    /// retry bookkeeping across middleware and parsing stages.
    ///
    /// ## Example
    ///
    /// ```rust,ignore
    /// use spider_util::request::Request;
    /// use url::Url;
    /// use serde_json::json;
    ///
    /// let request = Request::new(Url::parse("https://example.com").unwrap())
    ///     .with_meta("priority", json!(1))
    ///     .with_meta("source", json!("manual"));
    /// ```
    pub fn with_meta(mut self, key: &str, value: serde_json::Value) -> Self {
        self.meta
            .get_or_insert_with(|| Arc::new(DashMap::new()))
            .insert(key.to_string(), value);
        self
    }

    /// Gets a reference to a metadata value, if it exists.
    ///
    /// Returns a cloned JSON value because metadata is stored in a shared
    /// concurrent map. Returns `None` if the key doesn't exist or if metadata
    /// hasn't been set.
    pub fn get_meta(&self, key: &str) -> Option<serde_json::Value> {
        self.meta
            .as_ref()
            .and_then(|m| m.get(key).map(|e| e.value().clone()))
    }

    /// Returns `true` if the request has metadata.
    pub fn has_meta(&self) -> bool {
        self.meta.as_ref().is_some_and(|m| !m.is_empty())
    }

    /// Returns a reference to the internal metadata map, if it exists.
    pub fn meta_map(&self) -> Option<&Arc<DashMap<String, serde_json::Value>>> {
        self.meta.as_ref()
    }

    /// Inserts a value into metadata, creating the map if needed.
    ///
    /// This is intended for internal framework use.
    pub fn insert_meta(&mut self, key: String, value: serde_json::Value) {
        self.meta
            .get_or_insert_with(|| Arc::new(DashMap::new()))
            .insert(key, value);
    }

    /// Gets a value from metadata using DashMap's API.
    ///
    /// This is intended for internal framework use where direct access is needed.
    pub fn get_meta_ref(
        &self,
        key: &str,
    ) -> Option<dashmap::mapref::one::Ref<'_, String, serde_json::Value>> {
        self.meta.as_ref().and_then(|m| m.get(key))
    }

    /// Sets the metadata map directly.
    ///
    /// Used for internal framework operations.
    pub fn set_meta_from_option(&mut self, meta: Option<Arc<DashMap<String, serde_json::Value>>>) {
        self.meta = meta;
    }

    /// Clones the metadata map.
    ///
    /// Used for internal framework operations where metadata needs to be copied.
    pub fn clone_meta(&self) -> Option<Arc<DashMap<String, serde_json::Value>>> {
        self.meta.clone()
    }

    /// Takes the metadata map, leaving `None` in its place.
    ///
    /// Used for internal framework operations.
    pub fn take_meta(&mut self) -> Option<Arc<DashMap<String, serde_json::Value>>> {
        self.meta.take()
    }

    /// Returns a reference to the metadata Arc for internal framework use.
    pub fn meta_inner(&self) -> &Option<Arc<DashMap<String, serde_json::Value>>> {
        &self.meta
    }

    const RETRY_ATTEMPTS_KEY: &str = "retry_attempts";

    /// Gets the number of times the request has been retried.
    ///
    /// Returns `0` if no retry attempts have been recorded.
    pub fn get_retry_attempts(&self) -> u32 {
        self.meta
            .as_ref()
            .and_then(|m| m.get(Self::RETRY_ATTEMPTS_KEY))
            .and_then(|v| v.value().as_u64())
            .unwrap_or(0) as u32
    }

    /// Increments the retry count for the request.
    ///
    /// Lazily allocates the metadata map if not already present.
    pub fn increment_retry_attempts(&mut self) {
        let current_attempts = self.get_retry_attempts();
        self.meta
            .get_or_insert_with(|| Arc::new(DashMap::new()))
            .insert(
                Self::RETRY_ATTEMPTS_KEY.to_string(),
                serde_json::Value::from(current_attempts + 1),
            );
    }

    /// Generates a unique fingerprint for the request based on its URL, method, and body.
    ///
    /// This is the stable identity used by runtime deduplication and related
    /// components that need to recognize equivalent requests.
    ///
    /// The fingerprint is used for duplicate detection and caching. It combines:
    /// - The request URL
    /// - The HTTP method
    /// - The request body (if present)
    ///
    /// ## Example
    ///
    /// ```rust,ignore
    /// use spider_util::request::Request;
    /// use url::Url;
    ///
    /// let request = Request::new(Url::parse("https://example.com").unwrap());
    /// let fingerprint = request.fingerprint();
    /// ```
    pub fn fingerprint(&self) -> String {
        let mut hasher = XxHash64::default();
        hasher.write(self.url.as_str().as_bytes());
        hasher.write(self.method.as_str().as_bytes());

        if let Some(ref body) = self.body {
            match body {
                Body::Json(json_val) => {
                    if let Ok(serialized) = serde_json::to_string(json_val) {
                        hasher.write(serialized.as_bytes());
                    }
                }
                Body::Form(form_val) => {
                    // Optimized: hash components directly without building intermediate String
                    for r in form_val.iter() {
                        hasher.write(r.key().as_bytes());
                        hasher.write(r.value().as_bytes());
                    }
                }
                Body::Bytes(bytes_val) => {
                    hasher.write(bytes_val);
                }
            }
        }
        format!("{:x}", hasher.finish())
    }
}
