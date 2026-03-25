//! Response types and response-side helpers.
//!
//! [`Response`] wraps the downloaded body together with the final URL, status,
//! headers, and request metadata. It also provides convenience methods for
//! parsing HTML or JSON and for extracting links.
//!
//! ## Example
//!
//! ```rust,ignore
//! use spider_util::response::Response;
//! use reqwest::StatusCode;
//! use bytes::Bytes;
//! use url::Url;
//!
//! // Create a response (typically done internally by the downloader)
//! let response = Response {
//!     url: Url::parse("https://example.com").unwrap(),
//!     status: StatusCode::OK,
//!     headers: http::header::HeaderMap::new(),
//!     body: Bytes::from("<html><body>Hello</body></html>"),
//!     request_url: Url::parse("https://example.com").unwrap(),
//!     meta: None,
//!     cached: false,
//! };
//!
//! // Parse as HTML
//! let html = response.to_html().unwrap();
//!
//! // Extract links from the response
//! let links = response.links();
//! ```
//!
//! In the crawler lifecycle, a [`Response`] is produced by the downloader,
//! optionally rewritten by middleware, and then handed to
//! [`Spider::parse`](spider_core::Spider::parse).

use crate::request::Request;
use crate::selector::get_cached_selector;
use crate::util;
use dashmap::{DashMap, DashSet};
use linkify::{LinkFinder, LinkKind};
use reqwest::StatusCode;
use scraper::{ElementRef, Html};
use seahash::SeaHasher;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json;
use std::cell::RefCell;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::{str::Utf8Error, str::from_utf8, sync::Arc};
use url::Url;

thread_local! {
    static HTML_CACHE: RefCell<HashMap<u64, Html>> = RefCell::new(HashMap::new());
}

/// Classification for links discovered in a response.
///
/// ## Variants
///
/// - `Page`: Links to other web pages (typically `<a>` tags)
/// - `Script`: Links to JavaScript files (`<script>` tags)
/// - `Stylesheet`: Links to CSS stylesheets (`<link rel="stylesheet">`)
/// - `Image`: Links to images (`<img>` tags)
/// - `Media`: Links to audio/video files (`<audio>`, `<video>`, `<source>`)
/// - `Other`: Any other type of resource with a custom identifier
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum LinkType {
    /// A link to another web page.
    Page,
    /// A link to a script file.
    Script,
    /// A link to a stylesheet.
    Stylesheet,
    /// A link to an image.
    Image,
    /// A link to a media file (audio/video).
    Media,
    /// A link to another type of resource.
    Other(String),
}

/// A link discovered while extracting URLs from a response.
///
/// ## Example
///
/// ```rust,ignore
/// use spider_util::response::{Link, LinkType};
/// use url::Url;
///
/// let link = Link {
///     url: Url::parse("https://example.com/page").unwrap(),
///     link_type: LinkType::Page,
/// };
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Link {
    /// The URL of the discovered link.
    pub url: Url,
    /// The type of the discovered link.
    pub link_type: LinkType,
}

/// One selector/attribute pair used during link extraction.
///
/// This is useful when the default HTML link sources are not enough for the
/// target site and you need to teach the extractor about custom attributes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LinkSource {
    /// CSS selector used to find candidate elements.
    pub selector: String,
    /// Attribute name that contains the URL.
    pub attribute: String,
    /// Optional fixed link type for matches from this source.
    pub link_type: Option<LinkType>,
}

impl LinkSource {
    /// Creates a new source definition.
    pub fn new(selector: impl Into<String>, attribute: impl Into<String>) -> Self {
        Self {
            selector: selector.into(),
            attribute: attribute.into(),
            link_type: None,
        }
    }

    /// Overrides the inferred link type for this source.
    pub fn with_link_type(mut self, link_type: LinkType) -> Self {
        self.link_type = Some(link_type);
        self
    }
}

/// Options that control link extraction from a [`Response`].
///
/// The defaults are intentionally conservative for crawler use: same-site
/// filtering is enabled, text links are included, and common HTML elements are
/// scanned for navigable URLs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LinkExtractOptions {
    /// Restrict discovered links to the same registered domain.
    pub same_site_only: bool,
    /// Include URLs found in text content.
    pub include_text_links: bool,
    /// HTML sources used to discover attribute-based links.
    pub sources: Vec<LinkSource>,
    /// Optional allow-list of link types to include.
    pub allowed_link_types: Option<Vec<LinkType>>,
}

impl Default for LinkExtractOptions {
    fn default() -> Self {
        Self {
            same_site_only: true,
            include_text_links: true,
            sources: default_link_sources(),
            allowed_link_types: None,
        }
    }
}

impl LinkExtractOptions {
    /// Sets whether only same-site URLs should be returned.
    pub fn same_site_only(mut self, same_site_only: bool) -> Self {
        self.same_site_only = same_site_only;
        self
    }

    /// Sets whether URLs found in text content should be returned.
    pub fn include_text_links(mut self, include_text_links: bool) -> Self {
        self.include_text_links = include_text_links;
        self
    }

    /// Replaces the configured HTML extraction sources.
    pub fn with_sources(mut self, sources: impl IntoIterator<Item = LinkSource>) -> Self {
        self.sources = sources.into_iter().collect();
        self
    }

    /// Adds an HTML extraction source.
    pub fn add_source(mut self, source: LinkSource) -> Self {
        self.sources.push(source);
        self
    }

    /// Restricts extraction to the provided link types.
    pub fn with_allowed_link_types(
        mut self,
        allowed_link_types: impl IntoIterator<Item = LinkType>,
    ) -> Self {
        self.allowed_link_types = Some(allowed_link_types.into_iter().collect());
        self
    }
}

/// Structured page metadata extracted from an HTML response.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PageMetadata {
    /// Contents of the `<title>` element.
    pub title: Option<String>,
    /// Contents of `<meta name="description">`.
    pub description: Option<String>,
    /// Canonical URL from `<link rel="canonical">`.
    pub canonical_url: Option<Url>,
    /// Open Graph metadata such as `og:title` or `og:image`.
    pub open_graph: HashMap<String, String>,
    /// Feed URLs discovered from alternate RSS/Atom link tags.
    pub feed_urls: Vec<Url>,
}

impl PageMetadata {
    /// Returns `true` when no metadata fields were extracted.
    pub fn is_empty(&self) -> bool {
        self.title.is_none()
            && self.description.is_none()
            && self.canonical_url.is_none()
            && self.open_graph.is_empty()
            && self.feed_urls.is_empty()
    }
}

/// Represents an HTTP response received from a server.
///
/// [`Response`] contains all information about an HTTP response, including
/// the final URL (after redirects), status code, headers, body content,
/// and metadata carried over from the original request.
///
/// The type is designed for parse-time ergonomics:
/// - [`Response::to_html`] parses the body as HTML
/// - [`Response::json`] deserializes JSON payloads
/// - [`Response::links`] and related helpers extract follow-up links
/// - [`Response::to_request`] reconstructs the originating request context
///
/// ## Example
///
/// ```rust,ignore
/// use spider_util::response::Response;
/// use reqwest::StatusCode;
/// use bytes::Bytes;
/// use url::Url;
///
/// let response = Response {
///     url: Url::parse("https://example.com").unwrap(),
///     status: StatusCode::OK,
///     headers: http::header::HeaderMap::new(),
///     body: Bytes::from("<html><body>Hello</body></html>"),
///     request_url: Url::parse("https://example.com").unwrap(),
///     meta: None,
///     cached: false,
/// };
///
/// // Parse the response body as HTML
/// if let Ok(html) = response.to_html() {
///     // Process HTML...
/// }
/// ```
#[derive(Debug)]
pub struct Response {
    /// The final URL of the response after any redirects.
    pub url: Url,
    /// The HTTP status code of the response.
    pub status: StatusCode,
    /// The headers of the response.
    pub headers: http::header::HeaderMap,
    /// The body of the response.
    pub body: bytes::Bytes,
    /// The original URL of the request that led to this response.
    pub request_url: Url,
    /// Metadata associated with the response, carried over from the request.
    /// Uses Option to allow lazy initialization.
    pub meta: Option<Arc<DashMap<String, serde_json::Value>>>,
    /// Indicates if the response was served from a cache.
    pub cached: bool,
}

impl Response {
    /// Creates a new response with an empty HTML cache.
    ///
    /// Most application code receives responses from the runtime rather than
    /// constructing them directly. This constructor is mainly useful for custom
    /// downloaders and lower-level integrations.
    pub fn new(
        url: Url,
        status: StatusCode,
        headers: http::header::HeaderMap,
        body: bytes::Bytes,
        request_url: Url,
    ) -> Self {
        Self {
            url,
            status,
            headers,
            body,
            request_url,
            meta: None,
            cached: false,
        }
    }

    /// Reconstructs the original [`Request`] that led to this response.
    ///
    /// This method creates a new [`Request`] with the same URL and metadata
    /// as the request that produced this response. Useful for retry scenarios
    /// or when you need to re-request the same resource.
    ///
    /// ## Example
    ///
    /// ```rust,ignore
    /// # use spider_util::response::Response;
    /// # use reqwest::StatusCode;
    /// # use bytes::Bytes;
    /// # use url::Url;
    /// # let response = Response {
    /// #     url: Url::parse("https://example.com").unwrap(),
    /// #     status: StatusCode::OK,
    /// #     headers: http::header::HeaderMap::new(),
    /// #     body: Bytes::from("hello"),
    /// #     request_url: Url::parse("https://example.com").unwrap(),
    /// #     meta: None,
    /// #     cached: false,
    /// # };
    /// let original_request = response.request_from_response();
    /// ```
    pub fn request_from_response(&self) -> Request {
        let mut request = Request::new(self.request_url.clone());
        request.set_meta_from_option(self.meta.clone());
        request
    }

    /// Returns a cloned metadata value by key.
    pub fn get_meta(&self, key: &str) -> Option<serde_json::Value> {
        self.meta
            .as_ref()
            .and_then(|m| m.get(key).map(|entry| entry.value().clone()))
    }

    /// Inserts a metadata value, lazily allocating the map if needed.
    pub fn insert_meta(&mut self, key: impl Into<String>, value: serde_json::Value) {
        self.meta
            .get_or_insert_with(|| Arc::new(DashMap::new()))
            .insert(key.into(), value);
    }

    /// Returns a clone of the internal metadata map, if present.
    pub fn clone_meta(&self) -> Option<Arc<DashMap<String, serde_json::Value>>> {
        self.meta.clone()
    }

    /// Deserializes the response body as JSON.
    ///
    /// # Type Parameters
    ///
    /// - `T`: The target type to deserialize into (must implement `DeserializeOwned`)
    ///
    /// # Errors
    ///
    /// Returns a [`serde_json::Error`] if the body cannot be parsed as JSON
    /// or if it cannot be deserialized into type `T`.
    ///
    /// ## Example
    ///
    /// ```rust,ignore
    /// # use spider_util::response::Response;
    /// # use reqwest::StatusCode;
    /// # use bytes::Bytes;
    /// # use url::Url;
    /// # use serde::Deserialize;
    /// # #[derive(Deserialize)]
    /// # struct Data { value: String }
    /// # let response = Response {
    /// #     url: Url::parse("https://api.example.com").unwrap(),
    /// #     status: StatusCode::OK,
    /// #     headers: http::header::HeaderMap::new(),
    /// #     body: Bytes::from(r#"{"value": "test"}"#),
    /// #     request_url: Url::parse("https://api.example.com").unwrap(),
    /// #     meta: None,
    /// #     cached: false,
    /// # };
    /// let data: Data = response.json()?;
    /// # Ok::<(), serde_json::Error>(())
    /// ```
    pub fn json<T: DeserializeOwned>(&self) -> Result<T, serde_json::Error> {
        serde_json::from_slice(&self.body)
    }

    /// Parses the response body as HTML.
    ///
    /// Returns a [`scraper::Html`] document that can be queried using CSS selectors.
    ///
    /// # Errors
    ///
    /// Returns a [`Utf8Error`] if the response body is not valid UTF-8.
    ///
    /// ## Example
    ///
    /// ```rust,ignore
    /// # use spider_util::response::Response;
    /// # use reqwest::StatusCode;
    /// # use bytes::Bytes;
    /// # use url::Url;
    /// # let response = Response {
    /// #     url: Url::parse("https://example.com").unwrap(),
    /// #     status: StatusCode::OK,
    /// #     headers: http::header::HeaderMap::new(),
    /// #     body: Bytes::from("<html><body>Hello</body></html>"),
    /// #     request_url: Url::parse("https://example.com").unwrap(),
    /// #     meta: None,
    /// #     cached: false,
    /// # };
    /// let html = response.to_html()?;
    /// # Ok::<(), std::str::Utf8Error>(())
    /// ```
    pub fn to_html(&self) -> Result<Html, Utf8Error> {
        let cache_key = self.html_cache_key();

        HTML_CACHE.with(|cache| {
            if let Some(html) = cache.borrow().get(&cache_key).cloned() {
                return Ok(html);
            }

            let body_str = from_utf8(&self.body)?;
            let html = Html::parse_document(body_str);
            cache.borrow_mut().insert(cache_key, html.clone());
            Ok(html)
        })
    }

    /// Lazily parses the response body as HTML.
    ///
    /// Returns a closure that can be called when the HTML is actually needed.
    /// This avoids parsing HTML for responses where it may not be used.
    ///
    /// # Errors
    ///
    /// Returns a [`Utf8Error`] if the response body is not valid UTF-8.
    ///
    /// ## Example
    ///
    /// ```rust,ignore
    /// # use spider_util::response::Response;
    /// # use reqwest::StatusCode;
    /// # use bytes::Bytes;
    /// # use url::Url;
    /// # let response = Response {
    /// #     url: Url::parse("https://example.com").unwrap(),
    /// #     status: StatusCode::OK,
    /// #     headers: http::header::HeaderMap::new(),
    /// #     body: Bytes::from("<html><body>Hello</body></html>"),
    /// #     request_url: Url::parse("https://example.com").unwrap(),
    /// #     meta: None,
    /// #     cached: false,
    /// # };
    /// let html_fn = response.lazy_html()?;
    /// // Parse HTML only when needed
    /// let html = html_fn()?;
    /// # Ok::<(), std::str::Utf8Error>(())
    /// ```
    pub fn lazy_html(&self) -> Result<impl Fn() -> Result<Html, Utf8Error> + '_, Utf8Error> {
        Ok(move || self.to_html())
    }

    /// Returns the response body as UTF-8 text.
    pub fn text(&self) -> Result<&str, Utf8Error> {
        from_utf8(&self.body)
    }

    /// Extracts structured page metadata from HTML responses.
    pub fn page_metadata(&self) -> Result<PageMetadata, Utf8Error> {
        let html = self.to_html()?;
        let mut metadata = PageMetadata::default();

        if let Some(selector) = get_cached_selector("title") {
            metadata.title = html
                .select(&selector)
                .next()
                .map(|node| node.text().collect::<String>().trim().to_string())
                .filter(|value| !value.is_empty());
        }

        if let Some(selector) = get_cached_selector("meta[name], meta[property], meta[content]") {
            for element in html.select(&selector) {
                let Some(content) = element.value().attr("content") else {
                    continue;
                };
                let content = content.trim();
                if content.is_empty() {
                    continue;
                }

                if let Some(name) = element.value().attr("name")
                    && name.eq_ignore_ascii_case("description")
                    && metadata.description.is_none()
                {
                    metadata.description = Some(content.to_string());
                }

                if let Some(property) = element.value().attr("property")
                    && property.len() >= 3
                    && property[..3].eq_ignore_ascii_case("og:")
                {
                    metadata
                        .open_graph
                        .entry(property.to_string())
                        .or_insert_with(|| content.to_string());
                }
            }
        }

        if let Some(selector) = get_cached_selector("link[href]") {
            for element in html.select(&selector) {
                let Some(href) = element.value().attr("href") else {
                    continue;
                };
                let rel = element.value().attr("rel").unwrap_or_default();

                if rel
                    .split_ascii_whitespace()
                    .any(|token| token.eq_ignore_ascii_case("canonical"))
                    && metadata.canonical_url.is_none()
                {
                    if let Ok(url) = self.url.join(href) {
                        metadata.canonical_url = Some(url);
                    }
                }

                let is_alternate = rel
                    .split_ascii_whitespace()
                    .any(|token| token.eq_ignore_ascii_case("alternate"));
                let ty = element.value().attr("type").unwrap_or_default();
                let is_feed = ty.eq_ignore_ascii_case("application/rss+xml")
                    || ty.eq_ignore_ascii_case("application/atom+xml")
                    || ty.eq_ignore_ascii_case("application/xml")
                    || ty.eq_ignore_ascii_case("text/xml");

                if is_alternate
                    && is_feed
                    && let Ok(url) = self.url.join(href)
                    && !metadata.feed_urls.contains(&url)
                {
                    metadata.feed_urls.push(url);
                }
            }
        }

        Ok(metadata)
    }

    /// Returns a customizable iterator of links discovered in the response body.
    ///
    /// Unlike [`Response::links`], this method does not deduplicate results.
    /// Callers that need uniqueness can collect into a set or use [`Response::links`].
    ///
    /// ## Example
    ///
    /// ```rust,ignore
    /// # use spider_util::response::{LinkExtractOptions, Response};
    /// # use reqwest::StatusCode;
    /// # use bytes::Bytes;
    /// # use url::Url;
    /// # let response = Response {
    /// #     url: Url::parse("https://example.com").unwrap(),
    /// #     status: StatusCode::OK,
    /// #     headers: http::header::HeaderMap::new(),
    /// #     body: Bytes::from(r#"<html><body><a href="/page">Link</a></body></html>"#),
    /// #     request_url: Url::parse("https://example.com").unwrap(),
    /// #     meta: None,
    /// #     cached: false,
    /// # };
    /// let links: Vec<_> = response
    ///     .links_iter(LinkExtractOptions::default())
    ///     .collect();
    /// assert!(!links.is_empty());
    /// ```
    pub fn links_iter(&self, options: LinkExtractOptions) -> impl Iterator<Item = Link> {
        self.parse_links(options).unwrap_or_default().into_iter()
    }

    /// Extracts all unique, same-site links from the response body.
    ///
    /// This method discovers links from:
    /// - HTML elements with `href` or `src` attributes (`<a>`, `<link>`, `<script>`, `<img>`, etc.)
    /// - URLs found in text content (using link detection)
    ///
    /// Only links pointing to the same site (same registered domain) are included.
    ///
    /// ## Returns
    ///
    /// A [`DashSet`] of [`Link`] objects containing the URL and link type.
    ///
    /// ## Example
    ///
    /// ```rust,ignore
    /// # use spider_util::response::Response;
    /// # use reqwest::StatusCode;
    /// # use bytes::Bytes;
    /// # use url::Url;
    /// # let response = Response {
    /// #     url: Url::parse("https://example.com").unwrap(),
    /// #     status: StatusCode::OK,
    /// #     headers: http::header::HeaderMap::new(),
    /// #     body: Bytes::from(r#"<html><body><a href="/page">Link</a></body></html>"#),
    /// #     request_url: Url::parse("https://example.com").unwrap(),
    /// #     meta: None,
    /// #     cached: false,
    /// # };
    /// let links = response.links();
    /// for link in links.iter() {
    ///     println!("Found {:?} link: {}", link.link_type, link.url);
    /// }
    /// ```
    pub fn links(&self) -> DashSet<Link> {
        let links = DashSet::new();

        for link in self.links_iter(LinkExtractOptions::default()) {
            links.insert(link);
        }

        links
    }

    fn parse_links(&self, options: LinkExtractOptions) -> Result<Vec<Link>, Utf8Error> {
        let html_fn = self.lazy_html()?;
        let html = html_fn()?;
        let mut links = Vec::new();

        self.collect_attribute_links(&html, &options, &mut links);

        if options.include_text_links {
            self.collect_text_links(&html, &options, &mut links);
        }

        Ok(links)
    }

    fn collect_attribute_links(
        &self,
        html: &Html,
        options: &LinkExtractOptions,
        links: &mut Vec<Link>,
    ) {
        for source in &options.sources {
            let Some(selector) = get_cached_selector(&source.selector) else {
                continue;
            };

            for element in html.select(&selector) {
                let Some(attr_value) = element.value().attr(&source.attribute) else {
                    continue;
                };

                let link_type = source
                    .link_type
                    .clone()
                    .unwrap_or_else(|| infer_link_type(&element));

                if let Some(link) = self.build_link(attr_value, link_type, options) {
                    links.push(link);
                }
            }
        }
    }

    fn collect_text_links(&self, html: &Html, options: &LinkExtractOptions, links: &mut Vec<Link>) {
        let finder = LinkFinder::new();

        for text_node in html.tree.values().filter_map(|node| node.as_text()) {
            for link in finder.links(text_node) {
                if link.kind() != &LinkKind::Url {
                    continue;
                }

                if let Some(link) = self.build_link(link.as_str(), LinkType::Page, options) {
                    links.push(link);
                }
            }
        }
    }

    fn build_link(
        &self,
        raw_url: &str,
        link_type: LinkType,
        options: &LinkExtractOptions,
    ) -> Option<Link> {
        let url = self.url.join(raw_url).ok()?;

        if options.same_site_only && !util::is_same_site(&url, &self.url) {
            return None;
        }

        if !options
            .allowed_link_types
            .as_ref()
            .is_none_or(|allowed| allowed.contains(&link_type))
        {
            return None;
        }

        Some(Link { url, link_type })
    }

    fn html_cache_key(&self) -> u64 {
        let mut hasher = SeaHasher::new();
        self.url.as_str().hash(&mut hasher);
        self.request_url.as_str().hash(&mut hasher);
        self.body.hash(&mut hasher);
        hasher.finish()
    }
}

impl Clone for Response {
    fn clone(&self) -> Self {
        Response {
            url: self.url.clone(),
            status: self.status,
            headers: self.headers.clone(),
            body: self.body.clone(),
            request_url: self.request_url.clone(),
            meta: self.meta.clone(),
            cached: self.cached,
        }
    }
}

fn default_link_sources() -> Vec<LinkSource> {
    vec![
        LinkSource::new("a[href]", "href"),
        LinkSource::new("link[href]", "href"),
        LinkSource::new("script[src]", "src"),
        LinkSource::new("img[src]", "src"),
        LinkSource::new("audio[src]", "src"),
        LinkSource::new("video[src]", "src"),
        LinkSource::new("source[src]", "src"),
    ]
}

fn infer_link_type(element: &ElementRef<'_>) -> LinkType {
    match element.value().name() {
        "a" => LinkType::Page,
        "link" => {
            if let Some(rel) = element.value().attr("rel") {
                if rel.eq_ignore_ascii_case("stylesheet") {
                    LinkType::Stylesheet
                } else {
                    LinkType::Other(rel.to_string())
                }
            } else {
                LinkType::Other("link".to_string())
            }
        }
        "script" => LinkType::Script,
        "img" => LinkType::Image,
        "audio" | "video" | "source" => LinkType::Media,
        _ => LinkType::Other(element.value().name().to_string()),
    }
}
