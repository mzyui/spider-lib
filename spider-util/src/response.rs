//! Data structures and utilities for handling HTTP responses in `spider-lib`.
//!
//! This module defines the [`Response`] struct, which represents an HTTP response
//! received from a web server. It encapsulates crucial information such as
//! the URL, status code, headers, and body of the response, along with any
//! associated metadata.
//!
//! Additionally, this module provides:
//! - Helper methods for [`Response`] to facilitate common tasks like parsing
//!   the body as HTML or JSON, and reconstructing the original [`Request`]
//! - [`Link`] and [`LinkType`] enums for structured representation and extraction
//!   of hyperlinks found within the response content
//!
//! ## Example
//!
//! ```rust
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

use crate::request::Request;
use crate::selector::get_cached_selector;
use crate::util;
use dashmap::{DashMap, DashSet};
use linkify::{LinkFinder, LinkKind};
use reqwest::StatusCode;
use scraper::Html;
use serde::de::DeserializeOwned;
use serde_json;
use std::{str::Utf8Error, str::from_utf8, sync::Arc};
use url::Url;

/// Represents the type of a discovered link.
///
/// [`LinkType`] categorizes links found on web pages to enable
/// specialized handling based on the resource type.
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

/// Represents a link discovered on a web page.
///
/// [`Link`] encapsulates both the URL and the type of a discovered link,
/// enabling type-aware link processing during crawling.
///
/// ## Example
///
/// ```rust
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

/// Represents an HTTP response received from a server.
///
/// [`Response`] contains all information about an HTTP response, including
/// the final URL (after redirects), status code, headers, body content,
/// and metadata carried over from the original request.
///
/// ## Example
///
/// ```rust
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
    /// Reconstructs the original [`Request`] that led to this response.
    ///
    /// This method creates a new [`Request`] with the same URL and metadata
    /// as the request that produced this response. Useful for retry scenarios
    /// or when you need to re-request the same resource.
    ///
    /// ## Example
    ///
    /// ```rust
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
    /// ```rust
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
    /// ```rust
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
        let body_str = from_utf8(&self.body)?;
        Ok(Html::parse_document(body_str))
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
    /// ```rust
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
        let body_bytes = &self.body;
        Ok(move || {
            let body_str = from_utf8(body_bytes)?;
            Ok(Html::parse_document(body_str))
        })
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
    /// ```rust
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

        if let Ok(html_fn) = self.lazy_html()
            && let Ok(html) = html_fn()
        {
            let selectors = vec![
                ("a[href]", "href"),
                ("link[href]", "href"),
                ("script[src]", "src"),
                ("img[src]", "src"),
                ("audio[src]", "src"),
                ("video[src]", "src"),
                ("source[src]", "src"),
            ];

            for (selector_str, attr_name) in selectors {
                if let Some(selector) = get_cached_selector(selector_str) {
                    for element in html.select(&selector) {
                        if let Some(attr_value) = element.value().attr(attr_name)
                            && let Ok(url) = self.url.join(attr_value)
                            && util::is_same_site(&url, &self.url)
                        {
                            let link_type = match element.value().name() {
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
                            };
                            links.insert(Link { url, link_type });
                        }
                    }
                }
            }

            let finder = LinkFinder::new();
            for text_node in html.tree.values().filter_map(|node| node.as_text()) {
                for link in finder.links(text_node) {
                    if link.kind() == &LinkKind::Url
                        && let Ok(url) = self.url.join(link.as_str())
                        && util::is_same_site(&url, &self.url)
                    {
                        links.insert(Link {
                            url,
                            link_type: LinkType::Page,
                        });
                    }
                }
            }
        }

        links
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
