//! # Spider Module
//!
//! Defines the core [`Spider`] trait and related components for implementing custom web scrapers.
//!
//! ## Overview
//!
//! The [`Spider`] trait is the primary interface for defining custom scraping logic.
//! It specifies how to start a crawl (via start URLs) and how to process responses
//! to extract data and discover new URLs to follow. This trait follows the Scrapy
//! pattern of spiders that define the crawling behavior.
//!
//! ## Key Components
//!
//! - **[`Spider`] Trait**: The main trait for implementing custom scraping logic
//! - **[`ParseOutput`]**: Container for returning scraped items and new requests
//! - **Associated Types**: Define the item type and state type that the spider uses
//!
//! ## Implementation
//!
//! Implementors must define:
//! - [`start_urls`](Spider::start_urls): Optional static seed URLs
//! - [`start_requests`](Spider::start_requests): Initial request source definition
//! - [`parse`](Spider::parse): Logic for extracting data and discovering new URLs from responses
//! - `Item`: The type of data structure to store scraped information
//! - `State`: The type of state that the spider uses (must implement `Default`)
//!
//! ## Example
//!
//! ```rust,ignore
//! use spider_core::Spider;
//! use spider_util::{response::Response, error::SpiderError, item::ParseOutput};
//! use async_trait::async_trait;
//!
//! #[spider_macro::scraped_item]
//! struct Article {
//!     title: String,
//!     content: String,
//! }
//!
//! // State for tracking page count
//! use std::sync::Arc;
//! use std::sync::atomic::{AtomicUsize, Ordering};
//! use dashmap::DashMap;
//!
//! #[derive(Clone, Default)]
//! struct ArticleSpiderState {
//!     page_count: Arc<AtomicUsize>,
//!     visited_urls: Arc<DashMap<String, bool>>,
//! }
//!
//! impl ArticleSpiderState {
//!     fn increment_page_count(&self) {
//!         self.page_count.fetch_add(1, Ordering::SeqCst);
//!     }
//!
//!     fn mark_url_visited(&self, url: String) {
//!         self.visited_urls.insert(url, true);
//!     }
//! }
//!
//! struct ArticleSpider;
//!
//! #[async_trait]
//! impl Spider for ArticleSpider {
//!     type Item = Article;
//!     type State = ArticleSpiderState;
//!
//!     fn start_requests(&self) -> Result<StartRequests<'_>, SpiderError> {
//!         let req = Request::new("https://example.com/articles".parse()?);
//!         Ok(StartRequests::Iter(Box::new(std::iter::once(Ok(req)))))
//!     }
//!
//!     async fn parse(&self, response: Response, state: &Self::State) -> Result<ParseOutput<Self::Item>, SpiderError> {
//!         // Update state - can be done concurrently without blocking the spider
//!         state.increment_page_count();
//!         state.mark_url_visited(response.url.to_string());
//!
//!         let mut output = ParseOutput::new();
//!
//!         // Extract articles from the page
//!         // ... parsing logic ...
//!
//!         // Add discovered articles to output
//!         // output.add_item(Article { title, content });
//!
//!         // Add new URLs to follow
//!         // output.add_request(new_request);
//!
//!         Ok(output)
//!     }
//! }
//! ```

use spider_util::error::SpiderError;
use spider_util::item::{ParseOutput, ScrapedItem};
use spider_util::request::Request;
use spider_util::response::Response;

use anyhow::Result;
use async_trait::async_trait;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;
use url::Url;

/// A boxed iterator of start requests.
pub type StartRequestIter<'a> = Box<dyn Iterator<Item = Result<Request, SpiderError>> + Send + 'a>;

/// Initial request source returned by [`Spider::start_requests`].
pub enum StartRequests<'a> {
    /// Fixed list of seed URLs.
    Urls(Vec<&'a str>),
    /// Direct request iterator supplied by the spider.
    Iter(StartRequestIter<'a>),
    /// Path to a plain-text seed file (one URL per line).
    File(&'a str),
}

impl<'a> StartRequests<'a> {
    /// Creates a file-based source from a path string.
    pub fn file(path: &'a str) -> Self {
        StartRequests::File(path)
    }

    /// Resolves this source into a concrete request iterator.
    pub fn into_iter(self) -> Result<StartRequestIter<'a>, SpiderError> {
        match self {
            StartRequests::Urls(urls) => {
                let requests = urls
                    .into_iter()
                    .map(|u| Url::parse(u).map(Request::new).map_err(SpiderError::from));
                Ok(Box::new(requests))
            }
            StartRequests::Iter(iter) => Ok(iter),
            StartRequests::File(path) => start_requests_from_file(path),
        }
    }
}

fn start_requests_from_file<P: AsRef<Path>>(
    path: P,
) -> Result<StartRequestIter<'static>, SpiderError> {
    let path = path.as_ref();
    let file = File::open(path)?;
    let path_display = path.display().to_string();
    let mut lines = BufReader::new(file).lines().enumerate();

    let iter = std::iter::from_fn(move || {
        loop {
            let (line_idx, line_res) = lines.next()?;
            let line_number = line_idx + 1;
            match line_res {
                Ok(line) => {
                    let trimmed = line.trim();
                    if trimmed.is_empty() || trimmed.starts_with('#') {
                        continue;
                    }

                    return Some(match Url::parse(trimmed) {
                        Ok(url) => Ok(Request::new(url)),
                        Err(e) => Err(SpiderError::ConfigurationError(format!(
                            "Invalid start URL in {} at line {}: {}",
                            path_display, line_number, e
                        ))),
                    });
                }
                Err(e) => {
                    return Some(Err(SpiderError::IoError(format!(
                        "Failed reading {} at line {}: {}",
                        path_display, line_number, e
                    ))));
                }
            }
        }
    });

    Ok(Box::new(iter))
}

/// Defines the contract for a web spider.
///
/// This trait is the core abstraction for implementing custom web scraping logic.
/// Implementors define how to generate initial requests and how to parse responses
/// to extract structured data and discover new URLs to crawl.
///
/// ## Type Parameters
///
/// - `Item`: The type of scraped data structure (must implement [`ScrapedItem`])
/// - `State`: The type of shared state (must implement `Default`)
///
/// ## Design Notes
///
/// The trait uses `&self` (immutable reference) instead of `&mut self` for the
/// [`parse`](Spider::parse) method. This design enables efficient concurrent crawling
/// by eliminating the need for mutex locks when accessing the spider from multiple
/// async tasks. State that needs mutation should be stored in the associated
/// `State` type using thread-safe primitives like `Arc<AtomicUsize>` or `DashMap`.
#[async_trait]
pub trait Spider: Send + Sync + 'static {
    /// The type of item that the spider scrapes.
    ///
    /// This associated type must implement the [`ScrapedItem`] trait, which
    /// provides methods for type erasure, cloning, and JSON serialization.
    /// Use the `#[scraped_item]` procedural macro to automatically implement
    /// all required traits for your data structures.
    type Item: ScrapedItem;

    /// The type of state that the spider uses.
    ///
    /// The state type must implement `Default` so it can be instantiated
    /// automatically by the crawler. It should also be `Send + Sync` to
    /// enable safe concurrent access from multiple async tasks.
    ///
    /// ## Example
    ///
    /// ```rust
    /// use std::sync::Arc;
    /// use std::sync::atomic::{AtomicUsize, Ordering};
    /// use dashmap::DashMap;
    ///
    /// #[derive(Clone, Default)]
    /// struct MySpiderState {
    ///     page_count: Arc<AtomicUsize>,
    ///     visited_urls: Arc<DashMap<String, bool>>,
    /// }
    /// ```
    type State: Default + Send + Sync;

    /// Returns static seed URLs.
    ///
    /// This method is optional and useful for simple spiders. The default
    /// [`start_requests`](Spider::start_requests) implementation converts these
    /// URLs into a request iterator.
    fn start_urls(&self) -> Vec<&'static str> {
        Vec::new()
    }

    /// Returns the initial request source used to start crawling.
    ///
    /// The default implementation converts [`start_urls`](Spider::start_urls)
    /// into an iterator.
    ///
    /// To load from seed file, return `StartRequests::file(path)`.
    /// To use a fixed list of URL strings, return `StartRequests::Urls(...)`.
    /// To use custom generation logic, return `StartRequests::Iter(...)`.
    ///
    /// ## Example
    ///
    /// ```rust
    /// # use spider_core::{scraped_item, Spider, StartRequests};
    /// # use spider_util::{response::Response, error::SpiderError, item::{ParseOutput, ScrapedItem}};
    /// # #[scraped_item]
    /// # struct ExampleItem {
    /// #     value: String,
    /// # }
    /// # struct MySpider;
    /// # #[async_trait::async_trait]
    /// # impl Spider for MySpider {
    /// #     type Item = ExampleItem;
    /// #     type State = ();
    /// fn start_requests(&self) -> Result<StartRequests<'_>, SpiderError> {
    ///     Ok(StartRequests::file("seeds/start_urls.txt"))
    /// }
    /// # async fn parse(&self, response: Response, state: &Self::State) -> Result<ParseOutput<Self::Item>, SpiderError> {
    /// #     todo!()
    /// # }
    /// # }
    /// ```
    fn start_requests(&self) -> Result<StartRequests<'_>, SpiderError> {
        Ok(StartRequests::Urls(self.start_urls()))
    }

    /// Parses a response and extracts scraped items and new requests.
    ///
    /// # Errors
    ///
    /// This is the primary method where scraping logic is implemented. It receives
    /// a [`Response`] object and should extract structured data (items) and/or
    /// discover new URLs to crawl (requests).
    ///
    /// ## Parameters
    ///
    /// - `response`: The HTTP response to parse, containing the body, headers, and URL
    /// - `state`: A shared reference to the spider's state, which can be used to
    ///   track information across multiple parse calls
    ///
    /// ## Returns
    ///
    /// Returns a [`ParseOutput`] containing:
    /// - Scraped items of type `Self::Item`
    /// - New [`Request`] objects to be enqueued
    ///
    /// ## Design Notes
    ///
    /// This method takes an immutable reference to `self` (`&self`) instead of
    /// mutable (`&mut self`), eliminating the need for mutex locks when accessing
    /// the spider in concurrent environments. State that needs to be modified
    /// should be stored in the `State` type using thread-safe primitives.
    ///
    /// # Errors
    ///
    /// Returns a [`SpiderError`] if parsing fails or if an unrecoverable error
    /// occurs during processing.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use spider_core::{scraped_item, Spider, StartRequests};
    /// # use spider_util::{response::Response, error::SpiderError, item::{ParseOutput, ScrapedItem}};
    /// # use async_trait::async_trait;
    /// # struct MySpider;
    /// # #[scraped_item]
    /// # struct ExampleItem {
    /// #     value: String,
    /// # }
    /// # #[derive(Default)]
    /// # struct MySpiderState;
    /// # #[async_trait]
    /// # impl Spider for MySpider {
    /// #     type Item = ExampleItem;
    /// #     type State = MySpiderState;
    /// #     fn start_requests(&self) -> Result<StartRequests<'_>, SpiderError> {
    /// #         Ok(StartRequests::Iter(Box::new(std::iter::empty())))
    /// #     }
    /// async fn parse(&self, response: Response, state: &Self::State) -> Result<ParseOutput<Self::Item>, SpiderError> {
    ///     let mut output = ParseOutput::new();
    ///
    ///     // Parse HTML and extract data
    ///     if let Ok(html) = response.to_html() {
    ///         // ... extraction logic ...
    ///     }
    ///
    ///     Ok(output)
    /// }
    /// # }
    /// ```
    async fn parse(
        &self,
        response: Response,
        state: &Self::State,
    ) -> Result<ParseOutput<Self::Item>, SpiderError>;
}
