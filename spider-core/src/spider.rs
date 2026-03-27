//! The spider trait and request bootstrap types.
//!
//! [`Spider`] is the main contract every crawler implements. It defines how
//! a crawl starts and how each downloaded response turns into scraped items and
//! follow-up requests.
//!
//! ## Example
//!
//! ```rust,ignore
//! use spider_core::{ParseContext, Spider};
//! use spider_util::{error::SpiderError, item::ParseOutput};
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
//!         Ok(StartRequests::iter(std::iter::once(Ok(req))))
//!     }
//!
//!     async fn parse(&self, cx: ParseContext<'_, Self>) -> Result<(), SpiderError> {
//!         // Update state - can be done concurrently without blocking the spider
//!         cx.state().increment_page_count();
//!         cx.state().mark_url_visited(cx.url.to_string());
//!
//!         // Extract articles from the page
//!         // ... parsing logic ...
//!
//!         // Add discovered articles to output
//!         // cx.add_item(Article { title, content }).await?;
//!
//!         // Add new URLs to follow
//!         // cx.add_request(new_request).await?;
//!
//!         Ok(())
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
use std::ops::{Deref, DerefMut};
use std::path::Path;
use url::Url;

/// A boxed iterator of start requests.
pub type StartRequestIter<'a> = Box<dyn Iterator<Item = Result<Request, SpiderError>> + Send + 'a>;

/// Initial request source returned by [`Spider::start_requests`].
///
/// Use [`StartRequests::Urls`] for simple static seeds, [`StartRequests::Iter`]
/// when you need to construct full [`Request`] values or generate seeds
/// lazily, and [`StartRequests::File`] when you want to keep large seed lists
/// outside compiled code.
pub enum StartRequests<'a> {
    /// Fixed list of seed URLs.
    Urls(Vec<&'a str>),
    /// Direct request iterator supplied by the spider.
    Iter(StartRequestIter<'a>),
    /// Path to a plain-text seed file (one URL per line).
    File(&'a str),
}

impl<'a> StartRequests<'a> {
    /// Creates an iterator-based source from any compatible request iterator.
    pub fn iter<I>(iter: I) -> Self
    where
        I: Iterator<Item = Result<Request, SpiderError>> + Send + 'a,
    {
        StartRequests::Iter(Box::new(iter))
    }

    /// Creates a file-based source from a path string.
    ///
    /// The file is expected to contain one URL per line. Empty lines and lines
    /// starting with `#` are ignored.
    pub fn file(path: &'a str) -> Self {
        StartRequests::File(path)
    }

    /// Resolves this source into a concrete request iterator.
    #[allow(clippy::should_implement_trait)]
    ///
    /// URL strings are parsed eagerly as the iterator is consumed. Invalid file
    /// entries become `SpiderError::ConfigurationError` items that preserve the
    /// original line number.
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

impl<'a, I> From<I> for StartRequests<'a>
where
    I: Iterator<Item = Result<Request, SpiderError>> + Send + 'a,
{
    fn from(iter: I) -> Self {
        StartRequests::iter(iter)
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

/// Parse-time context passed into [`Spider::parse`].
///
/// This bundles the current [`Response`], shared spider state, and the async
/// output sink into a single value so user-facing parse signatures stay small.
///
/// The context dereferences to [`Response`], which means selector-heavy code
/// can keep the natural `cx.css(...)` style without manually reaching through a
/// nested response field.
pub struct ParseContext<'a, S: Spider + ?Sized> {
    response: Response,
    state: &'a S::State,
    output: ParseOutput<S::Item>,
}

impl<'a, S: Spider + ?Sized> ParseContext<'a, S> {
    pub(crate) fn new(
        response: Response,
        state: &'a S::State,
        output: ParseOutput<S::Item>,
    ) -> Self {
        Self {
            response,
            state,
            output,
        }
    }

    /// Returns the shared spider state for this parse call.
    pub fn state(&self) -> &'a S::State {
        self.state
    }

    /// Returns the current response explicitly.
    pub fn response(&self) -> &Response {
        &self.response
    }

    /// Returns the current response as a mutable reference.
    pub fn response_mut(&mut self) -> &mut Response {
        &mut self.response
    }

    /// Returns the underlying async parse output sink.
    pub fn output(&self) -> &ParseOutput<S::Item> {
        &self.output
    }

    /// Emits a scraped item into the runtime.
    pub async fn add_item(&self, item: S::Item) -> Result<(), SpiderError> {
        self.output.add_item(item).await
    }

    /// Emits multiple scraped items into the runtime.
    pub async fn add_items(
        &self,
        items: impl IntoIterator<Item = S::Item>,
    ) -> Result<(), SpiderError> {
        self.output.add_items(items).await
    }

    /// Emits a follow-up request into the runtime.
    pub async fn add_request(&self, request: Request) -> Result<(), SpiderError> {
        self.output.add_request(request).await
    }

    /// Emits multiple follow-up requests into the runtime.
    pub async fn add_requests(
        &self,
        requests: impl IntoIterator<Item = Request>,
    ) -> Result<(), SpiderError> {
        self.output.add_requests(requests).await
    }

    /// Consumes the context and returns the inner response, state reference,
    /// and output sink.
    pub fn into_parts(self) -> (Response, &'a S::State, ParseOutput<S::Item>) {
        (self.response, self.state, self.output)
    }

    /// Consumes the context and returns the inner response.
    pub fn into_response(self) -> Response {
        self.response
    }
}

impl<S: Spider + ?Sized> Deref for ParseContext<'_, S> {
    type Target = Response;

    fn deref(&self) -> &Self::Target {
        &self.response
    }
}

impl<S: Spider + ?Sized> DerefMut for ParseContext<'_, S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.response
    }
}

/// Defines the contract for a spider.
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
///
/// A typical crawl lifecycle looks like this:
///
/// 1. [`start_requests`](Spider::start_requests) produces the initial requests
/// 2. the runtime schedules and downloads them
/// 3. [`parse`](Spider::parse) receives a [`ParseContext`] for each response
/// 4. emitted items go to pipelines and emitted requests go back to the scheduler
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
    /// ```rust,ignore
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
    ///
    /// Prefer this method when plain URL strings are enough. Override
    /// [`start_requests`](Spider::start_requests) instead when you need custom
    /// headers, methods, request metadata, seed-file loading, or dynamic seed
    /// generation.
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
    /// To use custom generation logic, return `StartRequests::iter(...)`.
    ///
    /// This method is the better override point whenever initial requests need
    /// more than a URL string, such as per-request metadata, POST bodies, or
    /// custom headers.
    ///
    /// ## Example
    ///
    /// ```rust,ignore
    /// # use spider_core::{scraped_item, ParseContext, Spider, StartRequests};
    /// # use spider_util::{error::SpiderError, item::{ParseOutput, ScrapedItem}};
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
    /// # async fn parse(&self, cx: ParseContext<'_, Self>) -> Result<(), SpiderError> {
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
    /// - `cx`: A parse context containing the current response, shared spider
    ///   state, and async output sink
    ///
    /// ## Returns
    ///
    /// The provided [`ParseContext`] lets the spider stream:
    /// - Scraped items of type `Self::Item`
    /// - New [`Request`] objects to be enqueued
    ///
    /// The usual pattern is:
    /// - read the response through the context directly, for example
    ///   `cx.css(...)` via [`Deref`]
    /// - read shared state with [`ParseContext::state`]
    /// - call [`ParseContext::add_item`] or `add_items` for scraped items
    /// - call [`ParseContext::add_request`] or `add_requests` for follow-up
    ///   requests
    /// - return `Ok(())` when parsing is done
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
    /// ```rust,ignore
    /// # use spider_core::{scraped_item, ParseContext, Spider, StartRequests};
    /// # use spider_util::{error::SpiderError, item::{ParseOutput, ScrapedItem}};
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
    /// #         Ok(StartRequests::iter(std::iter::empty()))
    /// #     }
    /// async fn parse(&self, cx: ParseContext<'_, Self>) -> Result<(), SpiderError> {
    ///     // Parse HTML and extract data
    ///     let heading = cx.css("h1::text")?.get().unwrap_or_default();
    ///
    ///     Ok(())
    /// }
    /// # }
    /// ```
    async fn parse(&self, cx: ParseContext<'_, Self>) -> Result<(), SpiderError>;
}
