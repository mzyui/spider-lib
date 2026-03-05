//! Traits for HTTP downloaders in the `spider-lib` framework.

pub use spider_util::http_client::HttpClient;
pub use spider_util::request::Request;
pub use spider_util::response::Response;

use async_trait::async_trait;
use spider_util::error::SpiderError;

/// A trait for HTTP downloaders that can fetch web pages and apply middleware
#[async_trait]
pub trait Downloader: Send + Sync + 'static {
    /// Concrete HTTP client type used by the downloader.
    type Client: Send + Sync;

    /// Download a web page using the provided request.
    /// This function focuses solely on executing the HTTP request.
    ///
    /// # Errors
    ///
    /// Returns an error when request execution fails.
    async fn download(&self, request: Request) -> Result<Response, SpiderError>;

    /// Returns a reference to the underlying HTTP client.
    fn client(&self) -> &Self::Client;
}
