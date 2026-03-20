//! Core downloader traits.

pub use spider_util::http_client::HttpClient;
pub use spider_util::request::Request;
pub use spider_util::response::Response;

use async_trait::async_trait;
use spider_util::error::SpiderError;

/// Trait implemented by HTTP downloaders used by the crawler runtime.
#[async_trait]
pub trait Downloader: Send + Sync + 'static {
    /// Concrete HTTP client type used by the downloader.
    type Client: Send + Sync;

    /// Executes the HTTP transaction for a request.
    ///
    /// # Errors
    ///
    /// Returns an error when request execution fails.
    async fn download(&self, request: Request) -> Result<Response, SpiderError>;

    /// Returns the underlying client value used by this downloader.
    fn client(&self) -> &Self::Client;
}
