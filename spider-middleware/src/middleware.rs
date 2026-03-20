//! Middleware trait and control-flow types.
//!
//! Middleware can inspect or rewrite requests before download, inspect or
//! rewrite responses afterwards, and decide what should happen next.

use async_trait::async_trait;
use std::any::Any;
use std::time::Duration;

use spider_util::error::SpiderError;
use spider_util::request::Request;
use spider_util::response::Response;

#[allow(clippy::large_enum_variant)]
/// Control-flow result returned by middleware hooks.
pub enum MiddlewareAction<T> {
    /// Continue processing with the provided item.
    Continue(T),
    /// Retry the Request after the specified duration. (Only valid for Response processing)
    Retry(Box<Request>, Duration),
    /// Drop the item, stopping further processing.
    Drop,
    /// Return a Response directly, bypassing the downloader. (Only valid for Request processing)
    ReturnResponse(Response),
}

/// Trait implemented by request/response middleware.
#[async_trait]
pub trait Middleware<C: Send + Sync>: Any + Send + Sync + 'static {
    /// Returns a human-readable middleware name for logs and diagnostics.
    fn name(&self) -> &str;

    async fn process_request(
        &self,
        _client: &C,
        request: Request,
    ) -> Result<MiddlewareAction<Request>, SpiderError> {
        Ok(MiddlewareAction::Continue(request))
    }
    async fn process_response(
        &self,
        response: Response,
    ) -> Result<MiddlewareAction<Response>, SpiderError> {
        Ok(MiddlewareAction::Continue(response))
    }

    async fn handle_error(
        &self,
        _request: &Request,
        error: &SpiderError,
    ) -> Result<MiddlewareAction<Request>, SpiderError> {
        // The default implementation is to just pass the error through by cloning it.
        Err(error.clone())
    }
}
