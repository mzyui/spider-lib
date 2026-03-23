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
///
/// Not every variant is meaningful in every hook:
/// - request hooks typically return `Continue`, `Drop`, or `ReturnResponse`
/// - response hooks typically return `Continue`, `Drop`, or `Retry`
/// - error hooks typically return `Continue`, `Drop`, or `Retry`
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
///
/// Middleware runs around the downloader boundary:
///
/// 1. `process_request` sees outgoing requests before download
/// 2. the downloader executes the request unless middleware short-circuits it
/// 3. `process_response` sees successful responses
/// 4. `handle_error` sees download failures
///
/// Each hook can continue normal processing, stop it, or redirect control
/// flow through [`MiddlewareAction`].
#[async_trait]
pub trait Middleware<C: Send + Sync>: Any + Send + Sync + 'static {
    /// Returns a human-readable middleware name for logs and diagnostics.
    fn name(&self) -> &str;

    /// Intercepts an outgoing request before the downloader runs.
    ///
    /// Typical uses include header injection, request filtering, cache lookup,
    /// throttling, or proxy selection.
    ///
    /// Return:
    /// - `Continue(request)` to keep normal processing
    /// - `Drop` to stop processing that request entirely
    /// - `ReturnResponse(response)` to bypass the downloader
    async fn process_request(
        &self,
        _client: &C,
        request: Request,
    ) -> Result<MiddlewareAction<Request>, SpiderError> {
        Ok(MiddlewareAction::Continue(request))
    }
    /// Intercepts a successful response after download.
    ///
    /// Typical uses include cache population, adaptive throttling, cookie
    /// extraction, or retry decisions based on status/body.
    ///
    /// Return:
    /// - `Continue(response)` to forward the response to later middleware and parsing
    /// - `Drop` to stop processing the response
    /// - `Retry(request, delay)` to reschedule work after an optional wait
    async fn process_response(
        &self,
        response: Response,
    ) -> Result<MiddlewareAction<Response>, SpiderError> {
        Ok(MiddlewareAction::Continue(response))
    }

    /// Handles downloader errors for a request.
    ///
    /// The default behavior propagates the error unchanged. Override this for
    /// retry policy, selective suppression, or custom recovery behavior.
    ///
    /// Return:
    /// - `Continue(request)` to resubmit immediately
    /// - `Drop` to swallow the error and stop processing
    /// - `Retry(request, delay)` to resubmit after waiting
    async fn handle_error(
        &self,
        _request: &Request,
        error: &SpiderError,
    ) -> Result<MiddlewareAction<Request>, SpiderError> {
        // The default implementation is to just pass the error through by cloning it.
        Err(error.clone())
    }
}
