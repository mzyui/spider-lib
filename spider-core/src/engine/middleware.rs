//! Middleware Manager for efficient concurrent access.
//!
//! This module provides a `MiddlewareManager` that stores middlewares in a vector
//! and allows efficient concurrent access.

use spider_middleware::middleware::{Middleware, MiddlewareAction};
use spider_util::error::SpiderError;
use spider_util::request::Request;
use spider_util::response::Response;
use std::sync::Arc;

/// A manager for middlewares that provides efficient concurrent access.
pub struct MiddlewareManager<C> {
    middlewares: Vec<Box<dyn Middleware<C> + Send + Sync>>,
}

impl<C: Send + Sync + 'static> MiddlewareManager<C> {
    /// Creates a new `MiddlewareManager` with the given middlewares.
    pub fn new(middlewares: Vec<Box<dyn Middleware<C> + Send + Sync>>) -> Self {
        Self { middlewares }
    }

    /// Processes a request through all registered middlewares.
    pub async fn process_request(
        &self,
        client: &C,
        request: Request,
    ) -> Result<MiddlewareAction<Request>, SpiderError> {
        let mut current_request = request;

        for middleware in &self.middlewares {
            match middleware.process_request(client, current_request).await {
                Ok(MiddlewareAction::Continue(req)) => {
                    current_request = req;
                }
                Ok(action) => return Ok(action),
                Err(e) => return Err(e),
            }
        }

        Ok(MiddlewareAction::Continue(current_request))
    }

    /// Processes a response through all registered middlewares in reverse order.
    pub async fn process_response(
        &self,
        response: Response,
    ) -> Result<MiddlewareAction<Response>, SpiderError> {
        let mut current_response = response;

        // Process in reverse order to match the request processing chain
        for middleware in self.middlewares.iter().rev() {
            match middleware.process_response(current_response).await {
                Ok(MiddlewareAction::Continue(res)) => {
                    current_response = res;
                }
                Ok(action) => return Ok(action),
                Err(e) => return Err(e),
            }
        }

        Ok(MiddlewareAction::Continue(current_response))
    }

    /// Processes an error through all registered middlewares in reverse order.
    pub async fn handle_error(
        &self,
        request: &Request,
        error: &SpiderError,
    ) -> Result<MiddlewareAction<Request>, SpiderError> {
        let mut current_error = error.clone();

        for middleware in self.middlewares.iter().rev() {
            match middleware.handle_error(request, &current_error).await {
                Ok(action) => return Ok(action),
                Err(next_error) => current_error = next_error,
            }
        }

        Err(current_error)
    }
}

/// A shared middleware manager that can be safely accessed concurrently.
/// Middleware state is expected to use interior mutability when needed, so the
/// request path stays fully concurrent.
pub struct SharedMiddlewareManager<C> {
    manager: Arc<MiddlewareManager<C>>,
    has_middlewares: bool,
}

impl<C: Send + Sync + 'static> SharedMiddlewareManager<C> {
    /// Creates a new `SharedMiddlewareManager` with the given middlewares.
    pub fn new(middlewares: Vec<Box<dyn Middleware<C> + Send + Sync>>) -> Self {
        let has_middlewares = !middlewares.is_empty();
        Self {
            manager: Arc::new(MiddlewareManager::new(middlewares)),
            has_middlewares,
        }
    }

    /// Processes a request through all registered middlewares.
    pub async fn process_request(
        &self,
        client: &C,
        request: Request,
    ) -> Result<MiddlewareAction<Request>, SpiderError> {
        if !self.has_middlewares {
            return Ok(MiddlewareAction::Continue(request));
        }
        self.manager.process_request(client, request).await
    }

    /// Processes a response through all registered middlewares in reverse order.
    pub async fn process_response(
        &self,
        response: Response,
    ) -> Result<MiddlewareAction<Response>, SpiderError> {
        if !self.has_middlewares {
            return Ok(MiddlewareAction::Continue(response));
        }
        self.manager.process_response(response).await
    }

    /// Processes a downloader error through all registered middlewares in reverse order.
    pub async fn handle_error(
        &self,
        request: &Request,
        error: &SpiderError,
    ) -> Result<MiddlewareAction<Request>, SpiderError> {
        if !self.has_middlewares {
            return Err(error.clone());
        }
        self.manager.handle_error(request, error).await
    }
}

impl<C: Send + Sync + 'static> Clone for SharedMiddlewareManager<C> {
    fn clone(&self) -> Self {
        Self {
            manager: Arc::clone(&self.manager),
            has_middlewares: self.has_middlewares,
        }
    }
}
