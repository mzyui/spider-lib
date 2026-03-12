//! Contains the request handling logic for the spider crawler.
//!
//! This module implements the core request processing pipeline that manages the flow of requests
//! and responses through the crawling system. It handles:
//!
//! - Receiving requests from the scheduler
//! - Managing concurrent downloads with configurable limits
//! - Processing requests through middleware chains
//! - Applying backpressure mechanisms to prevent overload
//! - Handling response transmission back to the processing pipeline
//! - Coordinating with the scheduler for shutdown procedures
//!
//! The main entry point is the `spawn_downloader_task` function which creates an async task
//! responsible for continuously processing requests from a receiver channel, downloading them,
//! and sending responses to a transmitter channel.

use crate::Downloader;
use crate::engine::SharedMiddlewareManager;
use crate::scheduler::Scheduler;
use crate::state::CrawlerState;
use crate::stats::StatCollector;

use kanal::{AsyncReceiver, AsyncSender};
use log::{debug, error, trace};
use spider_middleware::middleware::MiddlewareAction;
use spider_util::error::SpiderError;
use spider_util::item::ScrapedItem;
use spider_util::request::Request;
use spider_util::response::Response;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio::task::yield_now;
use tokio::time::Instant;

#[allow(clippy::too_many_arguments)]
pub fn spawn_downloader_task<S, C>(
    scheduler: Arc<Scheduler>,
    req_rx: AsyncReceiver<Request>,
    downloader: Arc<dyn Downloader<Client = C> + Send + Sync>,
    middlewares: SharedMiddlewareManager<C>,
    state: Arc<CrawlerState>,
    res_tx: AsyncSender<Response>,
    max_concurrent_downloads: usize,
    response_backpressure_threshold: usize,
    retry_release_permit: bool,
    stats: Arc<StatCollector>,
) -> tokio::task::JoinHandle<()>
where
    S: crate::spider::Spider + 'static,
    S::Item: ScrapedItem,
    C: Send + Sync + Clone + 'static,
{
    let semaphore = Arc::new(Semaphore::new(max_concurrent_downloads));
    let mut tasks = JoinSet::new();

    tokio::spawn(async move {
        trace!(
            "Downloader task started with max_concurrent_downloads: {}",
            max_concurrent_downloads
        );
        loop {
            if scheduler.is_shutting_down.load(Ordering::SeqCst) {
                trace!("Scheduler shutdown flag detected, exiting downloader task");
                break;
            }

            // Check for backpressure by monitoring response channel capacity
            if res_tx.len() > response_backpressure_threshold {
                trace!("High response channel occupancy detected, applying backpressure");
                yield_now().await;
                continue;
            }

            let request = match req_rx.recv().await {
                Ok(req) => {
                    trace!("Received request for URL: {}", req.url);

                    // Apply backpressure if response channel is filling up.
                    if res_tx.len() > response_backpressure_threshold.saturating_div(2).max(1) {
                        trace!(
                            "Applying backpressure, response channel occupancy: {}",
                            res_tx.len()
                        );
                        yield_now().await;
                    }

                    req
                }
                Err(_) => {
                    trace!("Request channel closed, exiting downloader task");
                    break;
                }
            };

            // Acquire permit from semaphore for concurrency control
            let permit = match semaphore.clone().acquire_owned().await {
                Ok(permit) => permit,
                Err(_) => {
                    yield_now().await;
                    continue;
                }
            };

            state.in_flight_requests.fetch_add(1, Ordering::AcqRel);
            let downloader_clone = Arc::clone(&downloader);
            let middlewares_clone = middlewares.clone();
            let res_tx_clone = res_tx.clone();
            let state_clone = Arc::clone(&state);
            let scheduler_clone = Arc::clone(&scheduler);
            let stats_clone = Arc::clone(&stats);
            let request_url_for_metrics = request.url.to_string();
            let _permit = permit;

            tasks.spawn(async move {
                let start_time = Instant::now();

                trace!("Processing request through middlewares: {}", request.url);
                let response = process_request_through_middlewares::<S, C>(
                    request,
                    &downloader_clone,
                    &middlewares_clone,
                    &scheduler_clone,
                    retry_release_permit,
                    &stats_clone,
                )
                .await;

                // Record response time for statistics
                let response_time = start_time.elapsed();
                stats_clone.record_request_time(&request_url_for_metrics, response_time);

                if let Ok(Some(final_response)) = response {
                    trace!("Sending response for URL: {}", final_response.url);
                    if res_tx_clone.send(final_response).await.is_err() {
                        error!("Response channel closed, cannot send parsed response.");
                    }
                }

                scheduler_clone.complete_request();
                state_clone
                    .in_flight_requests
                    .fetch_sub(1, Ordering::AcqRel);
                // Permit is automatically released when dropped
            });
        }

        trace!("Waiting for active download tasks to complete");
        while let Some(res) = tasks.join_next().await {
            if let Err(e) = res {
                error!("A download task failed: {:?}", e);
            } else {
                trace!("Download task completed successfully");
            }
        }
        trace!("Downloader task finished");
    })
}

async fn process_request_through_middlewares<S, C>(
    request: Request,
    downloader: &Arc<dyn Downloader<Client = C> + Send + Sync>,
    middlewares: &SharedMiddlewareManager<C>,
    scheduler: &Arc<Scheduler>,
    retry_release_permit: bool,
    stats: &Arc<StatCollector>,
) -> Result<Option<Response>, ()>
where
    S: crate::spider::Spider + 'static,
    S::Item: ScrapedItem,
    C: Send + Sync + Clone + 'static,
{
    trace!("Processing request through middlewares: {}", request.url);
    let original_request_url = request.url.clone();
    let mut early_returned_response: Option<Response> = None;

    let mut processed_request_opt = Some(request);

    match middlewares
        .process_request(
            downloader.client(),
            match processed_request_opt.take() {
                Some(req) => req,
                None => {
                    error!(
                        "Internal state error: request missing before middleware processing for URL {}",
                        original_request_url
                    );
                    return Ok(None);
                }
            },
        )
        .await
    {
        Ok(MiddlewareAction::Continue(req)) => {
            trace!("Request middleware continued with URL: {}", req.url);
            processed_request_opt = Some(req);
        }
        Ok(MiddlewareAction::Retry(req, delay)) => {
            let request_url = req.url.clone();
            debug!(
                "Request middleware scheduled retry for URL: {} after {:?}",
                request_url, delay
            );
            stats.increment_requests_retried();
            schedule_retry(
                Arc::clone(scheduler),
                *req,
                delay,
                retry_release_permit,
                Arc::clone(stats),
            )
            .await;
            return Ok(None);
        }
        Ok(MiddlewareAction::Drop) => {
            debug!(
                "Request dropped by middleware for URL: {}",
                original_request_url
            );
            stats.increment_requests_dropped();
            return Ok(None);
        }
        Ok(MiddlewareAction::ReturnResponse(resp)) => {
            trace!(
                "Request middleware returned cached response for URL: {}",
                resp.url
            );
            early_returned_response = Some(resp);
        }
        Err(e) => {
            error!(
                "Request middleware error for URL {}: {:?}",
                original_request_url, e
            );
            return Ok(None);
        }
    }

    // Download or use early response
    // If early_returned_response is Some, request was consumed by a middleware
    // If early_returned_response is None, processed_request_opt must contain the request
    let response = match early_returned_response {
        Some(resp) => {
            trace!("Using early returned response for URL: {}", resp.url);
            if resp.cached {
                stats.increment_responses_from_cache();
            }
            stats.increment_requests_succeeded();
            stats.increment_responses_received();
            stats.record_response_status(resp.status.as_u16());
            resp
        }
        None => {
            let request_for_download = match processed_request_opt {
                Some(req) => req,
                None => {
                    error!(
                        "Internal state error: request missing before download for URL {}",
                        original_request_url
                    );
                    return Ok(None);
                }
            };
            let request_url = request_for_download.url.clone();
            trace!("Downloading request for URL: {}", request_url);
            stats.increment_requests_sent();

            let download_result = downloader.download(request_for_download.clone()).await;

            match download_result {
                Ok(resp) => {
                    trace!("Download successful for URL: {}", resp.url);

                    stats.increment_requests_succeeded();
                    stats.increment_responses_received();
                    stats.record_response_status(resp.status.as_u16());
                    stats.add_bytes_downloaded(resp.body.len());
                    resp
                }
                Err(e) => {
                    error!("Download error for URL {}: {:?}", request_url, e);
                    return handle_download_error(
                        request_for_download,
                        e,
                        middlewares,
                        scheduler,
                        retry_release_permit,
                        stats,
                    )
                    .await;
                }
            }
        }
    };

    let original_request_url = response.request_from_response().url.clone();
    trace!(
        "Processing response through response middlewares for URL: {}",
        original_request_url
    );
    let processed_response = match middlewares.process_response(response).await {
        Ok(MiddlewareAction::Continue(res)) => {
            trace!("Response middleware continued for URL: {}", res.url);
            Some(res)
        }
        Ok(MiddlewareAction::Retry(request, delay)) => {
            let request_url = request.url.clone();
            debug!(
                "Response middleware scheduled retry for URL: {} after {:?}",
                request_url, delay
            );
            stats.increment_requests_retried();
            schedule_retry(
                Arc::clone(scheduler),
                *request,
                delay,
                retry_release_permit,
                Arc::clone(stats),
            )
            .await;
            return Err(());
        }
        Ok(MiddlewareAction::Drop) => {
            debug!(
                "Response dropped by middleware for URL: {}",
                original_request_url
            );
            stats.increment_requests_dropped();
            return Err(());
        }
        Ok(MiddlewareAction::ReturnResponse(_)) => {
            // This indicates the middleware has fully handled or consumed the response.
            // Effectively, the response is dropped from further processing by this chain.
            debug!(
                "ReturnResponse action encountered in process_response; this is unexpected and effectively drops the response for further processing for URL: {}",
                original_request_url
            );
            None
        }
        Err(e) => {
            error!(
                "Response middleware error for URL {}: {:?}",
                original_request_url, e
            );
            return Ok(None);
        }
    };

    // Mark the original request URL as visited after successful processing
    if let Some(ref response) = processed_response {
        let original_request = response.request_from_response();
        let fingerprint = original_request.fingerprint();
        trace!("Marking URL as visited: {}", original_request.url);
        if let Err(e) = scheduler.mark_visited(fingerprint.clone()).await {
            error!(
                "Failed to mark URL as visited (fingerprint: {}): {:?}",
                fingerprint, e
            );
        }
    }

    Ok(processed_response)
}

async fn handle_download_error<C>(
    request: Request,
    error: SpiderError,
    middlewares: &SharedMiddlewareManager<C>,
    scheduler: &Arc<Scheduler>,
    retry_release_permit: bool,
    stats: &Arc<StatCollector>,
) -> Result<Option<Response>, ()>
where
    C: Send + Sync + Clone + 'static,
{
    match middlewares.handle_error(&request, &error).await {
        Ok(MiddlewareAction::Continue(next_request)) => {
            debug!(
                "Error middleware continued request after download failure for URL: {}",
                next_request.url
            );
            if scheduler.requeue_request(next_request).await.is_err() {
                error!("Failed to re-enqueue continued request after download failure.");
                stats.increment_requests_failed();
            }
        }
        Ok(MiddlewareAction::Retry(next_request, delay)) => {
            let request_url = next_request.url.clone();
            debug!(
                "Error middleware scheduled retry for URL: {} after {:?}",
                request_url, delay
            );
            stats.increment_requests_retried();
            schedule_retry(
                Arc::clone(scheduler),
                *next_request,
                delay,
                retry_release_permit,
                Arc::clone(stats),
            )
            .await;
        }
        Ok(MiddlewareAction::Drop) => {
            debug!(
                "Request dropped by error middleware after download failure for URL: {}",
                request.url
            );
            stats.increment_requests_dropped();
        }
        Ok(MiddlewareAction::ReturnResponse(response)) => {
            debug!(
                "Error middleware returned a synthetic response for URL: {}",
                response.url
            );
            if response.cached {
                stats.increment_responses_from_cache();
            }
            stats.increment_requests_succeeded();
            stats.increment_responses_received();
            stats.record_response_status(response.status.as_u16());
            return Ok(Some(response));
        }
        Err(next_error) => {
            error!(
                "Download failure remained unhandled for URL {}: {:?}",
                request.url, next_error
            );
            stats.increment_requests_failed();
        }
    }

    Ok(None)
}

async fn schedule_retry(
    scheduler: Arc<Scheduler>,
    request: Request,
    delay: tokio::time::Duration,
    release_permit: bool,
    stats: Arc<StatCollector>,
) {
    if scheduler.is_shutting_down.load(Ordering::SeqCst) {
        debug!(
            "Skipping retry scheduling during shutdown for URL: {}",
            request.url
        );
        return;
    }

    let request_url = request.url.clone();
    if release_permit {
        stats.increment_requests_scheduled_for_retry();
        stats.add_retry_delay_in_flight(delay);
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            stats.remove_retry_delay_in_flight(delay);
            if scheduler.is_shutting_down.load(Ordering::SeqCst) {
                debug!(
                    "Skipping retried request re-enqueue during shutdown for URL: {}",
                    request_url
                );
                return;
            }
            if scheduler.requeue_request(request).await.is_err() {
                error!(
                    "Failed to re-enqueue retried request for URL: {}",
                    request_url
                );
            }
        });
    } else {
        tokio::time::sleep(delay).await;
        if scheduler.is_shutting_down.load(Ordering::SeqCst) {
            debug!(
                "Skipping retried request re-enqueue during shutdown for URL: {}",
                request_url
            );
            return;
        }
        if scheduler.requeue_request(request).await.is_err() {
            error!(
                "Failed to re-enqueue retried request for URL: {}",
                request_url
            );
        }
    }
}

#[cfg(feature = "test-support")]
pub async fn test_process_request_through_middlewares<S, C>(
    request: Request,
    downloader: &Arc<dyn Downloader<Client = C> + Send + Sync>,
    middlewares: &SharedMiddlewareManager<C>,
    scheduler: &Arc<Scheduler>,
    retry_release_permit: bool,
    stats: &Arc<StatCollector>,
) -> Result<Option<Response>, ()>
where
    S: crate::spider::Spider + 'static,
    S::Item: ScrapedItem,
    C: Send + Sync + Clone + 'static,
{
    process_request_through_middlewares::<S, C>(
        request,
        downloader,
        middlewares,
        scheduler,
        retry_release_permit,
        stats,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::schedule_retry;
    use crate::Scheduler;
    use crate::stats::StatCollector;
    use spider_util::request::Request;
    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use std::time::Duration;
    use url::Url;

    #[tokio::test]
    async fn schedule_retry_noops_when_scheduler_is_already_shutting_down() {
        let (scheduler, _rx) = Scheduler::new(None, 32);
        scheduler.shutdown().await.unwrap();

        let stats = Arc::new(StatCollector::new());
        let request = Request::new(Url::parse("https://example.com/retry").unwrap());

        schedule_retry(
            Arc::clone(&scheduler),
            request,
            Duration::from_millis(10),
            true,
            Arc::clone(&stats),
        )
        .await;

        tokio::time::sleep(Duration::from_millis(20)).await;

        assert_eq!(
            stats.requests_scheduled_for_retry.load(Ordering::Acquire),
            0
        );
        assert_eq!(stats.retry_delay_in_flight_ms.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn scheduled_retry_does_not_requeue_after_shutdown_begins() {
        let (scheduler, rx) = Scheduler::new(None, 32);
        let stats = Arc::new(StatCollector::new());
        let request = Request::new(Url::parse("https://example.com/retry-late").unwrap());

        schedule_retry(
            Arc::clone(&scheduler),
            request,
            Duration::from_millis(30),
            true,
            Arc::clone(&stats),
        )
        .await;

        tokio::time::sleep(Duration::from_millis(5)).await;
        scheduler.shutdown().await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        assert!(rx.try_recv().ok().flatten().is_none());
        assert_eq!(
            stats.requests_scheduled_for_retry.load(Ordering::Acquire),
            1
        );
        assert_eq!(stats.retry_delay_in_flight_ms.load(Ordering::Acquire), 0);
    }
}
