//! Response parsing stage for the crawler engine.
//!
//! This module fans downloaded responses out to parser workers, runs
//! [`Spider::parse`](crate::spider::Spider::parse), and forwards emitted items
//! and follow-up requests deeper into the runtime.
//!
//! ## Example
//!
//! ```rust,ignore
//! use spider_core::engine::spawn_parser_task;
//! use spider_util::response::Response;
//! use spider_util::item::ScrapedItem;
//! use kanal::{AsyncReceiver, AsyncSender};
//! use std::sync::Arc;
//! use tokio::sync::Mutex;
//!
//! // The parser task is typically spawned internally by the crawler
//! // but can be used directly if needed for custom implementations
//! let parser_handle = spawn_parser_task(
//!     scheduler,
//!     spider,
//!     state,
//!     response_receiver,
//!     item_sender,
//!     num_parser_workers,
//!     stats,
//! );
//! ```

use crate::scheduler::Scheduler;
use crate::spider::Spider;
use crate::state::CrawlerState;
use crate::stats::StatCollector;
use crate::{
    config::DiscoveryConfig,
    discovery::{attach_page_metadata, discover_response},
};
use kanal::{AsyncReceiver, AsyncSender};
use log::{debug, error, info, trace, warn};
use spider_util::item::{ParseOutput, ScrapedItem};
use spider_util::response::Response;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tokio::time::Instant;

#[allow(clippy::too_many_arguments)]
fn spawn_parser_worker<S>(
    internal_parse_rx: AsyncReceiver<Response>,
    spider: Arc<S>,
    spider_state: Arc<S::State>,
    scheduler: Arc<Scheduler>,
    item_tx: AsyncSender<S::Item>,
    state: Arc<CrawlerState>,
    discovery_config: DiscoveryConfig,
    output_batch_size: usize,
    item_limit: Option<usize>,
    stats: Arc<StatCollector>,
) where
    S: Spider + 'static,
    S::Item: ScrapedItem,
{
    tokio::spawn(async move {
        while let Ok(mut response) = internal_parse_rx.recv().await {
            debug!("Parsing response from {}", response.url);

            let discovery = discover_response(&response, &discovery_config);
            if let Some(metadata) = discovery.metadata.as_ref() {
                attach_page_metadata(&mut response, metadata);
            }

            let start_time = Instant::now();
            let parse_output = spider.parse(response, &spider_state).await;
            let elapsed = start_time.elapsed();

            // Record parsing time for performance metrics
            stats.record_parsing_time(elapsed);

            match parse_output {
                Ok(mut outputs) => {
                    if !discovery.requests.is_empty() {
                        outputs.add_requests(discovery.requests);
                    }
                    process_crawl_outputs::<S>(
                        outputs,
                        scheduler.clone(),
                        item_tx.clone(),
                        state.clone(),
                        output_batch_size,
                        item_limit,
                        stats.clone(),
                    )
                    .await;
                }
                Err(e) => error!("Spider parsing error: {:?}", e),
            }

            state.parsing_responses.fetch_sub(1, Ordering::AcqRel);
        }
    });
}

#[allow(clippy::too_many_arguments)]
pub fn spawn_parser_task<S>(
    scheduler: Arc<Scheduler>,
    spider: Arc<S>,
    spider_state: Arc<S::State>,
    state: Arc<CrawlerState>,
    res_rx: AsyncReceiver<Response>,
    item_tx: AsyncSender<S::Item>,
    parser_workers: usize,
    discovery_config: DiscoveryConfig,
    output_batch_size: usize,
    item_backpressure_threshold: usize,
    item_limit: Option<usize>,
    stats: Arc<StatCollector>,
) -> tokio::task::JoinHandle<()>
where
    S: Spider + 'static,
    S::Item: ScrapedItem,
{
    let (internal_parse_tx, internal_parse_rx) =
        kanal::bounded_async::<Response>(parser_workers * 2);

    for _ in 0..parser_workers {
        spawn_parser_worker::<S>(
            internal_parse_rx.clone(),
            Arc::clone(&spider),
            Arc::clone(&spider_state),
            Arc::clone(&scheduler),
            item_tx.clone(),
            Arc::clone(&state),
            discovery_config.clone(),
            output_batch_size,
            item_limit,
            Arc::clone(&stats),
        );
    }

    tokio::spawn(async move {
        info!(
            "Response parser coordinator started with {} workers",
            parser_workers
        );
        while let Ok(response) = res_rx.recv().await {
            trace!("Received response for parsing from URL: {}", response.url);

            // Apply backpressure if item channel is filling up
            if item_tx.len() > item_backpressure_threshold {
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }

            state.parsing_responses.fetch_add(1, Ordering::AcqRel);
            if internal_parse_tx.send(response).await.is_err() {
                error!("Internal parse channel closed, cannot send response to parser worker.");
                state.parsing_responses.fetch_sub(1, Ordering::AcqRel);
            }
        }

        trace!("Closing internal parse channel");
        drop(internal_parse_tx);
        info!("Response parser coordinator finished");
    })
}

pub async fn process_crawl_outputs<S>(
    outputs: ParseOutput<S::Item>,
    scheduler: Arc<Scheduler>,
    item_tx: AsyncSender<S::Item>,
    state: Arc<CrawlerState>,
    output_batch_size: usize,
    item_limit: Option<usize>,
    stats: Arc<StatCollector>,
) where
    S: Spider + 'static,
    S::Item: ScrapedItem,
{
    let (items, requests) = outputs.into_parts();
    let items_len = items.len();
    let requests_len = requests.len();

    if requests_len > 0 || items_len > 0 {
        debug!(
            "Processing {} requests and {} items from spider output.",
            requests_len, items_len
        );
    } else {
        trace!("Spider output contained no requests or items");
    }

    let mut item_error_total = 0;
    let mut items_sent = 0usize;
    let mut item_limit_hit = false;
    let batch_size = output_batch_size.max(1);
    let mut item_batch_len = 0usize;
    for item in items {
        if scheduler.is_shutting_down.load(Ordering::SeqCst) {
            item_error_total += 1;
            continue;
        }

        if let Some(limit) = item_limit
            && state
                .admitted_items
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                    (current < limit).then_some(current + 1)
                })
                .is_err()
        {
            item_limit_hit = true;
            break;
        }

        item_batch_len += 1;
        if item_tx.is_closed() {
            warn!("Item channel is closed, stopping item processing");
            item_error_total += 1;
            if item_limit.is_some() {
                state.admitted_items.fetch_sub(1, Ordering::AcqRel);
            }
        } else {
            stats.record_current_item_preview(&item);
            state.processing_items.fetch_add(1, Ordering::AcqRel);
            if item_tx.send(item).await.is_err() {
                error!("Failed to send item to processing channel");
                item_error_total += 1;
                state.processing_items.fetch_sub(1, Ordering::AcqRel);
                if item_limit.is_some() {
                    state.admitted_items.fetch_sub(1, Ordering::AcqRel);
                }
            } else {
                items_sent += 1;
                if matches!(item_limit, Some(limit) if state.admitted_items.load(Ordering::Acquire) >= limit)
                {
                    item_limit_hit = true;
                }
            }
        }

        if item_batch_len == batch_size {
            item_batch_len = 0;
        }
    }

    if items_sent > 0 {
        stats.add_items_scraped(items_sent);
    }

    if item_error_total > 0 {
        warn!(
            "Failed to send {} of {} scraped items.",
            item_error_total, items_len
        );
    } else if items_sent > 0 {
        debug!(
            "Successfully sent {} scraped items for processing",
            items_sent
        );
    }

    if item_limit_hit
        && state
            .item_limit_reached
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    {
        info!("Item limit reached, initiating scheduler shutdown");
        if let Err(err) = scheduler.shutdown().await {
            error!(
                "Failed to shut down scheduler after reaching item limit: {:?}",
                err
            );
        }
    }

    let mut request_error_total = 0;

    if scheduler.is_shutting_down.load(Ordering::SeqCst) {
        request_error_total = requests_len;
        if requests_len > 0 {
            debug!("Scheduler is shutting down, skipping remaining requests");
        }
    } else {
        let mut request_batch = Vec::with_capacity(batch_size);
        for request in requests {
            request_batch.push(request);
            if request_batch.len() < batch_size {
                continue;
            }

            if scheduler.is_shutting_down.load(Ordering::SeqCst) {
                debug!("Scheduler is shutting down, skipping remaining requests");
                request_error_total += request_batch.len();
                request_batch.clear();
                continue;
            }

            let current_batch = std::mem::take(&mut request_batch);
            match scheduler.enqueue_requests_batch(current_batch).await {
                Ok(enqueued) => {
                    for _ in 0..enqueued {
                        stats.increment_requests_enqueued();
                    }
                    request_error_total += batch_size.saturating_sub(enqueued);
                }
                Err(e) => {
                    if scheduler.is_shutting_down.load(Ordering::SeqCst) {
                        debug!("Scheduler is shutting down, skipping remaining requests");
                        request_error_total += batch_size;
                        continue;
                    }

                    error!("Failed to enqueue request batch: {:?}", e);
                    request_error_total += batch_size;
                }
            }
        }

        if !request_batch.is_empty() {
            if scheduler.is_shutting_down.load(Ordering::SeqCst) {
                debug!("Scheduler is shutting down, skipping remaining requests");
                request_error_total += request_batch.len();
            } else {
                let remaining = request_batch.len();
                match scheduler.enqueue_requests_batch(request_batch).await {
                    Ok(enqueued) => {
                        for _ in 0..enqueued {
                            stats.increment_requests_enqueued();
                        }
                        request_error_total += remaining.saturating_sub(enqueued);
                    }
                    Err(e) => {
                        if scheduler.is_shutting_down.load(Ordering::SeqCst) {
                            debug!("Scheduler is shutting down, skipping remaining requests");
                            request_error_total += remaining;
                        } else {
                            error!("Failed to enqueue request batch: {:?}", e);
                            request_error_total += remaining;
                        }
                    }
                }
            }
        }
    }

    if request_error_total > 0 {
        warn!(
            "Failed to enqueue {} of {} requests.",
            request_error_total, requests_len
        );
    } else if requests_len > 0 {
        debug!("Successfully enqueued all {} requests", requests_len);
    }
}
