//! # Response Parser Module
//!
//! Contains the response parsing functionality for the crawler.
//!
//! ## Overview
//!
//! The response parser module handles the processing of HTTP responses received
//! from the downloader. It orchestrates the parsing of responses through the
//! spider's logic, extracting scraped items and new requests to follow.
//! The module implements a concurrent processing model with multiple parser
//! workers to efficiently handle the parsing workload.
//!
//! ## Key Components
//!
//! - **spawn_parser_task**: Creates the main parser coordinator task and worker tasks
//! - **process_crawl_outputs**: Handles the distribution of spider outputs (items and requests)
//! - **Parser Workers**: Multiple concurrent tasks that process individual responses
//! - **Coordinator**: Manages the distribution of responses to available workers
//!
//! ## Architecture
//!
//! The parser uses a coordinator-worker pattern where a coordinator task receives
//! responses from the downloader and distributes them to multiple worker tasks.
//! Each worker task processes responses through the spider's parsing logic,
//! extracting items and new requests. This design allows for concurrent parsing
//! while maintaining proper coordination and backpressure handling.
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
use kanal::{AsyncReceiver, AsyncSender};
use log::{debug, error, info, trace, warn};
use spider_util::item::{ParseOutput, ScrapedItem};
use spider_util::response::Response;
use std::cmp::max;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time::Instant;

fn spawn_parser_worker<S>(
    internal_parse_rx: AsyncReceiver<Response>,
    spider: Arc<S>,
    spider_state: Arc<S::State>,
    scheduler: Arc<Scheduler>,
    item_tx: AsyncSender<S::Item>,
    state: Arc<CrawlerState>,
    stats: Arc<StatCollector>,
) where
    S: Spider + 'static,
    S::Item: ScrapedItem,
{
    tokio::spawn(async move {
        while let Ok(response) = internal_parse_rx.recv().await {
            debug!("Parsing response from {}", response.url);
            state.parsing_responses.fetch_add(1, Ordering::AcqRel);

            let start_time = Instant::now();
            let parse_output = spider.parse(response, &spider_state).await;
            let elapsed = start_time.elapsed();

            // Record parsing time for performance metrics
            stats.record_parsing_time(elapsed);

            match parse_output {
                Ok(outputs) => {
                    process_crawl_outputs::<S>(
                        outputs,
                        scheduler.clone(),
                        item_tx.clone(),
                        state.clone(),
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
    stats: Arc<StatCollector>,
) -> tokio::task::JoinHandle<()>
where
    S: Spider + 'static,
    S::Item: ScrapedItem,
{
    let (internal_parse_tx, internal_parse_rx) =
        kanal::bounded_async::<Response>(parser_workers * 2);

    // Track worker count dynamically
    let current_worker_count = Arc::new(RwLock::new(parser_workers));

    // Spawn initial parsing worker tasks
    for _ in 0..parser_workers {
        spawn_parser_worker::<S>(
            internal_parse_rx.clone(),
            Arc::clone(&spider),
            Arc::clone(&spider_state),
            Arc::clone(&scheduler),
            item_tx.clone(),
            Arc::clone(&state),
            Arc::clone(&stats),
        );
    }

    // Dynamic worker scaling task
    let scaling_scheduler = Arc::clone(&scheduler);
    let scaling_spider = Arc::clone(&spider);
    let scaling_spider_state = Arc::clone(&spider_state);
    let scaling_state = Arc::clone(&state);
    let scaling_item_tx = item_tx.clone();
    let scaling_stats = Arc::clone(&stats);
    let scaling_worker_count = Arc::clone(&current_worker_count);
    let scaling_internal_parse_rx = internal_parse_rx.clone();

    tokio::spawn(async move {
        let mut last_scale_check = Instant::now();

        loop {
            tokio::time::sleep(Duration::from_millis(100)).await;

            // Scale workers based on queue depth and processing times
            if last_scale_check.elapsed() >= Duration::from_secs(1) {
                let current_workers = *scaling_worker_count.read().await;
                let queue_depth = scaling_internal_parse_rx.len();

                // Scale up if queue is backing up significantly
                if queue_depth > current_workers * 3 && current_workers < parser_workers * 4 {
                    let mut worker_count = scaling_worker_count.write().await;
                    if *worker_count < parser_workers * 4 {
                        *worker_count += 1;

                        // Spawn a new worker
                        let internal_parse_rx_clone = scaling_internal_parse_rx.clone();
                        let spider_clone = Arc::clone(&scaling_spider);
                        let spider_state_clone = Arc::clone(&scaling_spider_state);
                        let scheduler_clone = Arc::clone(&scaling_scheduler);
                        let item_tx_clone = scaling_item_tx.clone();
                        let state_clone = Arc::clone(&scaling_state);
                        let stats_clone = Arc::clone(&scaling_stats);
                        spawn_parser_worker::<S>(
                            internal_parse_rx_clone,
                            spider_clone,
                            spider_state_clone,
                            scheduler_clone,
                            item_tx_clone,
                            state_clone,
                            stats_clone,
                        );

                        trace!("Scaled up parser workers to: {}", *worker_count);
                    }
                }

                last_scale_check = Instant::now();
            }

            if scaling_scheduler.is_shutting_down.load(Ordering::SeqCst) {
                break;
            }
        }
    });

    tokio::spawn(async move {
        trace!(
            "Response parser coordinator started with {} workers (dynamic scaling enabled)",
            parser_workers
        );
        while let Ok(response) = res_rx.recv().await {
            trace!("Received response for parsing from URL: {}", response.url);

            // Apply backpressure if item channel is filling up
            if item_tx.len() > parser_workers * max(2, parser_workers / 2) {
                trace!(
                    "Applying backpressure to parser, item channel occupancy: {}",
                    item_tx.len()
                );
                tokio::time::sleep(Duration::from_millis(5)).await;
            }

            state.parsing_responses.fetch_add(1, Ordering::AcqRel);
            if internal_parse_tx.send(response).await.is_err() {
                error!("Internal parse channel closed, cannot send response to parser worker.");
            }
            state.parsing_responses.fetch_sub(1, Ordering::AcqRel);
        }

        trace!("Closing internal parse channel");
        drop(internal_parse_tx);
        trace!("Response parser coordinator finished");
    })
}

pub async fn process_crawl_outputs<S>(
    outputs: ParseOutput<S::Item>,
    scheduler: Arc<Scheduler>,
    item_tx: AsyncSender<S::Item>,
    state: Arc<CrawlerState>,
    stats: Arc<StatCollector>,
) where
    S: Spider + 'static,
    S::Item: ScrapedItem,
{
    let (items, requests) = outputs.into_parts();
    let items_len = items.len();
    let requests_len = requests.len();

    if requests_len > 0 || items_len > 0 {
        info!(
            "Processing {} requests and {} items from spider output.",
            requests_len, items_len
        );
    } else {
        trace!("Spider output contained no requests or items");
    }

    if items_len > 0 {
        stats.add_items_scraped(items_len);
    }

    let mut request_error_total = 0;

    // Process all requests first without delays
    for (idx, request) in requests.into_iter().enumerate() {
        trace!(
            "Processing request {} of {} from spider output: {}",
            idx + 1,
            requests_len,
            request.url
        );

        if scheduler.is_shutting_down.load(Ordering::SeqCst) {
            debug!(
                "Scheduler is shutting down, skipping request: {}",
                request.url
            );
            request_error_total += 1;
            continue;
        }

        match scheduler.enqueue_request(request).await {
            Ok(_) => {
                trace!("Successfully enqueued request");
                stats.increment_requests_enqueued();
            }
            Err(e) => {
                if scheduler.is_shutting_down.load(Ordering::SeqCst) {
                    debug!("Scheduler is shutting down, skipping remaining requests");
                    request_error_total += 1;
                    continue;
                }

                error!("Failed to enqueue request: {:?}", e);
                request_error_total += 1;
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

    let mut item_error_total = 0;
    for (idx, item) in items.into_iter().enumerate() {
        trace!(
            "Processing item {} of {} from spider output",
            idx + 1,
            items_len
        );

        if item_tx.is_closed() {
            warn!("Item channel is closed, stopping item processing");
            item_error_total += items_len - idx;
            break;
        }

        state.processing_items.fetch_add(1, Ordering::AcqRel);
        if item_tx.send(item).await.is_err() {
            error!("Failed to send item to processing channel");
            item_error_total += 1;
            state.processing_items.fetch_sub(1, Ordering::AcqRel);
        }
    }

    if item_error_total > 0 {
        warn!(
            "Failed to send {} of {} scraped items.",
            item_error_total, items_len
        );
    } else if items_len > 0 {
        debug!("Successfully sent all {} items for processing", items_len);
    }
}
