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
use crate::spider::{ParseContext, Spider};
use crate::state::CrawlerState;
use crate::stats::StatCollector;
use crate::{
    config::DiscoveryConfig,
    discovery::{attach_page_metadata, discover_response, discovery_rule_meta_key},
};
use kanal::{AsyncReceiver, AsyncSender};
use log::{debug, error, info, trace, warn};
use spider_util::error::SpiderError;
use spider_util::item::{ParseOutput, ParseSink, ScrapedItem};
use spider_util::request::Request;
use spider_util::response::Response;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tokio::time::Instant;

struct RuntimeParseSink<S>
where
    S: Spider + 'static,
    S::Item: ScrapedItem,
{
    scheduler: Arc<Scheduler>,
    item_tx: AsyncSender<S::Item>,
    state: Arc<CrawlerState>,
    item_limit: Option<usize>,
    stats: Arc<StatCollector>,
}

impl<S> RuntimeParseSink<S>
where
    S: Spider + 'static,
    S::Item: ScrapedItem,
{
    fn new(
        scheduler: Arc<Scheduler>,
        item_tx: AsyncSender<S::Item>,
        state: Arc<CrawlerState>,
        item_limit: Option<usize>,
        stats: Arc<StatCollector>,
    ) -> Self {
        Self {
            scheduler,
            item_tx,
            state,
            item_limit,
            stats,
        }
    }

    fn item_limit_shutdown(&self) -> bool {
        self.state.item_limit_reached.load(Ordering::SeqCst)
            && self.scheduler.is_shutting_down.load(Ordering::SeqCst)
    }

    async fn trigger_item_limit_shutdown(&self) {
        if self
            .state
            .item_limit_reached
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            info!("Item limit reached, initiating scheduler shutdown");
            if let Err(err) = self.scheduler.shutdown().await {
                error!(
                    "Failed to shut down scheduler after reaching item limit: {:?}",
                    err
                );
            }
        }
    }
}

#[async_trait::async_trait]
impl<S> ParseSink<S::Item> for RuntimeParseSink<S>
where
    S: Spider + 'static,
    S::Item: ScrapedItem,
{
    async fn add_item(&self, item: S::Item) -> Result<(), SpiderError> {
        if self.scheduler.is_shutting_down.load(Ordering::SeqCst) {
            if self.item_limit_shutdown() {
                self.state
                    .shutdown_dropped_items
                    .fetch_add(1, Ordering::AcqRel);
            }
            return Ok(());
        }

        if let Some(limit) = self.item_limit
            && self
                .state
                .admitted_items
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                    (current < limit).then_some(current + 1)
                })
                .is_err()
        {
            self.trigger_item_limit_shutdown().await;
            self.state
                .shutdown_dropped_items
                .fetch_add(1, Ordering::AcqRel);
            return Ok(());
        }

        if self.item_tx.is_closed() {
            if !self.item_limit_shutdown() {
                warn!("Item channel is closed, stopping item processing");
            }
            if self.item_limit.is_some() {
                self.state.admitted_items.fetch_sub(1, Ordering::AcqRel);
            }
            if self.item_limit_shutdown() {
                self.state
                    .shutdown_dropped_items
                    .fetch_add(1, Ordering::AcqRel);
            }
            return Ok(());
        }

        self.stats.record_current_item_preview(&item);
        self.state.processing_items.fetch_add(1, Ordering::AcqRel);

        if self.item_tx.send(item).await.is_err() {
            if !self.item_limit_shutdown() {
                error!("Failed to send item to processing channel");
            }
            self.state.processing_items.fetch_sub(1, Ordering::AcqRel);
            if self.item_limit.is_some() {
                self.state.admitted_items.fetch_sub(1, Ordering::AcqRel);
            }
            if self.item_limit_shutdown() {
                self.state
                    .shutdown_dropped_items
                    .fetch_add(1, Ordering::AcqRel);
            }
            return Ok(());
        }

        self.stats.add_items_scraped(1);
        if matches!(
            self.item_limit,
            Some(limit) if self.state.admitted_items.load(Ordering::Acquire) >= limit
        ) {
            self.trigger_item_limit_shutdown().await;
        }

        Ok(())
    }

    async fn add_request(&self, request: Request) -> Result<(), SpiderError> {
        if self.scheduler.is_shutting_down.load(Ordering::SeqCst) {
            if self.item_limit_shutdown() {
                self.state
                    .shutdown_skipped_requests
                    .fetch_add(1, Ordering::AcqRel);
            }
            return Ok(());
        }

        match self.scheduler.enqueue_request(request).await {
            Ok(()) => {
                self.stats.increment_requests_enqueued();
            }
            Err(err) => {
                if self.scheduler.is_shutting_down.load(Ordering::SeqCst) {
                    if self.item_limit_shutdown() {
                        self.state
                            .shutdown_skipped_requests
                            .fetch_add(1, Ordering::AcqRel);
                    }
                } else {
                    error!("Failed to enqueue request: {:?}", err);
                }
            }
        }

        Ok(())
    }
}

#[allow(clippy::too_many_arguments)]
fn spawn_parser_worker<S>(
    internal_parse_rx: AsyncReceiver<Response>,
    spider: Arc<S>,
    spider_state: Arc<S::State>,
    scheduler: Arc<Scheduler>,
    item_tx: AsyncSender<S::Item>,
    state: Arc<CrawlerState>,
    discovery_config: DiscoveryConfig,
    _output_batch_size: usize,
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
            if response.get_meta(discovery_rule_meta_key()).is_none()
                && let Some(rule_name) = discovery.rule_name.as_ref()
            {
                response.insert_meta(
                    discovery_rule_meta_key().to_string(),
                    serde_json::Value::String(rule_name.clone()),
                );
            }

            let start_time = Instant::now();
            let output = ParseOutput::from_sink(Arc::new(RuntimeParseSink::<S>::new(
                scheduler.clone(),
                item_tx.clone(),
                state.clone(),
                item_limit,
                stats.clone(),
            )));
            let cx = ParseContext::new(response, spider_state.as_ref(), output.clone());
            let parse_output = spider.parse(cx).await;
            let elapsed = start_time.elapsed();

            // Record parsing time for performance metrics
            stats.record_parsing_time(elapsed);

            match parse_output {
                Ok(()) => {
                    if !discovery.requests.is_empty() {
                        if let Err(err) = output.add_requests(discovery.requests).await {
                            error!("Failed to emit discovery requests: {:?}", err);
                        }
                    }
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
    _output_batch_size: usize,
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
            _output_batch_size,
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
