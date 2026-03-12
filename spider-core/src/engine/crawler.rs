//! The core Crawler implementation for the `spider-lib` framework.
//!
//! This module defines the `Crawler` struct, which acts as the central orchestrator
//! for the web scraping process. It ties together the scheduler, downloader,
//! middlewares, spiders, and item pipelines to execute a crawl. The crawler
//! manages the lifecycle of requests and items, handles concurrency, supports
//! checkpointing for fault tolerance, and collects statistics for monitoring.
//!
//! It utilizes a task-based asynchronous model, spawning distinct tasks for
//! handling initial requests, downloading web pages, parsing responses, and
//! processing scraped items.

use crate::Downloader;
use crate::config::CrawlerConfig;
use crate::engine::CrawlerContext;
use crate::scheduler::Scheduler;
use crate::spider::Spider;
use crate::state::CrawlerState;
use crate::stats::StatCollector;
use anyhow::Result;
#[cfg(feature = "live-stats")]
use crossterm::{
    cursor::{Hide, MoveToColumn, MoveUp, Show},
    execute, queue,
    terminal::{Clear, ClearType, size},
};
use futures_util::future::join_all;
use kanal::{AsyncReceiver, bounded_async};
use log::{debug, error, info, trace, warn};
use spider_middleware::middleware::Middleware;
use spider_pipeline::pipeline::Pipeline;
use spider_util::error::SpiderError;
use spider_util::item::ScrapedItem;
use spider_util::request::Request;

#[cfg(feature = "checkpoint")]
use crate::checkpoint::save_checkpoint;
#[cfg(feature = "checkpoint")]
use crate::config::CheckpointConfig;

#[cfg(feature = "live-stats")]
use std::io::{IsTerminal, Write};
use std::sync::Arc;
use std::time::Duration;

#[cfg(feature = "cookie-store")]
use tokio::sync::RwLock;
#[cfg(feature = "live-stats")]
use tokio::sync::oneshot;
#[cfg(feature = "live-stats")]
use tokio::time::MissedTickBehavior;

#[cfg(feature = "cookie-store")]
use cookie_store::CookieStore;

/// The central orchestrator for the web scraping process, handling requests, responses, items, concurrency, checkpointing, and statistics collection.
pub struct Crawler<S: Spider, C> {
    scheduler: Arc<Scheduler>,
    req_rx: AsyncReceiver<Request>,
    stats: Arc<StatCollector>,
    downloader: Arc<dyn Downloader<Client = C> + Send + Sync>,
    middlewares: Vec<Box<dyn Middleware<C> + Send + Sync>>,
    spider: Arc<S>,
    spider_state: Arc<S::State>,
    pipelines: Vec<Box<dyn Pipeline<S::Item>>>,
    config: CrawlerConfig,
    #[cfg(feature = "checkpoint")]
    checkpoint_config: CheckpointConfig,
    #[cfg(feature = "cookie-store")]
    pub cookie_store: Arc<RwLock<CookieStore>>,
}

impl<S, C> Crawler<S, C>
where
    S: Spider + 'static,
    S::Item: ScrapedItem,
    C: Send + Sync + Clone + 'static,
{
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        scheduler: Arc<Scheduler>,
        req_rx: AsyncReceiver<Request>,
        downloader: Arc<dyn Downloader<Client = C> + Send + Sync>,
        middlewares: Vec<Box<dyn Middleware<C> + Send + Sync>>,
        spider: S,
        pipelines: Vec<Box<dyn Pipeline<S::Item>>>,
        config: CrawlerConfig,
        #[cfg(feature = "checkpoint")] checkpoint_config: CheckpointConfig,
        stats: Arc<StatCollector>,
        #[cfg(feature = "cookie-store")] cookie_store: Arc<tokio::sync::RwLock<CookieStore>>,
    ) -> Self {
        Crawler {
            scheduler,
            req_rx,
            stats,
            downloader,
            middlewares,
            spider: Arc::new(spider),
            spider_state: Arc::new(S::State::default()),
            pipelines,
            config,
            #[cfg(feature = "checkpoint")]
            checkpoint_config,
            #[cfg(feature = "cookie-store")]
            cookie_store,
        }
    }

    pub async fn start_crawl(self) -> Result<(), SpiderError> {
        info!(
            "Crawler starting crawl with configuration: max_concurrent_downloads={}, parser_workers={}, max_concurrent_pipelines={}",
            self.config.max_concurrent_downloads,
            self.config.parser_workers,
            self.config.max_concurrent_pipelines
        );

        let Crawler {
            scheduler,
            req_rx,
            stats,
            downloader,
            middlewares,
            spider,
            spider_state,
            pipelines,
            config,
            #[cfg(feature = "checkpoint")]
            checkpoint_config,
            #[cfg(feature = "cookie-store")]
                cookie_store: _,
        } = self;

        let state = CrawlerState::new();
        let pipelines = Arc::new(pipelines);

        // Create aggregated context for efficient sharing across tasks
        let ctx = CrawlerContext::new(
            Arc::clone(&scheduler),
            Arc::clone(&stats),
            Arc::clone(&spider),
            Arc::clone(&spider_state),
            Arc::clone(&pipelines),
        );

        let channel_capacity = std::cmp::max(
            config.max_concurrent_downloads * 3,
            config.parser_workers * config.max_concurrent_pipelines * 2,
        )
        .max(config.channel_capacity);

        trace!(
            "Creating communication channels with capacity: {}",
            channel_capacity
        );
        let (res_tx, res_rx) = bounded_async(channel_capacity);
        let (item_tx, item_rx) = bounded_async(channel_capacity);

        trace!("Spawning initial requests task");
        let init_task = spawn_init_task(ctx.clone());

        trace!("Initializing middleware manager");
        let middlewares = super::SharedMiddlewareManager::new(middlewares);

        trace!("Spawning downloader task");
        let downloader_handle = super::spawn_downloader_task::<S, C>(
            Arc::clone(&ctx.scheduler),
            req_rx,
            downloader,
            middlewares,
            state.clone(),
            res_tx.clone(),
            config.max_concurrent_downloads,
            config.response_backpressure_threshold.max(1),
            config.retry_release_permit,
            Arc::clone(&ctx.stats),
        );

        trace!("Spawning parser task");
        let parser_handle = super::spawn_parser_task::<S>(
            Arc::clone(&ctx.scheduler),
            Arc::clone(&ctx.spider),
            Arc::clone(&ctx.spider_state),
            state.clone(),
            res_rx,
            item_tx.clone(),
            config.parser_workers,
            config.output_batch_size.max(1),
            config.item_backpressure_threshold.max(1),
            Arc::clone(&ctx.stats),
        );

        trace!("Spawning item processor task");
        let processor_handle = super::spawn_item_processor_task::<S>(
            state.clone(),
            item_rx,
            Arc::clone(&ctx.pipelines),
            config.max_concurrent_pipelines,
            Arc::clone(&ctx.stats),
        );

        #[cfg(feature = "live-stats")]
        let mut live_stats_task: Option<(
            oneshot::Sender<()>,
            tokio::task::JoinHandle<()>,
        )> = if config.live_stats && std::io::stdout().is_terminal() {
            let (stop_tx, stop_rx) = oneshot::channel();
            let stats_for_live = Arc::clone(&ctx.stats);
            let interval = config.live_stats_interval;
            let handle = tokio::spawn(async move {
                run_live_stats(stats_for_live, interval, stop_rx).await;
            });
            Some((stop_tx, handle))
        } else {
            None
        };
        #[cfg(not(feature = "live-stats"))]
        let mut live_stats_task: Option<((), ())> = None;

        #[cfg(feature = "checkpoint")]
        {
            if let (Some(path), Some(interval)) =
                (&checkpoint_config.path, checkpoint_config.interval)
            {
                let scheduler_cp = Arc::clone(&ctx.scheduler);
                let pipelines_cp = Arc::clone(&ctx.pipelines);
                let path_cp = path.clone();

                #[cfg(feature = "cookie-store")]
                let cookie_store_cp = self.cookie_store.clone();

                #[cfg(not(feature = "cookie-store"))]
                let _cookie_store_cp = ();

                trace!(
                    "Starting periodic checkpoint task with interval: {:?}",
                    interval
                );
                tokio::spawn(async move {
                    let mut interval_timer = tokio::time::interval(interval);
                    interval_timer.tick().await;
                    loop {
                        tokio::select! {
                            _ = interval_timer.tick() => {
                                trace!("Checkpoint timer ticked, creating snapshot");
                                if let Ok(scheduler_checkpoint) = scheduler_cp.snapshot().await {
                                    debug!("Scheduler snapshot created, saving checkpoint to {:?}", path_cp);

                                    #[cfg(feature = "cookie-store")]
                                    let save_result = save_checkpoint::<S>(&path_cp, scheduler_checkpoint, &pipelines_cp, &cookie_store_cp).await;

                                    #[cfg(not(feature = "cookie-store"))]
                                    let save_result = save_checkpoint::<S>(&path_cp, scheduler_checkpoint, &pipelines_cp, &()).await;

                                    if let Err(e) = save_result {
                                        error!("Periodic checkpoint save failed: {}", e);
                                    } else {
                                        debug!("Periodic checkpoint saved successfully to {:?}", path_cp);
                                    }
                                } else {
                                    warn!("Failed to create scheduler snapshot for checkpoint");
                                }
                            }
                        }
                    }
                });
            }
        }

        let interrupted = tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                info!("Ctrl-C received, initiating graceful shutdown.");
                if let Err(e) = scheduler.shutdown().await {
                    error!("Error during scheduler shutdown: {}", e);
                } else {
                    debug!("Scheduler shutdown initiated successfully");
                }
                true
            }
            _ = async {
                loop {
                    if scheduler.is_idle() && state.is_idle() {
                        tokio::time::sleep(Duration::from_millis(25)).await;
                        if scheduler.is_idle() && state.is_idle() {
                            break;
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
            } => {
                info!("Crawl has become idle, initiating shutdown.");
                false
            }
        };

        trace!("Closing communication channels");
        drop(res_tx);
        drop(item_tx);

        if !interrupted {
            if let Err(e) = scheduler.shutdown().await {
                error!("Error during scheduler shutdown: {}", e);
            } else {
                debug!("Scheduler shutdown initiated successfully");
            }
        }

        let mut tasks = tokio::task::JoinSet::new();
        tasks.spawn(processor_handle);
        tasks.spawn(parser_handle);
        tasks.spawn(downloader_handle);
        tasks.spawn(init_task);
        let mut results = Vec::new();
        let mut remaining_tasks = 4usize;

        if interrupted {
            let grace_period = config.shutdown_grace_period;
            let shutdown_deadline = tokio::time::sleep(grace_period);
            tokio::pin!(shutdown_deadline);

            while remaining_tasks > 0 {
                tokio::select! {
                    result = tasks.join_next() => {
                        match result {
                            Some(result) => {
                                results.push(result);
                                remaining_tasks = remaining_tasks.saturating_sub(1);
                            }
                            None => break,
                        }
                    }
                    _ = tokio::signal::ctrl_c() => {
                        warn!("Second Ctrl-C received, aborting remaining tasks immediately.");
                        tasks.abort_all();
                        tokio::time::sleep(Duration::from_millis(25)).await;
                        break;
                    }
                    _ = &mut shutdown_deadline => {
                        warn!(
                            "Tasks did not complete within shutdown grace period ({}s), aborting remaining tasks and continuing with shutdown...",
                            grace_period.as_secs()
                        );
                        tasks.abort_all();
                        tokio::time::sleep(Duration::from_millis(25)).await;
                        break;
                    }
                }
            }
        } else {
            while let Some(result) = tasks.join_next().await {
                results.push(result);
            }
            trace!("All tasks completed during shutdown");
        }

        for result in results {
            if let Err(e) = result {
                error!("Task failed during shutdown: {}", e);
            } else {
                trace!("Task completed successfully during shutdown");
            }
        }

        #[cfg(feature = "live-stats")]
        if let Some((stop_tx, handle)) = live_stats_task.take() {
            let _ = stop_tx.send(());
            let _ = handle.await;
        }
        #[cfg(not(feature = "live-stats"))]
        let _ = live_stats_task.take();

        #[cfg(feature = "checkpoint")]
        {
            if let Some(path) = &checkpoint_config.path {
                debug!("Creating final checkpoint at {:?}", path);
                let scheduler_checkpoint = scheduler.snapshot().await?;

                #[cfg(feature = "cookie-store")]
                let result = save_checkpoint::<S>(
                    path,
                    scheduler_checkpoint,
                    &pipelines,
                    &self.cookie_store,
                )
                .await;

                #[cfg(not(feature = "cookie-store"))]
                let result =
                    save_checkpoint::<S>(path, scheduler_checkpoint, &pipelines, &()).await;

                if let Err(e) = result {
                    error!("Final checkpoint save failed: {}", e);
                } else {
                    info!("Final checkpoint saved successfully to {:?}", path);
                }
            }
        }

        info!("Closing item pipelines...");
        let futures: Vec<_> = pipelines.iter().map(|p| p.close()).collect();
        join_all(futures).await;
        debug!("All item pipelines closed");

        if config.live_stats {
            println!("{}\n", stats.to_live_report_string());
        } else {
            info!("Crawl finished successfully\n{}", stats);
        }
        Ok(())
    }

    /// Returns a shared handle to crawler runtime statistics.
    pub fn stats(&self) -> Arc<StatCollector> {
        Arc::clone(&self.stats)
    }

    /// Returns a reference to the spider state.
    pub fn state(&self) -> &S::State {
        &self.spider_state
    }

    /// Returns an Arc clone of the spider state.
    pub fn state_arc(&self) -> Arc<S::State> {
        Arc::clone(&self.spider_state)
    }
}

fn spawn_init_task<S, I>(ctx: CrawlerContext<S, I>) -> tokio::task::JoinHandle<()>
where
    S: Spider<Item = I> + 'static,
    I: ScrapedItem,
{
    tokio::spawn(async move {
        match ctx.spider.start_requests() {
            Ok(source) => match source.into_iter() {
                Ok(requests) => {
                    for req_res in requests {
                        let mut req = match req_res {
                            Ok(req) => req,
                            Err(e) => {
                                warn!("Skipping invalid start URL entry: {}", e);
                                continue;
                            }
                        };

                        req.url.set_fragment(None);
                        match ctx.scheduler.enqueue_request(req).await {
                            Ok(_) => {
                                ctx.stats.increment_requests_enqueued();
                            }
                            Err(e) => {
                                error!("Failed to enqueue initial request: {}", e);
                            }
                        }
                    }
                }
                Err(e) => error!("Failed to resolve start request source: {}", e),
            },
            Err(e) => error!("Failed to create start request source: {}", e),
        }
    })
}
#[cfg(feature = "live-stats")]
struct LiveStatsRenderer {
    previous_lines: Vec<String>,
}

#[cfg(feature = "live-stats")]
impl LiveStatsRenderer {
    fn new() -> Self {
        let mut out = std::io::stdout();
        let _ = execute!(out, Hide);
        let _ = writeln!(out);
        let _ = out.flush();
        Self {
            previous_lines: Vec::new(),
        }
    }

    fn render(&mut self, content: &str) {
        let mut out = std::io::stdout();
        let terminal_width = Self::terminal_width();
        let next_lines: Vec<String> = content
            .lines()
            .map(|line| Self::trim_to_width(line, terminal_width))
            .collect();
        let previous_len = self.previous_lines.len();
        let next_len = next_lines.len();
        let max_len = previous_len.max(next_len);

        if previous_len > 1 {
            let _ = queue!(out, MoveUp((previous_len - 1) as u16));
        }
        let _ = queue!(out, MoveToColumn(0));

        for line_idx in 0..max_len {
            let _ = queue!(out, MoveToColumn(0), Clear(ClearType::CurrentLine));

            if let Some(line) = next_lines.get(line_idx) {
                let _ = write!(out, "{}", line);
            }

            if line_idx + 1 < max_len {
                let _ = writeln!(out);
            }
        }

        let _ = out.flush();
        self.previous_lines = next_lines;
    }

    fn terminal_width() -> usize {
        size()
            .map(|(width, _)| usize::from(width.max(1)))
            .unwrap_or(usize::MAX)
    }

    fn trim_to_width(line: &str, width: usize) -> String {
        if width == usize::MAX {
            return line.to_owned();
        }
        line.chars().take(width).collect()
    }

    fn finish(self) {
        let mut out = std::io::stdout();
        self.clear_previous(&mut out);
        let _ = execute!(out, MoveToColumn(0), Clear(ClearType::CurrentLine), Show);
        let _ = out.flush();
    }

    fn clear_previous(&self, out: &mut std::io::Stdout) {
        if self.previous_lines.is_empty() {
            return;
        }
        let lines = self.previous_lines.len();
        let _ = queue!(out, MoveToColumn(0));
        if lines > 1 {
            let _ = queue!(out, MoveUp((lines - 1) as u16));
        }
        for line_idx in 0..lines {
            let _ = queue!(out, MoveToColumn(0), Clear(ClearType::CurrentLine));
            if line_idx + 1 < lines {
                let _ = writeln!(out);
            }
        }
        if lines > 1 {
            let _ = queue!(out, MoveUp((lines - 1) as u16));
        }
    }
}

#[cfg(feature = "live-stats")]
async fn run_live_stats(
    stats: Arc<StatCollector>,
    interval: Duration,
    mut stop_rx: oneshot::Receiver<()>,
) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut renderer = LiveStatsRenderer::new();

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                renderer.render(&stats.to_live_report_string());
            }
            _ = &mut stop_rx => {
                break;
            }
        }
    }

    renderer.finish();
}
