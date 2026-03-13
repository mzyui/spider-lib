//! # Builder Module
//!
//! Provides the [`CrawlerBuilder`], a fluent API for constructing and configuring
//! [`Crawler`](crate::Crawler) instances with customizable settings and components.
//!
//! ## Overview
//!
//! The [`CrawlerBuilder`] simplifies the process of assembling various `spider-core`
//! components into a fully configured web crawler. It provides a flexible,
//! ergonomic interface for setting up all aspects of the crawling process.
//!
//! ## Key Features
//!
//! - **Concurrency Configuration**: Control the number of concurrent downloads,
//!   parsing workers, and pipeline processors
//! - **Component Registration**: Attach custom downloaders, middlewares, and pipelines
//! - **Checkpoint Management**: Configure automatic saving and loading of crawl state
//!   (requires `checkpoint` feature)
//! - **Statistics Integration**: Initialize and connect the [`StatCollector`](crate::stats::StatCollector)
//! - **Default Handling**: Automatic addition of essential middlewares when needed
//!
//! ## Example
//!
//! ```rust,ignore
//! use spider_core::CrawlerBuilder;
//! use spider_middleware::rate_limit::RateLimitMiddleware;
//! use spider_pipeline::console::ConsolePipeline;
//! use spider_util::error::SpiderError;
//!
//! async fn setup_crawler() -> Result<(), SpiderError> {
//!     let crawler = CrawlerBuilder::new(MySpider)
//!         .max_concurrent_downloads(10)
//!         .max_parser_workers(4)
//!         .add_middleware(RateLimitMiddleware::default())
//!         .add_pipeline(ConsolePipeline::new())
//!         .with_checkpoint_path("./crawl.checkpoint")
//!         .build()
//!         .await?;
//!
//!     crawler.start_crawl().await
//! }
//! ```

use crate::Downloader;
use crate::ReqwestClientDownloader;
use crate::config::{CheckpointConfig, CrawlerConfig};
use crate::scheduler::Scheduler;
use crate::spider::Spider;
use spider_middleware::middleware::Middleware;
use spider_pipeline::pipeline::Pipeline;

#[cfg(feature = "checkpoint")]
type RestoreResult = (
    Option<crate::SchedulerCheckpoint>,
    Option<std::collections::HashMap<String, serde_json::Value>>,
);
use spider_util::error::SpiderError;
use std::marker::PhantomData;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use super::Crawler;
use crate::stats::StatCollector;
use log::LevelFilter;
#[cfg(feature = "checkpoint")]
use log::{debug, warn};

#[cfg(feature = "checkpoint")]
use rmp_serde;
#[cfg(feature = "checkpoint")]
use std::fs;

/// A fluent builder for constructing [`Crawler`] instances.
///
/// `CrawlerBuilder` provides a chainable API for configuring all aspects
/// of a web crawler, including concurrency settings, middleware, pipelines,
/// and checkpoint options.
///
/// ## Type Parameters
///
/// - `S`: The [`Spider`] implementation type
/// - `D`: The [`Downloader`] implementation type
///
/// ## Example
///
/// ```rust,ignore
/// # use spider_core::{CrawlerBuilder, Spider};
/// # use spider_util::{response::Response, error::SpiderError, item::ParseOutput};
/// # struct MySpider;
/// # #[async_trait::async_trait]
/// # impl Spider for MySpider {
/// #     type Item = String;
/// #     type State = ();
/// #     fn start_requests(&self) -> Result<spider_core::spider::StartRequests<'_>, SpiderError> {
/// #         Ok(spider_core::spider::StartRequests::Iter(Box::new(std::iter::empty())))
/// #     }
/// #     async fn parse(&self, response: Response, state: &Self::State) -> Result<ParseOutput<Self::Item>, SpiderError> { todo!() }
/// # }
/// let builder = CrawlerBuilder::new(MySpider)
///     .max_concurrent_downloads(8)
///     .max_pending_requests(16)
///     .max_parser_workers(4);
/// ```
pub struct CrawlerBuilder<S: Spider, D>
where
    D: Downloader,
{
    config: CrawlerConfig,
    checkpoint_config: CheckpointConfig,
    downloader: D,
    spider: Option<S>,
    middlewares: Vec<Box<dyn Middleware<D::Client> + Send + Sync>>,
    pipelines: Vec<Box<dyn Pipeline<S::Item>>>,
    log_level: Option<LevelFilter>,
    _phantom: PhantomData<S>,
}

impl<S: Spider> Default for CrawlerBuilder<S, ReqwestClientDownloader> {
    fn default() -> Self {
        Self {
            config: CrawlerConfig::default(),
            checkpoint_config: CheckpointConfig::default(),
            downloader: ReqwestClientDownloader::default(),
            spider: None,
            middlewares: Vec::new(),
            pipelines: Vec::new(),
            log_level: None,
            _phantom: PhantomData,
        }
    }
}

impl<S: Spider> CrawlerBuilder<S, ReqwestClientDownloader> {
    /// Creates a new `CrawlerBuilder` for a given spider with the default [`ReqwestClientDownloader`].
    ///
    /// ## Example
    ///
    /// ```rust,ignore
    /// let crawler = CrawlerBuilder::new(MySpider)
    ///     .build()
    ///     .await?;
    /// ```
    pub fn new(spider: S) -> Self {
        Self {
            spider: Some(spider),
            ..Default::default()
        }
    }
}

impl<S: Spider, D: Downloader> CrawlerBuilder<S, D> {
    #[cfg(feature = "checkpoint")]
    fn load_checkpoint_from_path(&self, path: &std::path::Path) -> Option<crate::Checkpoint> {
        match fs::read(path) {
            Ok(bytes) => match rmp_serde::from_slice::<crate::Checkpoint>(&bytes) {
                Ok(checkpoint) => Some(checkpoint),
                Err(e) => {
                    warn!("Failed to deserialize checkpoint from {:?}: {}", path, e);
                    None
                }
            },
            Err(e) => {
                warn!("Failed to read checkpoint file {:?}: {}", path, e);
                None
            }
        }
    }

    /// Sets the maximum number of concurrent downloads.
    ///
    /// This controls how many HTTP requests can be in-flight simultaneously.
    /// Higher values increase throughput but may overwhelm target servers.
    ///
    /// ## Default
    ///
    /// Defaults to twice the number of CPU cores, clamped between 4 and 64.
    pub fn max_concurrent_downloads(mut self, limit: usize) -> Self {
        self.config.max_concurrent_downloads = limit;
        self
    }

    /// Sets the maximum number of outstanding requests tracked by the scheduler.
    ///
    /// This includes queued requests plus requests already handed off for download.
    /// Lower values keep the frontier tighter and reduce internal request buildup.
    pub fn max_pending_requests(mut self, limit: usize) -> Self {
        self.config.max_pending_requests = limit;
        self
    }

    /// Sets the number of worker tasks dedicated to parsing responses.
    ///
    /// Parser workers process HTTP responses concurrently, calling the
    /// spider's [`parse`](Spider::parse) method to extract items and
    /// discover new URLs.
    ///
    /// ## Default
    ///
    /// Defaults to the number of CPU cores, clamped between 4 and 16.
    pub fn max_parser_workers(mut self, limit: usize) -> Self {
        self.config.parser_workers = limit;
        self
    }

    /// Sets the maximum number of concurrent item processing pipelines.
    ///
    /// This controls how many items can be processed by pipelines simultaneously.
    ///
    /// ## Default
    ///
    /// Defaults to the number of CPU cores, with a maximum of 8.
    pub fn max_concurrent_pipelines(mut self, limit: usize) -> Self {
        self.config.max_concurrent_pipelines = limit;
        self
    }

    /// Sets the capacity of internal communication channels.
    ///
    /// This controls the buffer size for channels between the downloader,
    /// parser, and pipeline components. Higher values can improve throughput
    /// at the cost of increased memory usage.
    ///
    /// ## Default
    ///
    /// Defaults to 1000.
    pub fn channel_capacity(mut self, capacity: usize) -> Self {
        self.config.channel_capacity = capacity;
        self
    }

    /// Sets the parser output batch size.
    pub fn output_batch_size(mut self, batch_size: usize) -> Self {
        self.config.output_batch_size = batch_size;
        self
    }

    /// Sets the downloader response-channel backpressure threshold.
    pub fn response_backpressure_threshold(mut self, threshold: usize) -> Self {
        self.config.response_backpressure_threshold = threshold;
        self
    }

    /// Sets the parser item-channel backpressure threshold.
    pub fn item_backpressure_threshold(mut self, threshold: usize) -> Self {
        self.config.item_backpressure_threshold = threshold;
        self
    }

    /// Controls whether retries release downloader permits before waiting.
    pub fn retry_release_permit(mut self, enabled: bool) -> Self {
        self.config.retry_release_permit = enabled;
        self
    }

    /// Enables or disables live, in-place statistics updates on terminal stdout.
    ///
    /// When enabled, spider-* logs are forced to `LevelFilter::Off` during build
    /// to avoid interleaving with the live terminal renderer.
    pub fn live_stats(mut self, enabled: bool) -> Self {
        self.config.live_stats = enabled;
        self
    }

    /// Sets the refresh interval for live statistics updates.
    pub fn live_stats_interval(mut self, interval: Duration) -> Self {
        self.config.live_stats_interval = interval;
        self
    }

    /// Sets the maximum grace period for crawler shutdown before forcing task abort.
    pub fn shutdown_grace_period(mut self, grace_period: Duration) -> Self {
        self.config.shutdown_grace_period = grace_period;
        self
    }

    /// Sets a custom downloader implementation.
    ///
    /// Use this method to provide a custom [`Downloader`] implementation
    /// instead of the default [`ReqwestClientDownloader`].
    pub fn downloader(mut self, downloader: D) -> Self {
        self.downloader = downloader;
        self
    }

    /// Adds a middleware to the crawler's middleware stack.
    ///
    /// Middlewares intercept and modify requests before they are sent and
    /// responses after they are received. They are executed in the order
    /// they are added.
    ///
    /// ## Example
    ///
    /// ```rust,ignore
    /// let crawler = CrawlerBuilder::new(MySpider)
    ///     .add_middleware(RateLimitMiddleware::default())
    ///     .add_middleware(RetryMiddleware::new())
    ///     .build()
    ///     .await?;
    /// ```
    pub fn add_middleware<M>(mut self, middleware: M) -> Self
    where
        M: Middleware<D::Client> + Send + Sync + 'static,
    {
        self.middlewares.push(Box::new(middleware));
        self
    }

    /// Adds a pipeline to the crawler's pipeline stack.
    ///
    /// Pipelines process scraped items after they are extracted by the spider.
    /// They can be used for validation, transformation, deduplication, or
    /// storage (e.g., writing to files or databases).
    ///
    /// ## Example
    ///
    /// ```rust,ignore
    /// let crawler = CrawlerBuilder::new(MySpider)
    ///     .add_pipeline(ConsolePipeline::new())
    ///     .add_pipeline(JsonPipeline::new("output.json")?)
    ///     .build()
    ///     .await?;
    /// ```
    pub fn add_pipeline<P>(mut self, pipeline: P) -> Self
    where
        P: Pipeline<S::Item> + 'static,
    {
        self.pipelines.push(Box::new(pipeline));
        self
    }

    /// Sets the log level for `spider-*` library crates.
    ///
    /// This configures the logging level specifically for the spider-lib ecosystem
    /// (spider-core, spider-middleware, spider-pipeline, spider-util, spider-downloader).
    /// Logs from other dependencies (e.g., reqwest, tokio) will not be affected.
    ///
    /// ## Log Levels
    ///
    /// - `LevelFilter::Error` - Only error messages
    /// - `LevelFilter::Warn` - Warnings and errors
    /// - `LevelFilter::Info` - Informational messages, warnings, and errors
    /// - `LevelFilter::Debug` - Debug messages and above
    /// - `LevelFilter::Trace` - All messages including trace
    ///
    /// ## Example
    ///
    /// ```rust,ignore
    /// use log::LevelFilter;
    ///
    /// let crawler = CrawlerBuilder::new(MySpider)
    ///     .log_level(LevelFilter::Debug)
    ///     .build()
    ///     .await?;
    /// ```
    pub fn log_level(mut self, level: LevelFilter) -> Self {
        self.log_level = Some(level);
        self
    }

    /// Sets the path for saving and loading checkpoints.
    ///
    /// When enabled, the crawler periodically saves its state to this file,
    /// allowing crawls to be resumed after interruption.
    ///
    /// Requires the `checkpoint` feature to be enabled.
    pub fn with_checkpoint_path<P: AsRef<Path>>(mut self, path: P) -> Self {
        self.checkpoint_config.path = Some(path.as_ref().to_path_buf());
        self
    }

    /// Sets the interval between automatic checkpoint saves.
    ///
    /// When enabled, the crawler saves its state at this interval.
    /// Shorter intervals provide more frequent recovery points but may
    /// impact performance.
    ///
    /// Requires the `checkpoint` feature to be enabled.
    pub fn with_checkpoint_interval(mut self, interval: Duration) -> Self {
        self.checkpoint_config.interval = Some(interval);
        self
    }

    /// Builds the [`Crawler`] instance.
    ///
    /// This method finalizes the crawler configuration and initializes all
    /// components. It performs validation and sets up default values where
    /// necessary.
    ///
    /// # Errors
    ///
    /// Returns a [`SpiderError::ConfigurationError`] if:
    /// - `max_concurrent_downloads` is 0
    /// - `parser_workers` is 0
    /// - No spider was provided to the builder
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let crawler = CrawlerBuilder::new(MySpider)
    ///     .max_concurrent_downloads(10)
    ///     .build()
    ///     .await?;
    /// ```
    pub async fn build(mut self) -> Result<Crawler<S, D::Client>, SpiderError>
    where
        D: Downloader + Send + Sync + 'static,
        D::Client: Send + Sync + Clone,
        S::Item: Send + Sync + 'static,
    {
        let spider = self.take_spider()?;
        self.init_default_pipeline();

        // Live stats redraw on stdout and should not interleave with spider-* logs.
        // Force spider-* logs to Off when live stats mode is enabled.
        let effective_log_level = if self.config.live_stats {
            Some(LevelFilter::Off)
        } else {
            self.log_level
        };

        // Initialize logging for spider-* crates if an effective log level is configured.
        if let Some(level) = effective_log_level {
            self.init_logging(level);
        }

        // Validate config
        self.config
            .validate()
            .map_err(SpiderError::ConfigurationError)?;

        // Restore checkpoint and get scheduler state
        #[cfg(feature = "checkpoint")]
        let (scheduler_state, pipeline_states) = self.restore_checkpoint()?;
        #[cfg(not(feature = "checkpoint"))]
        let scheduler_state: Option<()> = None;

        // Restore pipeline states if checkpoint was loaded
        #[cfg(feature = "checkpoint")]
        {
            if let Some(states) = pipeline_states {
                for (name, state) in states {
                    if let Some(pipeline) = self.pipelines.iter().find(|p| p.name() == name) {
                        pipeline.restore_state(state).await?;
                    } else {
                        warn!("Checkpoint contains state for unknown pipeline: {}", name);
                    }
                }
            }
        }

        // Get cookie store if feature is enabled
        #[cfg(feature = "cookie-store")]
        let cookie_store = {
            #[cfg(feature = "checkpoint")]
            {
                let (_, cookie_store) = self.restore_cookie_store().await?;
                cookie_store
            }
            #[cfg(not(feature = "checkpoint"))]
            {
                Some(crate::CookieStore::default())
            }
        };

        // Create scheduler with or without checkpoint state
        let (scheduler_arc, req_rx) =
            Scheduler::new(scheduler_state, self.config.max_pending_requests);
        let downloader_arc = Arc::new(self.downloader);
        let stats = Arc::new(StatCollector::new());

        // Build crawler with or without cookie store based on feature flag
        #[cfg(feature = "cookie-store")]
        let crawler = Crawler::new(
            scheduler_arc,
            req_rx,
            downloader_arc,
            self.middlewares,
            spider,
            self.pipelines,
            self.config,
            #[cfg(feature = "checkpoint")]
            self.checkpoint_config,
            stats,
            Arc::new(tokio::sync::RwLock::new(cookie_store.unwrap_or_default())),
        );

        #[cfg(not(feature = "cookie-store"))]
        let crawler = Crawler::new(
            scheduler_arc,
            req_rx,
            downloader_arc,
            self.middlewares,
            spider,
            self.pipelines,
            self.config,
            #[cfg(feature = "checkpoint")]
            self.checkpoint_config,
            stats,
        );

        Ok(crawler)
    }

    /// Restores checkpoint state from disk (checkpoint feature only).
    ///
    /// This internal method loads a previously saved checkpoint file and
    /// restores the scheduler state and pipeline states.
    ///
    /// # Returns
    ///
    /// Returns a tuple of `(SchedulerCheckpoint, Option<HashMap<String, Value>>)`.
    /// If no checkpoint path is configured or the file doesn't exist, returns
    /// default values.
    ///
    /// # Errors
    ///
    /// Returns a [`SpiderError`] if deserialization fails.
    /// Note: Checkpoint file read/deserialization errors are logged as warnings
    /// but do not fail the operation—the crawl proceeds without checkpoint data.
    #[cfg(feature = "checkpoint")]
    fn restore_checkpoint(&mut self) -> Result<RestoreResult, SpiderError> {
        let mut scheduler_state = None;
        let mut pipeline_states = None;

        if let Some(path) = &self.checkpoint_config.path {
            debug!("Attempting to load checkpoint from {:?}", path);
            if let Some(checkpoint) = self.load_checkpoint_from_path(path) {
                scheduler_state = Some(checkpoint.scheduler);
                pipeline_states = Some(checkpoint.pipelines);
            }
        }

        Ok((scheduler_state, pipeline_states))
    }

    /// Restores cookie store from checkpoint (checkpoint + cookie-store features).
    #[cfg(all(feature = "checkpoint", feature = "cookie-store"))]
    async fn restore_cookie_store(
        &mut self,
    ) -> Result<(Option<()>, Option<crate::CookieStore>), SpiderError> {
        let mut cookie_store = None;

        if let Some(path) = &self.checkpoint_config.path {
            debug!("Attempting to load cookie store from checkpoint {:?}", path);
            if let Some(checkpoint) = self.load_checkpoint_from_path(path) {
                cookie_store = Some(checkpoint.cookie_store);
            }
        }

        Ok((None, cookie_store))
    }

    /// Extracts the spider from the builder.
    ///
    /// # Errors
    ///
    /// Returns a [`SpiderError::ConfigurationError`] if:
    /// - `max_concurrent_downloads` is 0
    /// - `parser_workers` is 0
    /// - No spider was provided
    fn take_spider(&mut self) -> Result<S, SpiderError> {
        if self.config.max_concurrent_downloads == 0 {
            return Err(SpiderError::ConfigurationError(
                "max_concurrent_downloads must be greater than 0.".to_string(),
            ));
        }
        if self.config.max_pending_requests == 0 {
            return Err(SpiderError::ConfigurationError(
                "max_pending_requests must be greater than 0.".to_string(),
            ));
        }
        if self.config.parser_workers == 0 {
            return Err(SpiderError::ConfigurationError(
                "parser_workers must be greater than 0.".to_string(),
            ));
        }
        self.spider.take().ok_or_else(|| {
            SpiderError::ConfigurationError("Crawler must have a spider.".to_string())
        })
    }

    /// Initializes the pipeline stack with a default [`ConsolePipeline`] if empty.
    ///
    /// This ensures that scraped items are always output somewhere, even if
    /// no explicit pipelines are configured by the user.
    fn init_default_pipeline(&mut self) {
        if self.pipelines.is_empty() {
            use spider_pipeline::console::ConsolePipeline;
            self.pipelines.push(Box::new(ConsolePipeline::new()));
        }
    }

    /// Initializes logging for spider-* crates only.
    ///
    /// This sets up env_logger with a filter that only enables logging for
    /// crates within the spider-lib ecosystem.
    fn init_logging(&self, level: LevelFilter) {
        use env_logger::{Builder, Env};

        let mut builder = Builder::from_env(Env::default().default_filter_or("off"));

        // Set filter specifically for spider-* crates
        builder.filter_module("spider_core", level);
        builder.filter_module("spider_middleware", level);
        builder.filter_module("spider_pipeline", level);
        builder.filter_module("spider_util", level);
        builder.filter_module("spider_downloader", level);
        builder.filter_module("spider_macro", level);

        builder.init();
    }
}

#[cfg(test)]
mod tests {
    use super::CrawlerBuilder;
    use crate::Spider;
    use async_trait::async_trait;
    use serde_json::Value;
    use spider_util::error::SpiderError;
    use spider_util::item::{ParseOutput, ScrapedItem};
    use spider_util::response::Response;
    use std::any::Any;
    use std::time::Duration;

    struct TestSpider;
    #[derive(Debug, Clone)]
    struct TestItem;

    impl ScrapedItem for TestItem {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn box_clone(&self) -> Box<dyn ScrapedItem + Send + Sync> {
            Box::new(self.clone())
        }

        fn to_json_value(&self) -> Value {
            Value::Null
        }
    }

    #[async_trait]
    impl Spider for TestSpider {
        type Item = TestItem;
        type State = ();

        fn start_requests(&self) -> Result<crate::StartRequests<'_>, SpiderError> {
            Ok(crate::StartRequests::Iter(Box::new(std::iter::empty())))
        }

        async fn parse(
            &self,
            _response: Response,
            _state: &Self::State,
        ) -> Result<ParseOutput<Self::Item>, SpiderError> {
            Ok(ParseOutput::new())
        }
    }

    #[test]
    fn shutdown_grace_period_builder_sets_config_value() {
        let builder = CrawlerBuilder::new(TestSpider).shutdown_grace_period(Duration::from_secs(2));

        assert_eq!(builder.config.shutdown_grace_period, Duration::from_secs(2));
    }
}
