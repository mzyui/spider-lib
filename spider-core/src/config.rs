//! Configuration types used by the crawler runtime.
//!
//! Most users touch these settings indirectly through [`crate::CrawlerBuilder`],
//! but they are public because they are also useful for explicit configuration
//! and inspection.
//!
//! ## Example
//!
//! ```rust,ignore
//! use spider_core::config::{CrawlerConfig, CheckpointConfig};
//! use std::time::Duration;
//!
//! let crawler_config = CrawlerConfig::default()
//!     .with_max_concurrent_downloads(10)
//!     .with_parser_workers(4)
//!     .with_max_concurrent_pipelines(8)
//!     .with_channel_capacity(2000);
//!
//! let checkpoint_config = CheckpointConfig::builder()
//!     .path("./crawl.checkpoint")
//!     .interval(Duration::from_secs(60))
//!     .build();
//! ```

use std::path::{Path, PathBuf};
use std::time::Duration;

/// Core runtime configuration for the crawler.
#[derive(Debug, Clone)]
pub struct CrawlerConfig {
    /// The maximum number of concurrent downloads.
    pub max_concurrent_downloads: usize,
    /// The maximum number of outstanding requests tracked by the scheduler.
    pub max_pending_requests: usize,
    /// The number of workers dedicated to parsing responses.
    pub parser_workers: usize,
    /// The maximum number of concurrent item processing pipelines.
    pub max_concurrent_pipelines: usize,
    /// The capacity of communication channels between components.
    pub channel_capacity: usize,
    /// Number of requests/items processed per parser output batch.
    pub output_batch_size: usize,
    /// Downloader backpressure threshold for the response channel.
    pub response_backpressure_threshold: usize,
    /// Parser backpressure threshold for the item channel.
    pub item_backpressure_threshold: usize,
    /// When enabled, retries are scheduled outside the downloader permit path.
    pub retry_release_permit: bool,
    /// Enables in-place live statistics updates on terminal stdout.
    pub live_stats: bool,
    /// Refresh interval for live statistics output.
    pub live_stats_interval: Duration,
    /// Optional item fields to show in live-stats preview instead of full JSON.
    pub live_stats_preview_fields: Option<Vec<String>>,
    /// Maximum time to wait for a graceful shutdown before forcing task abort.
    pub shutdown_grace_period: Duration,
    /// Maximum number of scraped items to process before stopping the crawl.
    pub item_limit: Option<usize>,
}

impl Default for CrawlerConfig {
    fn default() -> Self {
        let cpu_count = num_cpus::get();
        let max_concurrent_downloads = (cpu_count * 4).clamp(8, 128);
        let max_pending_requests = (max_concurrent_downloads * 8).clamp(64, 4096);
        let parser_workers = (cpu_count * 2).clamp(4, 32);
        let max_concurrent_pipelines = (cpu_count * 2).clamp(4, 16);
        let channel_capacity = (max_pending_requests / 2).clamp(512, 4096);
        CrawlerConfig {
            max_concurrent_downloads,
            max_pending_requests,
            parser_workers,
            max_concurrent_pipelines,
            channel_capacity,
            output_batch_size: 64,
            response_backpressure_threshold: (max_concurrent_downloads * 6).min(channel_capacity),
            item_backpressure_threshold: (parser_workers * 6).min(channel_capacity),
            retry_release_permit: true,
            live_stats: false,
            live_stats_interval: Duration::from_millis(50),
            live_stats_preview_fields: None,
            shutdown_grace_period: Duration::from_secs(5),
            item_limit: None,
        }
    }
}

impl CrawlerConfig {
    /// Creates a new `CrawlerConfig` with default settings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the maximum number of concurrent downloads.
    pub fn with_max_concurrent_downloads(mut self, limit: usize) -> Self {
        self.max_concurrent_downloads = limit;
        self
    }

    /// Sets the maximum number of outstanding requests tracked by the scheduler.
    pub fn with_max_pending_requests(mut self, limit: usize) -> Self {
        self.max_pending_requests = limit;
        self
    }

    /// Sets the number of parser workers.
    pub fn with_parser_workers(mut self, count: usize) -> Self {
        self.parser_workers = count;
        self
    }

    /// Sets the maximum number of concurrent pipelines.
    pub fn with_max_concurrent_pipelines(mut self, limit: usize) -> Self {
        self.max_concurrent_pipelines = limit;
        self
    }

    /// Sets the channel capacity.
    pub fn with_channel_capacity(mut self, capacity: usize) -> Self {
        self.channel_capacity = capacity;
        self
    }

    /// Sets the parser output batch size.
    pub fn with_output_batch_size(mut self, batch_size: usize) -> Self {
        self.output_batch_size = batch_size;
        self
    }

    /// Sets the downloader response-channel backpressure threshold.
    pub fn with_response_backpressure_threshold(mut self, threshold: usize) -> Self {
        self.response_backpressure_threshold = threshold;
        self
    }

    /// Sets the parser item-channel backpressure threshold.
    pub fn with_item_backpressure_threshold(mut self, threshold: usize) -> Self {
        self.item_backpressure_threshold = threshold;
        self
    }

    /// Controls whether retry delays release the downloader permit immediately.
    pub fn with_retry_release_permit(mut self, enabled: bool) -> Self {
        self.retry_release_permit = enabled;
        self
    }

    /// Enables or disables in-place live stats updates on stdout.
    pub fn with_live_stats(mut self, enabled: bool) -> Self {
        self.live_stats = enabled;
        self
    }

    /// Sets the refresh interval used by live stats mode.
    pub fn with_live_stats_interval(mut self, interval: Duration) -> Self {
        self.live_stats_interval = interval;
        self
    }

    /// Sets which item fields should be shown in live stats preview output.
    ///
    /// Field names support dot notation for nested JSON objects, for example:
    /// `title`, `source_url`, or `metadata.Japanese`.
    ///
    /// You can also set aliases with `label=path`, for example:
    /// `url=source_url` or `jp=metadata.Japanese`.
    pub fn with_live_stats_preview_fields(
        mut self,
        fields: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.live_stats_preview_fields = Some(fields.into_iter().map(Into::into).collect());
        self
    }

    /// Sets the maximum grace period for crawler shutdown.
    pub fn with_shutdown_grace_period(mut self, grace_period: Duration) -> Self {
        self.shutdown_grace_period = grace_period;
        self
    }

    /// Sets the maximum number of scraped items to process before stopping the crawl.
    pub fn with_item_limit(mut self, limit: usize) -> Self {
        self.item_limit = Some(limit);
        self
    }

    /// Validates the configuration.
    pub fn validate(&self) -> Result<(), String> {
        if self.max_concurrent_downloads == 0 {
            return Err("max_concurrent_downloads must be greater than 0".to_string());
        }
        if self.max_pending_requests == 0 {
            return Err("max_pending_requests must be greater than 0".to_string());
        }
        if self.parser_workers == 0 {
            return Err("parser_workers must be greater than 0".to_string());
        }
        if self.max_concurrent_pipelines == 0 {
            return Err("max_concurrent_pipelines must be greater than 0".to_string());
        }
        if self.output_batch_size == 0 {
            return Err("output_batch_size must be greater than 0".to_string());
        }
        if self.response_backpressure_threshold == 0 {
            return Err("response_backpressure_threshold must be greater than 0".to_string());
        }
        if self.item_backpressure_threshold == 0 {
            return Err("item_backpressure_threshold must be greater than 0".to_string());
        }
        if self.live_stats_interval.is_zero() {
            return Err("live_stats_interval must be greater than 0".to_string());
        }
        if matches!(self.live_stats_preview_fields.as_ref(), Some(fields) if fields.is_empty()) {
            return Err("live_stats_preview_fields must not be empty".to_string());
        }
        if self.shutdown_grace_period.is_zero() {
            return Err("shutdown_grace_period must be greater than 0".to_string());
        }
        if matches!(self.item_limit, Some(0)) {
            return Err("item_limit must be greater than 0".to_string());
        }
        Ok(())
    }
}

/// Configuration for checkpoint save/load operations.
///
/// This struct holds settings for automatic checkpoint persistence,
/// allowing crawls to be resumed after interruption.
#[derive(Debug, Clone, Default)]
pub struct CheckpointConfig {
    /// Optional path for saving and loading checkpoints.
    pub path: Option<PathBuf>,
    /// Optional interval between automatic checkpoint saves.
    pub interval: Option<Duration>,
}

impl CheckpointConfig {
    /// Creates a new `CheckpointConfig` with no path or interval.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a new `CheckpointConfigBuilder` for fluent construction.
    pub fn builder() -> CheckpointConfigBuilder {
        CheckpointConfigBuilder::default()
    }

    /// Sets the checkpoint path.
    pub fn with_path<P: AsRef<Path>>(mut self, path: P) -> Self {
        self.path = Some(path.as_ref().to_path_buf());
        self
    }

    /// Sets the checkpoint interval.
    pub fn with_interval(mut self, interval: Duration) -> Self {
        self.interval = Some(interval);
        self
    }

    /// Returns true if checkpointing is enabled.
    pub fn is_enabled(&self) -> bool {
        self.path.is_some()
    }
}

/// Builder for `CheckpointConfig`.
#[derive(Debug, Default)]
pub struct CheckpointConfigBuilder {
    path: Option<PathBuf>,
    interval: Option<Duration>,
}

impl CheckpointConfigBuilder {
    /// Creates a new builder with default settings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the checkpoint path.
    pub fn path<P: AsRef<Path>>(mut self, path: P) -> Self {
        self.path = Some(path.as_ref().to_path_buf());
        self
    }

    /// Sets the checkpoint interval.
    pub fn interval(mut self, interval: Duration) -> Self {
        self.interval = Some(interval);
        self
    }

    /// Builds the `CheckpointConfig`.
    pub fn build(self) -> CheckpointConfig {
        CheckpointConfig {
            path: self.path,
            interval: self.interval,
        }
    }
}

/// Configuration for the parser workers.
///
/// This struct holds settings specific to the response parsing subsystem.
#[derive(Debug, Clone)]
pub struct ParserConfig {
    /// The number of parser worker tasks to spawn.
    pub worker_count: usize,
    /// The capacity of the internal parse queue per worker.
    pub queue_capacity: usize,
}

impl Default for ParserConfig {
    fn default() -> Self {
        ParserConfig {
            worker_count: num_cpus::get().clamp(4, 16),
            queue_capacity: 100,
        }
    }
}

impl ParserConfig {
    /// Creates a new `ParserConfig` with default settings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the number of parser workers.
    pub fn with_worker_count(mut self, count: usize) -> Self {
        self.worker_count = count;
        self
    }

    /// Sets the internal queue capacity per worker.
    pub fn with_queue_capacity(mut self, capacity: usize) -> Self {
        self.queue_capacity = capacity;
        self
    }
}

/// Configuration for the downloader.
///
/// This struct holds settings specific to the HTTP download subsystem.
#[derive(Debug, Clone)]
pub struct DownloaderConfig {
    /// The maximum number of concurrent downloads.
    pub max_concurrent: usize,
    /// The backpressure threshold for response channel occupancy.
    pub backpressure_threshold: usize,
}

impl Default for DownloaderConfig {
    fn default() -> Self {
        let max_concurrent = num_cpus::get().max(16);
        DownloaderConfig {
            max_concurrent,
            backpressure_threshold: max_concurrent * 2,
        }
    }
}

impl DownloaderConfig {
    /// Creates a new `DownloaderConfig` with default settings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the maximum number of concurrent downloads.
    pub fn with_max_concurrent(mut self, limit: usize) -> Self {
        self.max_concurrent = limit;
        self
    }

    /// Sets the backpressure threshold.
    pub fn with_backpressure_threshold(mut self, threshold: usize) -> Self {
        self.backpressure_threshold = threshold;
        self
    }
}

/// Configuration for the item processor.
///
/// This struct holds settings specific to the item processing pipeline.
#[derive(Debug, Clone)]
pub struct ItemProcessorConfig {
    /// The maximum number of concurrent pipeline processors.
    pub max_concurrent: usize,
}

impl Default for ItemProcessorConfig {
    fn default() -> Self {
        ItemProcessorConfig {
            max_concurrent: num_cpus::get().min(8),
        }
    }
}

impl ItemProcessorConfig {
    /// Creates a new `ItemProcessorConfig` with default settings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the maximum number of concurrent processors.
    pub fn with_max_concurrent(mut self, limit: usize) -> Self {
        self.max_concurrent = limit;
        self
    }
}
