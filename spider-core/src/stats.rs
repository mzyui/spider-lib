//! Runtime statistics and reporting helpers.
//!
//! [`StatCollector`] records request counts, response status codes, cache hits,
//! timings, bandwidth, and item throughput while a crawl is running.
//!
//! ## Example
//!
//! ```rust,ignore
//! use spider_core::StatCollector;
//!
//! let stats = StatCollector::new();
//!
//! // During crawling, metrics are automatically updated
//! stats.increment_requests_sent();
//! stats.increment_items_scraped();
//!
//! // Export statistics in various formats
//! println!("{}", stats.to_json_string_pretty().unwrap());
//! println!("{}", stats.to_markdown_string());
//! ```

use parking_lot::RwLock;
use spider_util::error::SpiderError;
use spider_util::item::ScrapedItem;
use spider_util::metrics::{
    ExpMovingAverage, MetricsSnapshot, MetricsSnapshotProvider, format_plain_text_metrics,
};
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

// A snapshot of the current statistics, used for reporting.
// This avoids code duplication in the various export/display methods.
struct StatsSnapshot {
    requests_enqueued: usize,
    requests_sent: usize,
    requests_succeeded: usize,
    requests_failed: usize,
    requests_retried: usize,
    requests_scheduled_for_retry: usize,
    requests_dropped: usize,
    retry_delay_in_flight_ms: u64,
    responses_received: usize,
    responses_from_cache: usize,
    total_bytes_downloaded: usize,
    items_scraped: usize,
    items_processed: usize,
    items_dropped_by_pipeline: usize,
    queue_depth: usize,
    parser_backlog: usize,
    pipeline_backlog: usize,
    retry_backlog: usize,
    response_status_counts: HashMap<u16, usize>,
    elapsed_duration: Duration,
    average_request_time: Option<Duration>,
    fastest_request_time: Option<Duration>,
    slowest_request_time: Option<Duration>,
    request_time_count: usize,
    average_parsing_time: Option<Duration>,
    fastest_parsing_time: Option<Duration>,
    slowest_parsing_time: Option<Duration>,
    parsing_time_count: usize,

    // Recent rates from sliding windows
    recent_requests_per_second: f64,
    recent_responses_per_second: f64,
    recent_items_per_second: f64,
    current_item_preview: String,
}

impl StatsSnapshot {
    fn formatted_duration(&self) -> String {
        let total_secs = self.elapsed_duration.as_secs();
        let hours = total_secs / 3600;
        let minutes = (total_secs % 3600) / 60;
        let seconds = self.elapsed_duration.as_secs_f64() % 60.0;

        if hours > 0 {
            format!("{hours}h {minutes:02}m {seconds:05.2}s")
        } else if minutes > 0 {
            format!("{minutes}m {seconds:05.2}s")
        } else if total_secs > 0 {
            format!("{:.2}s", self.elapsed_duration.as_secs_f64())
        } else if self.elapsed_duration.as_millis() > 0 {
            format!("{}ms", self.elapsed_duration.as_millis())
        } else {
            format!("{}us", self.elapsed_duration.as_micros())
        }
    }

    fn formatted_request_time(&self, duration: Option<Duration>) -> String {
        match duration {
            Some(d) => {
                if d.as_millis() < 1000 {
                    format!("{} ms", d.as_millis())
                } else {
                    format!("{:.2} s", d.as_secs_f64())
                }
            }
            None => "N/A".to_string(),
        }
    }

    fn requests_per_second(&self) -> f64 {
        let elapsed = self.elapsed_duration.as_secs_f64();
        if elapsed > 0.0 {
            self.requests_sent as f64 / elapsed
        } else {
            0.0
        }
    }

    fn responses_per_second(&self) -> f64 {
        let elapsed = self.elapsed_duration.as_secs_f64();
        if elapsed > 0.0 {
            self.responses_received as f64 / elapsed
        } else {
            0.0
        }
    }

    fn items_per_second(&self) -> f64 {
        let elapsed = self.elapsed_duration.as_secs_f64();
        if elapsed > 0.0 {
            self.items_scraped as f64 / elapsed
        } else {
            0.0
        }
    }

    fn bytes_per_second(&self) -> f64 {
        let elapsed = self.elapsed_duration.as_secs_f64();
        if elapsed > 0.0 {
            self.total_bytes_downloaded as f64 / elapsed
        } else {
            0.0
        }
    }

    fn formatted_bytes(&self) -> String {
        const KB: usize = 1024;
        const MB: usize = 1024 * KB;
        const GB: usize = 1024 * MB;

        if self.total_bytes_downloaded >= GB {
            format!("{:.2} GB", self.total_bytes_downloaded as f64 / GB as f64)
        } else if self.total_bytes_downloaded >= MB {
            format!("{:.2} MB", self.total_bytes_downloaded as f64 / MB as f64)
        } else if self.total_bytes_downloaded >= KB {
            format!("{:.2} KB", self.total_bytes_downloaded as f64 / KB as f64)
        } else {
            format!("{} B", self.total_bytes_downloaded)
        }
    }

    fn formatted_bytes_per_second(&self) -> String {
        let bytes_per_second = self.bytes_per_second() as usize;
        const KB: usize = 1024;
        const MB: usize = 1024 * KB;
        const GB: usize = 1024 * MB;

        if bytes_per_second >= GB {
            format!("{:.2} GB/s", bytes_per_second as f64 / GB as f64)
        } else if bytes_per_second >= MB {
            format!("{:.2} MB/s", bytes_per_second as f64 / MB as f64)
        } else if bytes_per_second >= KB {
            format!("{:.2} KB/s", bytes_per_second as f64 / KB as f64)
        } else {
            format!("{bytes_per_second} B/s")
        }
    }

    fn pending_requests(&self) -> usize {
        self.requests_enqueued
            .saturating_sub(self.requests_succeeded + self.requests_failed + self.requests_dropped)
    }

    fn success_ratio(&self) -> f64 {
        if self.requests_sent == 0 {
            0.0
        } else {
            self.requests_succeeded as f64 / self.requests_sent as f64 * 100.0
        }
    }

    fn failure_ratio(&self) -> f64 {
        if self.requests_sent == 0 {
            0.0
        } else {
            self.requests_failed as f64 / self.requests_sent as f64 * 100.0
        }
    }

    fn cache_hit_ratio(&self) -> f64 {
        if self.responses_received == 0 {
            0.0
        } else {
            self.responses_from_cache as f64 / self.responses_received as f64 * 100.0
        }
    }
}

impl MetricsSnapshotProvider for StatsSnapshot {
    fn get_requests_enqueued(&self) -> usize {
        self.requests_enqueued
    }

    fn get_requests_sent(&self) -> usize {
        self.requests_sent
    }

    fn get_requests_succeeded(&self) -> usize {
        self.requests_succeeded
    }

    fn get_requests_failed(&self) -> usize {
        self.requests_failed
    }

    fn get_requests_retried(&self) -> usize {
        self.requests_retried
    }

    fn get_requests_scheduled_for_retry(&self) -> usize {
        self.requests_scheduled_for_retry
    }

    fn get_requests_dropped(&self) -> usize {
        self.requests_dropped
    }

    fn get_retry_delay_in_flight_ms(&self) -> u64 {
        self.retry_delay_in_flight_ms
    }

    fn get_responses_received(&self) -> usize {
        self.responses_received
    }

    fn get_responses_from_cache(&self) -> usize {
        self.responses_from_cache
    }

    fn get_total_bytes_downloaded(&self) -> usize {
        self.total_bytes_downloaded
    }

    fn get_items_scraped(&self) -> usize {
        self.items_scraped
    }

    fn get_items_processed(&self) -> usize {
        self.items_processed
    }

    fn get_items_dropped_by_pipeline(&self) -> usize {
        self.items_dropped_by_pipeline
    }

    fn get_queue_depth(&self) -> usize {
        self.queue_depth
    }

    fn get_parser_backlog(&self) -> usize {
        self.parser_backlog
    }

    fn get_pipeline_backlog(&self) -> usize {
        self.pipeline_backlog
    }

    fn get_retry_backlog(&self) -> usize {
        self.retry_backlog
    }

    fn get_response_status_counts(&self) -> &HashMap<u16, usize> {
        &self.response_status_counts
    }

    fn get_elapsed_duration(&self) -> Duration {
        self.elapsed_duration
    }

    fn get_average_request_time(&self) -> Option<Duration> {
        self.average_request_time
    }

    fn get_fastest_request_time(&self) -> Option<Duration> {
        self.fastest_request_time
    }

    fn get_slowest_request_time(&self) -> Option<Duration> {
        self.slowest_request_time
    }

    fn get_request_time_count(&self) -> usize {
        self.request_time_count
    }

    fn get_average_parsing_time(&self) -> Option<Duration> {
        self.average_parsing_time
    }

    fn get_fastest_parsing_time(&self) -> Option<Duration> {
        self.fastest_parsing_time
    }

    fn get_slowest_parsing_time(&self) -> Option<Duration> {
        self.slowest_parsing_time
    }

    fn get_parsing_time_count(&self) -> usize {
        self.parsing_time_count
    }

    fn get_recent_requests_per_second(&self) -> f64 {
        self.recent_requests_per_second
    }

    fn get_recent_responses_per_second(&self) -> f64 {
        self.recent_responses_per_second
    }

    fn get_recent_items_per_second(&self) -> f64 {
        self.recent_items_per_second
    }

    fn get_current_item_preview(&self) -> &str {
        &self.current_item_preview
    }

    fn formatted_duration(&self) -> String {
        self.formatted_duration()
    }

    fn formatted_request_time(&self, duration: Option<Duration>) -> String {
        self.formatted_request_time(duration)
    }

    fn formatted_bytes(&self) -> String {
        self.formatted_bytes()
    }
}

/// Collects and stores various statistics about the crawler's operation.
#[derive(Debug, serde::Serialize)]
pub struct StatCollector {
    // Crawl-related metrics
    #[serde(skip)]
    pub start_time: Instant,

    // Request-related metrics
    pub requests_enqueued: AtomicUsize,
    pub requests_sent: AtomicUsize,
    pub requests_succeeded: AtomicUsize,
    pub requests_failed: AtomicUsize,
    pub requests_retried: AtomicUsize,
    pub requests_scheduled_for_retry: AtomicUsize,
    pub requests_dropped: AtomicUsize,
    pub retry_delay_in_flight_ms: AtomicU64,

    // Response-related metrics
    pub responses_received: AtomicUsize,
    pub responses_from_cache: AtomicUsize,
    pub response_status_counts: Arc<dashmap::DashMap<u16, usize>>, // e.g., 200, 404, 500
    pub total_bytes_downloaded: AtomicUsize,

    // Add more advanced response time metrics if needed (e.g., histograms)

    // Item-related metrics
    pub items_scraped: AtomicUsize,
    pub items_processed: AtomicUsize,
    pub items_dropped_by_pipeline: AtomicUsize,
    pub queue_depth: AtomicUsize,
    pub parser_backlog: AtomicUsize,
    pub pipeline_backlog: AtomicUsize,
    pub retry_backlog: AtomicUsize,

    // Timing metrics - Using bounded LRU caches to prevent memory leaks
    // Only keeps recent entries (max 10,000 for requests, 1,000 for parsing)
    #[serde(skip)]
    request_time_total_nanos: AtomicU64,
    #[serde(skip)]
    request_time_fastest_nanos: AtomicU64,
    #[serde(skip)]
    request_time_slowest_nanos: AtomicU64,
    #[serde(skip)]
    request_time_count_total: AtomicUsize,
    #[serde(skip)]
    parsing_time_total_nanos: AtomicU64,
    #[serde(skip)]
    parsing_time_fastest_nanos: AtomicU64,
    #[serde(skip)]
    parsing_time_slowest_nanos: AtomicU64,
    #[serde(skip)]
    parsing_time_count_total: AtomicUsize,

    // Exponential moving average metrics for accurate speed calculations
    #[serde(skip)]
    requests_sent_ema: ExpMovingAverage,
    #[serde(skip)]
    responses_received_ema: ExpMovingAverage,
    #[serde(skip)]
    items_scraped_ema: ExpMovingAverage,
    #[serde(skip)]
    current_item_preview: Arc<RwLock<String>>,
    #[serde(skip)]
    live_stats_preview_fields: Option<Vec<String>>,
}

impl StatCollector {
    /// Creates a new `StatCollector` with all counters initialized to zero.
    pub(crate) fn new(live_stats_preview_fields: Option<Vec<String>>) -> Self {
        Self::build(live_stats_preview_fields)
    }

    fn build(live_stats_preview_fields: Option<Vec<String>>) -> Self {
        StatCollector {
            start_time: Instant::now(),
            requests_enqueued: AtomicUsize::new(0),
            requests_sent: AtomicUsize::new(0),
            requests_succeeded: AtomicUsize::new(0),
            requests_failed: AtomicUsize::new(0),
            requests_retried: AtomicUsize::new(0),
            requests_scheduled_for_retry: AtomicUsize::new(0),
            requests_dropped: AtomicUsize::new(0),
            retry_delay_in_flight_ms: AtomicU64::new(0),
            responses_received: AtomicUsize::new(0),
            responses_from_cache: AtomicUsize::new(0),
            response_status_counts: Arc::new(dashmap::DashMap::new()),
            total_bytes_downloaded: AtomicUsize::new(0),
            items_scraped: AtomicUsize::new(0),
            items_processed: AtomicUsize::new(0),
            items_dropped_by_pipeline: AtomicUsize::new(0),
            queue_depth: AtomicUsize::new(0),
            parser_backlog: AtomicUsize::new(0),
            pipeline_backlog: AtomicUsize::new(0),
            retry_backlog: AtomicUsize::new(0),
            request_time_total_nanos: AtomicU64::new(0),
            request_time_fastest_nanos: AtomicU64::new(u64::MAX),
            request_time_slowest_nanos: AtomicU64::new(0),
            request_time_count_total: AtomicUsize::new(0),
            parsing_time_total_nanos: AtomicU64::new(0),
            parsing_time_fastest_nanos: AtomicU64::new(u64::MAX),
            parsing_time_slowest_nanos: AtomicU64::new(0),
            parsing_time_count_total: AtomicUsize::new(0),
            // Initialize exponential moving averages for recent speed calculations (alpha = 0.2 for good balance)
            requests_sent_ema: ExpMovingAverage::new(0.2),
            responses_received_ema: ExpMovingAverage::new(0.2),
            items_scraped_ema: ExpMovingAverage::new(0.2),
            current_item_preview: Arc::new(RwLock::new("none".to_string())),
            live_stats_preview_fields,
        }
    }

    /// Creates a snapshot of the current statistics.
    /// This is the single source of truth for all presentation logic.
    fn internal_snapshot(&self) -> StatsSnapshot {
        let mut status_counts: HashMap<u16, usize> = HashMap::new();
        for entry in self.response_status_counts.iter() {
            let (key, value) = entry.pair();
            status_counts.insert(*key, *value);
        }

        // Get recent rates from exponential moving averages
        let recent_requests_per_second = self.requests_sent_ema.get_rate();
        let recent_responses_per_second = self.responses_received_ema.get_rate();
        let recent_items_per_second = self.items_scraped_ema.get_rate();

        StatsSnapshot {
            requests_enqueued: self.requests_enqueued.load(Ordering::Acquire),
            requests_sent: self.requests_sent.load(Ordering::Acquire),
            requests_succeeded: self.requests_succeeded.load(Ordering::Acquire),
            requests_failed: self.requests_failed.load(Ordering::Acquire),
            requests_retried: self.requests_retried.load(Ordering::Acquire),
            requests_scheduled_for_retry: self.requests_scheduled_for_retry.load(Ordering::Acquire),
            requests_dropped: self.requests_dropped.load(Ordering::Acquire),
            retry_delay_in_flight_ms: self.retry_delay_in_flight_ms.load(Ordering::Acquire),
            responses_received: self.responses_received.load(Ordering::Acquire),
            responses_from_cache: self.responses_from_cache.load(Ordering::Acquire),
            total_bytes_downloaded: self.total_bytes_downloaded.load(Ordering::Acquire),
            items_scraped: self.items_scraped.load(Ordering::Acquire),
            items_processed: self.items_processed.load(Ordering::Acquire),
            items_dropped_by_pipeline: self.items_dropped_by_pipeline.load(Ordering::Acquire),
            queue_depth: self.queue_depth.load(Ordering::Acquire),
            parser_backlog: self.parser_backlog.load(Ordering::Acquire),
            pipeline_backlog: self.pipeline_backlog.load(Ordering::Acquire),
            retry_backlog: self.retry_backlog.load(Ordering::Acquire),
            response_status_counts: status_counts,
            elapsed_duration: self.start_time.elapsed(),
            average_request_time: self.average_request_time(),
            fastest_request_time: self.fastest_request_time(),
            slowest_request_time: self.slowest_request_time(),
            request_time_count: self.request_time_count(),
            average_parsing_time: self.average_parsing_time(),
            fastest_parsing_time: self.fastest_parsing_time(),
            slowest_parsing_time: self.slowest_parsing_time(),
            parsing_time_count: self.parsing_time_count(),

            // Recent rates from sliding windows
            recent_requests_per_second,
            recent_responses_per_second,
            recent_items_per_second,
            current_item_preview: self.current_item_preview.read().clone(),
        }
    }

    /// Returns a public immutable snapshot of the current crawl metrics.
    pub fn snapshot(&self) -> MetricsSnapshot {
        let snapshot = self.internal_snapshot();
        MetricsSnapshot {
            requests_enqueued: snapshot.requests_enqueued,
            requests_sent: snapshot.requests_sent,
            requests_succeeded: snapshot.requests_succeeded,
            requests_failed: snapshot.requests_failed,
            requests_retried: snapshot.requests_retried,
            requests_scheduled_for_retry: snapshot.requests_scheduled_for_retry,
            requests_dropped: snapshot.requests_dropped,
            retry_delay_in_flight_ms: snapshot.retry_delay_in_flight_ms,
            responses_received: snapshot.responses_received,
            responses_from_cache: snapshot.responses_from_cache,
            total_bytes_downloaded: snapshot.total_bytes_downloaded,
            items_scraped: snapshot.items_scraped,
            items_processed: snapshot.items_processed,
            items_dropped_by_pipeline: snapshot.items_dropped_by_pipeline,
            queue_depth: snapshot.queue_depth,
            parser_backlog: snapshot.parser_backlog,
            pipeline_backlog: snapshot.pipeline_backlog,
            retry_backlog: snapshot.retry_backlog,
            response_status_counts: snapshot.response_status_counts,
            elapsed_duration: snapshot.elapsed_duration,
            average_request_time: snapshot.average_request_time,
            fastest_request_time: snapshot.fastest_request_time,
            slowest_request_time: snapshot.slowest_request_time,
            request_time_count: snapshot.request_time_count,
            average_parsing_time: snapshot.average_parsing_time,
            fastest_parsing_time: snapshot.fastest_parsing_time,
            slowest_parsing_time: snapshot.slowest_parsing_time,
            parsing_time_count: snapshot.parsing_time_count,
            recent_requests_per_second: snapshot.recent_requests_per_second,
            recent_responses_per_second: snapshot.recent_responses_per_second,
            recent_items_per_second: snapshot.recent_items_per_second,
            current_item_preview: snapshot.current_item_preview,
        }
    }

    /// Increments the count of enqueued requests.
    pub(crate) fn increment_requests_enqueued(&self) {
        self.requests_enqueued.fetch_add(1, Ordering::AcqRel);
    }

    /// Increments the count of sent requests.
    pub(crate) fn increment_requests_sent(&self) {
        self.requests_sent.fetch_add(1, Ordering::AcqRel);
        // Update the EMA with a count of 1 for this event
        self.requests_sent_ema.update(1);
    }

    /// Increments the count of successful requests.
    pub(crate) fn increment_requests_succeeded(&self) {
        self.requests_succeeded.fetch_add(1, Ordering::AcqRel);
    }

    /// Increments the count of failed requests.
    pub(crate) fn increment_requests_failed(&self) {
        self.requests_failed.fetch_add(1, Ordering::AcqRel);
    }

    /// Increments the count of retried requests.
    pub(crate) fn increment_requests_retried(&self) {
        self.requests_retried.fetch_add(1, Ordering::AcqRel);
    }

    /// Increments the count of dropped requests.
    pub(crate) fn increment_requests_dropped(&self) {
        self.requests_dropped.fetch_add(1, Ordering::AcqRel);
    }

    /// Increments the count of retries scheduled outside the downloader permit path.
    pub(crate) fn increment_requests_scheduled_for_retry(&self) {
        self.requests_scheduled_for_retry
            .fetch_add(1, Ordering::AcqRel);
        self.retry_backlog.fetch_add(1, Ordering::AcqRel);
    }

    /// Marks a scheduled retry as no longer waiting.
    pub(crate) fn complete_scheduled_retry(&self) {
        let mut current = self.retry_backlog.load(Ordering::Acquire);
        loop {
            let next = current.saturating_sub(1);
            match self.retry_backlog.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }
    }

    /// Adds the currently scheduled retry delay in milliseconds.
    pub(crate) fn add_retry_delay_in_flight(&self, delay: Duration) {
        let millis = delay.as_millis().min(u128::from(u64::MAX)) as u64;
        self.retry_delay_in_flight_ms
            .fetch_add(millis, Ordering::AcqRel);
    }

    /// Removes completed retry delay from the in-flight total.
    pub(crate) fn remove_retry_delay_in_flight(&self, delay: Duration) {
        let millis = delay.as_millis().min(u128::from(u64::MAX)) as u64;
        let mut current = self.retry_delay_in_flight_ms.load(Ordering::Acquire);
        loop {
            let next = current.saturating_sub(millis);
            match self.retry_delay_in_flight_ms.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }
    }

    /// Increments the count of received responses.
    pub(crate) fn increment_responses_received(&self) {
        self.responses_received.fetch_add(1, Ordering::AcqRel);
        // Update the EMA with a count of 1 for this event
        self.responses_received_ema.update(1);
    }

    /// Increments the count of responses served from cache.
    pub(crate) fn increment_responses_from_cache(&self) {
        self.responses_from_cache.fetch_add(1, Ordering::AcqRel);
    }

    /// Records a response status code.
    pub(crate) fn record_response_status(&self, status_code: u16) {
        *self.response_status_counts.entry(status_code).or_insert(0) += 1;
    }

    /// Adds to the total bytes downloaded.
    pub(crate) fn add_bytes_downloaded(&self, bytes: usize) {
        self.total_bytes_downloaded
            .fetch_add(bytes, Ordering::AcqRel);
    }

    /// Adds multiple scraped items to the counter.
    pub(crate) fn add_items_scraped(&self, count: usize) {
        if count == 0 {
            return;
        }
        self.items_scraped.fetch_add(count, Ordering::AcqRel);
        self.items_scraped_ema.update(count);
    }

    /// Stores a compact single-line preview of the most recently scraped item.
    pub(crate) fn record_current_item_preview<I: ScrapedItem>(&self, item: &I) {
        let json = item.to_json_value();
        let preview = build_item_preview(&json, self.live_stats_preview_fields.as_deref())
            .unwrap_or_else(|| {
                serde_json::to_string(&json).unwrap_or_else(|_| format!("{:?}", item))
            })
            .replace(['\n', '\r'], " ");
        let preview = truncate_preview(&preview, 160);
        *self.current_item_preview.write() = preview;
    }

    /// Increments the count of processed items.
    pub(crate) fn increment_items_processed(&self) {
        self.items_processed.fetch_add(1, Ordering::AcqRel);
    }

    /// Increments the count of items dropped by pipelines.
    pub(crate) fn increment_items_dropped_by_pipeline(&self) {
        self.items_dropped_by_pipeline
            .fetch_add(1, Ordering::AcqRel);
    }

    /// Updates queue and worker backlog gauges used by snapshots and live stats.
    pub(crate) fn update_runtime_backlog(
        &self,
        queue_depth: usize,
        parser_backlog: usize,
        pipeline_backlog: usize,
    ) {
        self.queue_depth.store(queue_depth, Ordering::Release);
        self.parser_backlog.store(parser_backlog, Ordering::Release);
        self.pipeline_backlog
            .store(pipeline_backlog, Ordering::Release);
    }

    /// Records the time taken for a request.
    pub fn record_request_time(&self, _url: &str, duration: Duration) {
        let nanos = duration.as_nanos().min(u128::from(u64::MAX)) as u64;
        self.request_time_total_nanos
            .fetch_add(nanos, Ordering::AcqRel);
        self.request_time_count_total.fetch_add(1, Ordering::AcqRel);
        update_min(&self.request_time_fastest_nanos, nanos);
        update_max(&self.request_time_slowest_nanos, nanos);
    }

    /// Calculates the average request time across all recorded requests.
    pub fn average_request_time(&self) -> Option<Duration> {
        average_duration(
            &self.request_time_total_nanos,
            self.request_time_count_total.load(Ordering::Acquire),
        )
    }

    /// Gets the fastest request time among all recorded requests.
    pub fn fastest_request_time(&self) -> Option<Duration> {
        duration_from_extreme(
            &self.request_time_fastest_nanos,
            self.request_time_count_total.load(Ordering::Acquire),
            true,
        )
    }

    /// Gets the slowest request time among all recorded requests.
    pub fn slowest_request_time(&self) -> Option<Duration> {
        duration_from_extreme(
            &self.request_time_slowest_nanos,
            self.request_time_count_total.load(Ordering::Acquire),
            false,
        )
    }

    /// Gets the total number of recorded request times.
    pub fn request_time_count(&self) -> usize {
        self.request_time_count_total.load(Ordering::Acquire)
    }

    /// Gets the request time for a specific URL.
    pub fn get_request_time(&self, url: &str) -> Option<Duration> {
        let _ = url;
        None
    }

    /// Gets all recorded request times as a vector of (URL, Duration) pairs.
    pub fn get_all_request_times(&self) -> Vec<(String, Duration)> {
        Vec::new()
    }

    /// Records the time taken for parsing a response.
    pub fn record_parsing_time(&self, duration: Duration) {
        let nanos = duration.as_nanos().min(u128::from(u64::MAX)) as u64;
        self.parsing_time_total_nanos
            .fetch_add(nanos, Ordering::AcqRel);
        self.parsing_time_count_total.fetch_add(1, Ordering::AcqRel);
        update_min(&self.parsing_time_fastest_nanos, nanos);
        update_max(&self.parsing_time_slowest_nanos, nanos);
    }

    /// Calculates the average parsing time across all recorded parses.
    pub fn average_parsing_time(&self) -> Option<Duration> {
        average_duration(
            &self.parsing_time_total_nanos,
            self.parsing_time_count_total.load(Ordering::Acquire),
        )
    }

    /// Gets the fastest parsing time among all recorded parses.
    pub fn fastest_parsing_time(&self) -> Option<Duration> {
        duration_from_extreme(
            &self.parsing_time_fastest_nanos,
            self.parsing_time_count_total.load(Ordering::Acquire),
            true,
        )
    }

    /// Gets the slowest parsing time among all recorded parses.
    pub fn slowest_parsing_time(&self) -> Option<Duration> {
        duration_from_extreme(
            &self.parsing_time_slowest_nanos,
            self.parsing_time_count_total.load(Ordering::Acquire),
            false,
        )
    }

    /// Gets the total number of recorded parsing times.
    pub fn parsing_time_count(&self) -> usize {
        self.parsing_time_count_total.load(Ordering::Acquire)
    }

    /// Clears all recorded request times.
    pub fn clear_request_times(&self) {
        self.request_time_total_nanos.store(0, Ordering::Release);
        self.request_time_fastest_nanos
            .store(u64::MAX, Ordering::Release);
        self.request_time_slowest_nanos.store(0, Ordering::Release);
        self.request_time_count_total.store(0, Ordering::Release);
    }

    /// Clears all recorded parsing times.
    pub fn clear_parsing_times(&self) {
        self.parsing_time_total_nanos.store(0, Ordering::Release);
        self.parsing_time_fastest_nanos
            .store(u64::MAX, Ordering::Release);
        self.parsing_time_slowest_nanos.store(0, Ordering::Release);
        self.parsing_time_count_total.store(0, Ordering::Release);
    }

    /// Converts the snapshot into a JSON string.
    pub fn to_json_string(&self) -> Result<String, SpiderError> {
        Ok(serde_json::to_string(&self.snapshot())?)
    }

    /// Converts the snapshot into a pretty-printed JSON string.
    pub fn to_json_string_pretty(&self) -> Result<String, SpiderError> {
        Ok(serde_json::to_string_pretty(&self.snapshot())?)
    }

    /// Exports the current statistics to a Markdown formatted string.
    pub fn to_markdown_string(&self) -> String {
        let snapshot = self.internal_snapshot();

        let status_codes_list: String = snapshot
            .response_status_counts
            .iter()
            .map(|(code, count)| format!("- **{}**: {}", code, count))
            .collect::<Vec<String>>()
            .join("\n");
        let status_codes_output = if status_codes_list.is_empty() {
            "N/A".to_string()
        } else {
            status_codes_list
        };

        format!(
            r#"# Crawl Statistics Report

- **Duration**: {}
- **Current Rate** (last 10s): {:.2} req/s, {:.2} resp/s, {:.2} item/s
- **Overall Rate** (total): {:.2} req/s, {:.2} resp/s, {:.2} item/s
- **Bytes Per Second**: {}
- **Request Ratios**: success {:.2}%, failure {:.2}%
- **Cache Hit Ratio**: {:.2}%

## Requests
| Metric     | Count |
|------------|-------|
| Enqueued   | {}     |
| Sent       | {}     |
| Pending    | {}     |
| Succeeded  | {}     |
| Failed     | {}     |
| Retried    | {}     |
| Retry Scheduled | {} |
| Dropped    | {}     |
| Retry Delay In Flight | {} ms |

## Responses
| Metric     | Count |
|------------|-------|
| Received   | {}     |
| From Cache | {}     |
| Downloaded | {}     |

## Items
| Metric     | Count |
|------------|--------|
| Scraped    | {}     |
| Processed  | {}     |
| Dropped    | {}     |

## Request Times
| Metric           | Value      |
|------------------|------------|
| Average Time     | {}         |
| Fastest Request  | {}         |
| Slowest Request  | {}         |
| Total Recorded   | {}         |

## Parsing Times
| Metric           | Value      |
|------------------|------------|
| Average Time     | {}         |
| Fastest Parse    | {}         |
| Slowest Parse    | {}         |
| Total Recorded   | {}         |

## Status Codes
{}
"#,
            snapshot.formatted_duration(),
            snapshot.requests_per_second(),
            snapshot.responses_per_second(),
            snapshot.items_per_second(),
            // Calculate cumulative speeds for comparison
            {
                let total_seconds = snapshot.elapsed_duration.as_secs() as f64;
                if total_seconds > 0.0 {
                    snapshot.requests_sent as f64 / total_seconds
                } else {
                    0.0
                }
            },
            {
                let total_seconds = snapshot.elapsed_duration.as_secs() as f64;
                if total_seconds > 0.0 {
                    snapshot.responses_received as f64 / total_seconds
                } else {
                    0.0
                }
            },
            {
                let total_seconds = snapshot.elapsed_duration.as_secs() as f64;
                if total_seconds > 0.0 {
                    snapshot.items_scraped as f64 / total_seconds
                } else {
                    0.0
                }
            },
            snapshot.formatted_bytes_per_second(),
            snapshot.success_ratio(),
            snapshot.failure_ratio(),
            snapshot.cache_hit_ratio(),
            snapshot.requests_enqueued,
            snapshot.requests_sent,
            snapshot.pending_requests(),
            snapshot.requests_succeeded,
            snapshot.requests_failed,
            snapshot.requests_retried,
            snapshot.requests_scheduled_for_retry,
            snapshot.requests_dropped,
            snapshot.retry_delay_in_flight_ms,
            snapshot.responses_received,
            snapshot.responses_from_cache,
            snapshot.formatted_bytes(),
            snapshot.items_scraped,
            snapshot.items_processed,
            snapshot.items_dropped_by_pipeline,
            snapshot.formatted_request_time(snapshot.average_request_time),
            snapshot.formatted_request_time(snapshot.fastest_request_time),
            snapshot.formatted_request_time(snapshot.slowest_request_time),
            snapshot.request_time_count,
            snapshot.formatted_request_time(snapshot.average_parsing_time),
            snapshot.formatted_request_time(snapshot.fastest_parsing_time),
            snapshot.formatted_request_time(snapshot.slowest_parsing_time),
            snapshot.parsing_time_count,
            status_codes_output
        )
    }

    /// Exports current statistics to the text layout used for terminal output.
    pub fn to_live_report_string(&self) -> String {
        let snapshot = self.internal_snapshot();
        format_plain_text_metrics(&snapshot)
    }
}

fn truncate_preview(input: &str, max_chars: usize) -> String {
    let mut chars = input.chars();
    let truncated: String = chars.by_ref().take(max_chars).collect();
    if chars.next().is_some() {
        format!("{truncated}...")
    } else {
        truncated
    }
}

fn build_item_preview(json: &serde_json::Value, fields: Option<&[String]>) -> Option<String> {
    let fields = fields?;

    if fields.len() == 1 {
        let (_, path) = parse_preview_field(&fields[0]);
        return get_value_by_path(json, path).map(format_preview_value);
    }

    let mut preview = serde_json::Map::new();

    for field in fields {
        let (label, path) = parse_preview_field(field);
        if let Some(value) = get_value_by_path(json, path) {
            preview.insert(label.to_string(), value.clone());
        }
    }

    if preview.is_empty() {
        None
    } else {
        serde_json::to_string(&serde_json::Value::Object(preview)).ok()
    }
}

fn format_preview_value(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Null => "null".to_string(),
        serde_json::Value::Bool(boolean) => boolean.to_string(),
        serde_json::Value::Number(number) => number.to_string(),
        serde_json::Value::String(text) => text.clone(),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
            serde_json::to_string(value).unwrap_or_else(|_| value.to_string())
        }
    }
}

fn parse_preview_field(field: &str) -> (&str, &str) {
    match field.split_once('=') {
        Some((label, path)) if !label.is_empty() && !path.is_empty() => (label, path),
        _ => (field, field),
    }
}

fn get_value_by_path<'a>(
    value: &'a serde_json::Value,
    path: &str,
) -> Option<&'a serde_json::Value> {
    let mut current = value;
    for segment in path.split('.') {
        current = current.get(segment)?;
    }
    Some(current)
}

impl Default for StatCollector {
    fn default() -> Self {
        Self::new(None)
    }
}

impl std::fmt::Display for StatCollector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "\n{}\n", self.to_live_report_string())
    }
}

fn average_duration(total_nanos: &AtomicU64, count: usize) -> Option<Duration> {
    if count == 0 {
        return None;
    }

    Some(Duration::from_nanos(
        total_nanos.load(Ordering::Acquire) / count as u64,
    ))
}

fn duration_from_extreme(extreme: &AtomicU64, count: usize, is_min: bool) -> Option<Duration> {
    if count == 0 {
        return None;
    }

    let nanos = extreme.load(Ordering::Acquire);
    if is_min && nanos == u64::MAX {
        None
    } else {
        Some(Duration::from_nanos(nanos))
    }
}

fn update_min(target: &AtomicU64, candidate: u64) {
    let mut current = target.load(Ordering::Acquire);
    while candidate < current {
        match target.compare_exchange_weak(current, candidate, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => break,
            Err(actual) => current = actual,
        }
    }
}

fn update_max(target: &AtomicU64, candidate: u64) {
    let mut current = target.load(Ordering::Acquire);
    while candidate > current {
        match target.compare_exchange_weak(current, candidate, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => break,
            Err(actual) => current = actual,
        }
    }
}
