//! # Metrics Utilities
//!
//! Common metrics-related utilities and structures for the spider framework.

use parking_lot::RwLock;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Formatter traits and default implementations for metrics output.
pub use crate::formatters::{
    ByteFormatter, DefaultByteFormatter, DefaultDurationFormatter, DefaultRateCalculator,
    DurationFormatter, RateCalculator,
};

/// Thread-safe exponential moving average used to track recent event rates.
#[derive(Debug)]
pub struct ExpMovingAverage {
    alpha: f64,
    rate: Arc<RwLock<f64>>,
    last_update: Arc<RwLock<Instant>>,
    event_count: Arc<RwLock<usize>>,
}

impl ExpMovingAverage {
    /// Creates a new moving average with smoothing factor `alpha`.
    ///
    /// Lower values react more slowly to changes; higher values react faster.
    pub fn new(alpha: f64) -> Self {
        ExpMovingAverage {
            alpha,
            rate: Arc::new(RwLock::new(0.0)),
            last_update: Arc::new(RwLock::new(Instant::now())),
            event_count: Arc::new(RwLock::new(0)),
        }
    }

    /// Records `count` new events and updates the smoothed rate periodically.
    pub fn update(&self, count: usize) {
        let now = Instant::now();
        let mut last_update = self.last_update.write();
        let mut event_count = self.event_count.write();

        *event_count += count;
        let time_delta = now.duration_since(*last_update).as_secs_f64();

        if time_delta >= 1.0 {
            let current_rate = *event_count as f64 / time_delta;
            let mut rate = self.rate.write();
            *rate = self.alpha * current_rate + (1.0 - self.alpha) * (*rate);

            *event_count = 0;
            *last_update = now;
        }
    }

    /// Returns the current smoothed events-per-second rate.
    pub fn get_rate(&self) -> f64 {
        *self.rate.read()
    }
}

/// Point-in-time snapshot of crawler metrics for reporting and export.
#[derive(Debug, Clone, serde::Serialize)]
pub struct MetricsSnapshot {
    pub requests_enqueued: usize,
    pub requests_sent: usize,
    pub requests_succeeded: usize,
    pub requests_failed: usize,
    pub requests_retried: usize,
    pub requests_scheduled_for_retry: usize,
    pub requests_dropped: usize,
    pub retry_delay_in_flight_ms: u64,
    pub responses_received: usize,
    pub responses_from_cache: usize,
    pub total_bytes_downloaded: usize,
    pub items_scraped: usize,
    pub items_processed: usize,
    pub items_dropped_by_pipeline: usize,
    pub response_status_counts: std::collections::HashMap<u16, usize>,
    pub elapsed_duration: Duration,
    pub average_request_time: Option<Duration>,
    pub fastest_request_time: Option<Duration>,
    pub slowest_request_time: Option<Duration>,
    pub request_time_count: usize,
    pub average_parsing_time: Option<Duration>,
    pub fastest_parsing_time: Option<Duration>,
    pub slowest_parsing_time: Option<Duration>,
    pub parsing_time_count: usize,
    pub recent_requests_per_second: f64,
    pub recent_responses_per_second: f64,
    pub recent_items_per_second: f64,
}

impl MetricsSnapshot {
    /// Formats [`Self::elapsed_duration`] into a human-readable string.
    pub fn formatted_duration(&self) -> String {
        DefaultDurationFormatter.formatted_duration(self.elapsed_duration)
    }

    /// Formats an optional request duration for display.
    pub fn formatted_request_time(&self, duration: Option<Duration>) -> String {
        DefaultDurationFormatter.formatted_request_time(duration)
    }

    /// Returns average sent requests per second over total elapsed duration.
    pub fn requests_per_second(&self) -> f64 {
        DefaultRateCalculator.calculate_rate(self.requests_sent, self.elapsed_duration)
    }

    /// Returns average received responses per second over total elapsed duration.
    pub fn responses_per_second(&self) -> f64 {
        DefaultRateCalculator.calculate_rate(self.responses_received, self.elapsed_duration)
    }

    /// Returns average scraped items per second over total elapsed duration.
    pub fn items_per_second(&self) -> f64 {
        DefaultRateCalculator.calculate_rate(self.items_scraped, self.elapsed_duration)
    }

    /// Returns average downloaded bytes per second over total elapsed duration.
    pub fn bytes_per_second(&self) -> f64 {
        DefaultRateCalculator.calculate_rate(self.total_bytes_downloaded, self.elapsed_duration)
    }

    /// Formats [`Self::total_bytes_downloaded`] into a human-readable size string.
    pub fn formatted_bytes(&self) -> String {
        DefaultByteFormatter.formatted_bytes(self.total_bytes_downloaded)
    }

    /// Formats [`Self::bytes_per_second`] into a human-readable rate string.
    pub fn formatted_bytes_per_second(&self) -> String {
        format!(
            "{}/s",
            DefaultByteFormatter.formatted_bytes(self.bytes_per_second() as usize)
        )
    }
}

/// Trait for metrics collectors that can produce a snapshot value.
pub trait SnapshotProvider {
    /// Snapshot type produced by this provider.
    type Snapshot: Clone;

    /// Builds a snapshot of the current metrics state.
    fn create_snapshot(&self) -> Self::Snapshot;
}

/// Trait for exporting metrics into multiple output formats.
pub trait MetricsExporter<T> {
    /// Exports metrics as compact JSON.
    ///
    /// # Errors
    ///
    /// Returns an error when serialization fails.
    fn to_json_string(&self) -> Result<String, crate::error::SpiderError>;

    /// Exports metrics as pretty-printed JSON.
    ///
    /// # Errors
    ///
    /// Returns an error when serialization fails.
    fn to_json_string_pretty(&self) -> Result<String, crate::error::SpiderError>;

    /// Exports metrics as a Markdown report.
    fn to_markdown_string(&self) -> String;

    /// Exports metrics as a plain-text display report.
    fn to_display_string(&self) -> String;
}

/// Default formatter for human-readable metrics display output.
pub struct MetricsDisplayFormatter;

impl MetricsDisplayFormatter {
    /// Formats a snapshot provider into a multi-line summary string.
    pub fn format_metrics<T: MetricsSnapshotProvider>(&self, snapshot: &T) -> String {
        format!("\n{}\n", format_plain_text_metrics(snapshot))
    }
}

/// Formats a metrics snapshot provider into the shared plain-text terminal layout.
pub fn format_plain_text_metrics<T: MetricsSnapshotProvider>(snapshot: &T) -> String {
    let overall_req_per_sec = calculate_rate(
        snapshot.get_requests_sent(),
        snapshot.get_elapsed_duration(),
    );
    let overall_resp_per_sec = calculate_rate(
        snapshot.get_responses_received(),
        snapshot.get_elapsed_duration(),
    );
    let overall_item_per_sec = calculate_rate(
        snapshot.get_items_scraped(),
        snapshot.get_elapsed_duration(),
    );
    let pending_requests = snapshot.get_requests_enqueued().saturating_sub(
        snapshot.get_requests_succeeded()
            + snapshot.get_requests_failed()
            + snapshot.get_requests_dropped(),
    );
    let success_ratio = format_ratio(
        snapshot.get_requests_succeeded(),
        snapshot.get_requests_sent(),
    );
    let failure_ratio = format_ratio(snapshot.get_requests_failed(), snapshot.get_requests_sent());
    let cache_hit_ratio = format_ratio(
        snapshot.get_responses_from_cache(),
        snapshot.get_responses_received(),
    );
    let bytes_per_second = format_byte_rate(
        snapshot.get_total_bytes_downloaded(),
        snapshot.get_elapsed_duration(),
    );

    format!(
        "Crawl Statistics\n\
         ----------------\n\
         duration : {}\n\
         speed    : req/s {:.2}, resp/s {:.2}, item/s {:.2}\n\
         requests : enqueued {}, sent {}, pending {}, ok {}, fail {}\n\
         retry    : retry {}, scheduled {}, drop {}\n\
         ratios   : success {}, failure {}, cache hit {}\n\
         response : received {}, cache {}, downloaded {}, bytes/s {}\n\
         delay    : retry in flight {} ms\n\
         items    : scraped {}, processed {}, dropped {}\n\
         req time : avg {}, fastest {}, slowest {}, total {}\n\
         parsing  : avg {}, fastest {}, slowest {}, total {}\n\
         status   : {}",
        snapshot.formatted_duration(),
        overall_req_per_sec,
        overall_resp_per_sec,
        overall_item_per_sec,
        snapshot.get_requests_enqueued(),
        snapshot.get_requests_sent(),
        pending_requests,
        snapshot.get_requests_succeeded(),
        snapshot.get_requests_failed(),
        snapshot.get_requests_retried(),
        snapshot.get_requests_scheduled_for_retry(),
        snapshot.get_requests_dropped(),
        success_ratio,
        failure_ratio,
        cache_hit_ratio,
        snapshot.get_responses_received(),
        snapshot.get_responses_from_cache(),
        snapshot.formatted_bytes(),
        bytes_per_second,
        snapshot.get_retry_delay_in_flight_ms(),
        snapshot.get_items_scraped(),
        snapshot.get_items_processed(),
        snapshot.get_items_dropped_by_pipeline(),
        snapshot.formatted_request_time(snapshot.get_average_request_time()),
        snapshot.formatted_request_time(snapshot.get_fastest_request_time()),
        snapshot.formatted_request_time(snapshot.get_slowest_request_time()),
        snapshot.get_request_time_count(),
        snapshot.formatted_request_time(snapshot.get_average_parsing_time()),
        snapshot.formatted_request_time(snapshot.get_fastest_parsing_time()),
        snapshot.formatted_request_time(snapshot.get_slowest_parsing_time()),
        snapshot.get_parsing_time_count(),
        format_status_counts(snapshot.get_response_status_counts())
    )
}

fn format_status_counts(status_counts: &std::collections::HashMap<u16, usize>) -> String {
    if status_counts.is_empty() {
        return "none".to_string();
    }

    let mut status_entries = status_counts
        .iter()
        .map(|(code, count)| (*code, *count))
        .collect::<Vec<_>>();
    status_entries.sort_unstable_by_key(|(code, _)| *code);

    status_entries
        .into_iter()
        .map(|(code, count)| format!("{code}: {count}"))
        .collect::<Vec<_>>()
        .join(", ")
}

fn calculate_rate(count: usize, elapsed_duration: Duration) -> f64 {
    DefaultRateCalculator.calculate_rate(count, elapsed_duration)
}

fn format_ratio(numerator: usize, denominator: usize) -> String {
    if denominator == 0 {
        return "0.00%".to_string();
    }

    format!("{:.2}%", (numerator as f64 / denominator as f64) * 100.0)
}

fn format_byte_rate(total_bytes: usize, elapsed_duration: Duration) -> String {
    let bytes_per_second = calculate_rate(total_bytes, elapsed_duration);
    format!(
        "{}/s",
        DefaultByteFormatter.formatted_bytes(bytes_per_second as usize)
    )
}

/// Read-only accessor interface consumed by metrics display/export formatters.
pub trait MetricsSnapshotProvider {
    fn get_requests_enqueued(&self) -> usize;
    fn get_requests_sent(&self) -> usize;
    fn get_requests_succeeded(&self) -> usize;
    fn get_requests_failed(&self) -> usize;
    fn get_requests_retried(&self) -> usize;
    fn get_requests_scheduled_for_retry(&self) -> usize;
    fn get_requests_dropped(&self) -> usize;
    fn get_retry_delay_in_flight_ms(&self) -> u64;
    fn get_responses_received(&self) -> usize;
    fn get_responses_from_cache(&self) -> usize;
    fn get_total_bytes_downloaded(&self) -> usize;
    fn get_items_scraped(&self) -> usize;
    fn get_items_processed(&self) -> usize;
    fn get_items_dropped_by_pipeline(&self) -> usize;
    fn get_response_status_counts(&self) -> &std::collections::HashMap<u16, usize>;
    fn get_elapsed_duration(&self) -> Duration;
    fn get_average_request_time(&self) -> Option<Duration>;
    fn get_fastest_request_time(&self) -> Option<Duration>;
    fn get_slowest_request_time(&self) -> Option<Duration>;
    fn get_request_time_count(&self) -> usize;
    fn get_average_parsing_time(&self) -> Option<Duration>;
    fn get_fastest_parsing_time(&self) -> Option<Duration>;
    fn get_slowest_parsing_time(&self) -> Option<Duration>;
    fn get_parsing_time_count(&self) -> usize;
    fn get_recent_requests_per_second(&self) -> f64;
    fn get_recent_responses_per_second(&self) -> f64;
    fn get_recent_items_per_second(&self) -> f64;
    fn formatted_duration(&self) -> String;
    fn formatted_request_time(&self, duration: Option<Duration>) -> String;
    fn formatted_bytes(&self) -> String;
}

impl MetricsSnapshotProvider for MetricsSnapshot {
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

    fn get_response_status_counts(&self) -> &std::collections::HashMap<u16, usize> {
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
