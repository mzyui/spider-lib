//! # Scheduler Module
//!
//! Implements the request scheduler for managing the crawling frontier and duplicate detection.
//!
//! ## Overview
//!
//! The [`Scheduler`] is a central component that coordinates the web crawling process
//! by managing the queue of pending requests and tracking visited URLs to prevent
//! duplicate processing. It uses an actor-like design pattern with internal message
//! processing for thread-safe operations.
//!
//! ## Key Responsibilities
//!
//! - **Request Queue Management**: Maintains a queue of pending requests to be processed
//! - **Duplicate Detection**: Tracks visited URLs using a [`BloomFilter`](spider_util::bloom::BloomFilter)
//!   and LRU cache for efficiency
//! - **Request Salvaging**: Handles failed enqueuing attempts to prevent request loss
//! - **State Snapshots**: Provides checkpointing capabilities for crawl resumption
//! - **Concurrent Access**: Thread-safe operations for multi-threaded crawling
//!
//! ## Architecture
//!
//! The scheduler operates asynchronously using an internal message queue to handle
//! operations like request enqueuing, URL marking, and state snapshots. It combines
//! a Bloom Filter for fast preliminary duplicate checks with an LRU cache for
//! definitive tracking, optimizing performance when handling millions of URLs.
//!
//! ## Example
//!
//! ```rust,ignore
//! use spider_core::Scheduler;
//! use spider_util::request::Request;
//! use url::Url;
//!
//! let (scheduler, request_receiver) = Scheduler::new(None);
//!
//! // Enqueue a request
//! let request = Request::new(Url::parse("https://example.com").unwrap());
//! scheduler.enqueue_request(request).await?;
//!
//! // Mark a URL as visited
//! scheduler.mark_visited("unique_fingerprint".to_string()).await?;
//! ```

#[cfg(feature = "checkpoint")]
use spider_util::constants::DEFAULT_VISITED_CACHE_SIZE;
#[cfg(feature = "checkpoint")]
use crate::SchedulerCheckpoint;

use spider_util::constants::{
    BLOOM_FILTER_CAPACITY, BLOOM_FILTER_HASH_FUNCTIONS,
    MAX_PENDING_REQUESTS,
    VISITED_URL_CACHE_CAPACITY, VISITED_URL_CACHE_TTL_SECS,
};
use spider_util::error::SpiderError;
use spider_util::request::Request;
use crossbeam::queue::SegQueue;
use kanal::{AsyncReceiver, AsyncSender, bounded_async, unbounded_async};
use moka::sync::Cache;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use log::{debug, error, info, trace, warn};

/// Internal messages sent to the scheduler's event loop.
enum SchedulerMessage {
    /// Enqueue a new request for processing.
    Enqueue(Arc<Request>),
    /// Mark a URL fingerprint as visited.
    MarkAsVisited(String),
    /// Mark multiple URL fingerprints as visited in a batch.
    MarkAsVisitedBatch(Vec<String>),
    /// Signal the scheduler to shut down.
    Shutdown,
}

use spider_util::bloom::BloomFilter;

use tokio::sync::Notify;
use parking_lot::Mutex;

/// Manages the request queue and tracks visited URLs to prevent duplicate crawling.
///
/// The [`Scheduler`] is responsible for:
/// - Maintaining a queue of pending requests
/// - Tracking which URLs have been visited using a Bloom Filter and LRU cache
/// - Providing backpressure when too many requests are pending
/// - Supporting checkpoint-based state restoration
///
/// ## Architecture
///
/// The scheduler runs as a separate async task and communicates via message passing.
/// This design ensures thread-safe access without requiring explicit locks.
///
/// ## Duplicate Detection
///
/// The scheduler uses a two-tier approach for duplicate detection:
/// 1. **Bloom Filter**: Fast, memory-efficient probabilistic check (may have false positives)
/// 2. **LRU Cache**: Definitive check with TTL-based eviction
///
/// Requests are first checked against the Bloom Filter. If it indicates a possible
/// duplicate, the LRU cache is consulted for confirmation.
pub struct Scheduler {
    /// Queue of pending requests waiting to be processed.
    queue: SegQueue<Request>,
    /// Cache of visited URL fingerprints with TTL-based eviction.
    visited: Cache<String, bool>,
    /// Bloom filter for fast preliminary duplicate detection.
    bloom: std::sync::Arc<parking_lot::RwLock<BloomFilter>>,
    /// Buffer for batching Bloom filter updates.
    buffer: Arc<Mutex<Vec<String>>>,
    /// Notifier for triggering buffer flushes.
    notify: Arc<Notify>,
    /// Sender for internal scheduler messages.
    tx: AsyncSender<SchedulerMessage>,
    /// Count of pending requests (queued + in-flight).
    pending: AtomicUsize,
    /// Queue of requests that could not be enqueued and were salvaged.
    salvaged: SegQueue<Request>,
    /// Flag indicating if the scheduler is shutting down.
    pub(crate) is_shutting_down: AtomicBool,
    /// Maximum number of pending requests before applying backpressure.
    max_pending: usize,
}

impl Scheduler {
    /// Creates a new [`Scheduler`] and returns a tuple containing the scheduler and a request receiver.
    ///
    /// This method initializes the scheduler with optional checkpoint state for resuming
    /// interrupted crawls. When checkpoint data is provided, the scheduler restores:
    /// - Pending request queue
    /// - Visited URL cache
    /// - Salvaged requests
    ///
    /// The scheduler spawns two background tasks:
    /// 1. **Bloom Filter Flush Task**: Periodically flushes the URL fingerprint buffer
    /// 2. **Run Loop Task**: Processes incoming messages and dispatches requests
    ///
    /// ## Parameters
    ///
    /// - `initial_state`: Optional checkpoint state to restore from a previous crawl
    ///
    /// ## Returns
    ///
    /// A tuple of `(Arc<Scheduler>, AsyncReceiver<Request>)`:
    /// - `Arc<Scheduler>`: Thread-safe reference to the scheduler for sending commands
    /// - `AsyncReceiver<Request>`: Channel receiver for consuming scheduled requests
    ///
    /// ## Example
    ///
    /// ```rust
    /// # use spider_core::Scheduler;
    /// let (scheduler, request_rx) = Scheduler::new(None::<()>);
    /// ```
    pub fn new(
        #[cfg(feature = "checkpoint")] initial_state: Option<SchedulerCheckpoint>,
        #[cfg(not(feature = "checkpoint"))] _initial_state: Option<()>,
    ) -> (Arc<Self>, AsyncReceiver<Request>) {
        let (tx, rx_internal) = unbounded_async();
        let (tx_out, rx_out) = bounded_async(100);

        let queue: SegQueue<Request>;
        let visited: Cache<String, bool>;
        let pending: AtomicUsize;
        let salvaged: SegQueue<Request>;

        #[cfg(feature = "checkpoint")]
        {
            if let Some(state) = initial_state {
                info!(
                    "Initializing scheduler from checkpoint with {} requests, {} visited URLs, and {} salvaged requests.",
                    state.request_queue.len(),
                    state.visited_urls.len(),
                    state.salvaged_requests.len(),
                );
                let pend = state.request_queue.len() + state.salvaged_requests.len();
                queue = SegQueue::new();
                for request in state.request_queue {
                    queue.push(request);
                }

                visited = Cache::builder()
                    .max_capacity(VISITED_URL_CACHE_CAPACITY)
                    .time_to_idle(std::time::Duration::from_secs(VISITED_URL_CACHE_TTL_SECS))
                    .eviction_listener(|_key, _value, _cause| {})
                    .build();
                for url in state.visited_urls {
                    visited.insert(url, true);
                }

                pending = AtomicUsize::new(pend);
                salvaged = SegQueue::new();
                for request in state.salvaged_requests {
                    salvaged.push(request);
                }
            } else {
                queue = SegQueue::new();
                visited = Cache::builder().max_capacity(DEFAULT_VISITED_CACHE_SIZE).build();
                pending = AtomicUsize::new(0);
                salvaged = SegQueue::new();
            }
        }

        #[cfg(not(feature = "checkpoint"))]
        {
            queue = SegQueue::new();
            visited = Cache::builder()
                .max_capacity(VISITED_URL_CACHE_CAPACITY)
                .time_to_idle(std::time::Duration::from_secs(VISITED_URL_CACHE_TTL_SECS))
                .eviction_listener(|_key, _value, _cause| {})
                .build();
            pending = AtomicUsize::new(0);
            salvaged = SegQueue::new();
        }

        let buffer = Arc::new(Mutex::new(Vec::new()));
        let notify = Arc::new(Notify::new());

        let scheduler = Arc::new(Scheduler {
            queue,
            visited,
            bloom: std::sync::Arc::new(parking_lot::RwLock::new(BloomFilter::new(
                BLOOM_FILTER_CAPACITY,
                BLOOM_FILTER_HASH_FUNCTIONS,
            ))),
            buffer: buffer.clone(),
            notify: notify.clone(),
            tx,
            pending,
            salvaged,
            is_shutting_down: AtomicBool::new(false),
            max_pending: MAX_PENDING_REQUESTS,
        });

        let scheduler_bloom = Arc::clone(&scheduler);
        let buffer_clone = buffer.clone();
        let notify_clone = notify.clone();
        tokio::spawn(async move {
            scheduler_bloom.flush_buffer(buffer_clone, notify_clone).await;
        });

        let scheduler_task = Arc::clone(&scheduler);
        tokio::spawn(async move {
            scheduler_task.run_loop(rx_internal, tx_out).await;
        });

        (scheduler, rx_out)
    }

    async fn run_loop(
        &self,
        rx_internal: AsyncReceiver<SchedulerMessage>,
        tx_out: AsyncSender<Request>,
    ) {
        info!(
            "Scheduler run_loop started with max pending: {}",
            self.max_pending
        );
        loop {
            if let Ok(Some(msg)) = rx_internal.try_recv() {
                trace!("Processing pending internal message");
                if !self.handle_message(Ok(msg)).await {
                    break;
                }
                continue;
            }

            let request = if !tx_out.is_closed() && !self.is_idle() {
                self.queue.pop()
            } else {
                None
            };

            if let Some(request) = request {
                trace!("Sending request to crawler: {}", request.url);
                tokio::select! {
                    send_res = tx_out.send(request) => {
                        if send_res.is_err() {
                            error!("Crawler receiver dropped. Scheduler can no longer send requests.");
                        } else {
                            trace!("Successfully sent request to crawler");
                        }
                        self.pending.fetch_sub(1, Ordering::AcqRel);
                    },
                    recv_res = rx_internal.recv() => {
                        trace!("Received internal message while sending request");
                        if !self.handle_message(recv_res).await {
                            break;
                        }
                        continue;
                    }
                }
            } else {
                trace!("No pending requests, waiting for internal message");
                if !self.handle_message(rx_internal.recv().await).await {
                    break;
                }
            }
        }
        info!(
            "Scheduler run_loop finished with {} pending requests remaining.",
            self.pending.load(Ordering::SeqCst)
        );
    }

    async fn handle_message(&self, msg: Result<SchedulerMessage, kanal::ReceiveError>) -> bool {
        match msg {
            Ok(SchedulerMessage::Enqueue(arc_request)) => {
                // Arc allows sharing without cloning; if this is the only reference, we can unwrap
                let request = Arc::unwrap_or_clone(arc_request);
                trace!("Enqueuing request: {}", request.url);
                self.queue.push(request);
                self.pending.fetch_add(1, Ordering::AcqRel);
                true
            }
            Ok(SchedulerMessage::MarkAsVisited(fingerprint)) => {
                trace!("Marking URL fingerprint as visited: {}", fingerprint);

                // Insert into visited cache first (clone needed for cache)
                self.visited.insert(fingerprint.clone(), true);

                // Log before moving fingerprint
                debug!("Marked URL as visited: {}", fingerprint);

                // Then move fingerprint into buffer (no clone needed)
                {
                    let mut buffer = self.buffer.lock();
                    buffer.push(fingerprint);
                    if buffer.len() >= 100 {
                        self.notify.notify_one();
                    }
                }

                true
            }
            Ok(SchedulerMessage::MarkAsVisitedBatch(mut fingerprints)) => {
                let count = fingerprints.len();
                trace!("Marking {} URL fingerprints as visited in batch", count);
                
                // Insert all fingerprints into visited cache
                for fingerprint in &fingerprints {
                    self.visited.insert(fingerprint.clone(), true);
                }

                // Then extend buffer with the fingerprints (no clone needed)
                {
                    let mut buffer = self.buffer.lock();
                    buffer.append(&mut fingerprints);
                    if buffer.len() >= 100 {
                        self.notify.notify_one();
                    }
                }

                debug!("Marked {} URLs as visited in batch", count);
                true
            }
            Ok(SchedulerMessage::Shutdown) => {
                info!("Scheduler received shutdown signal. Exiting run_loop.");
                self.is_shutting_down.store(true, Ordering::SeqCst);
                self.flush_buffer_now();
                false
            }
            Err(_) => {
                warn!("Scheduler internal message channel closed. Exiting run_loop.");
                self.is_shutting_down.store(true, Ordering::SeqCst);
                false
            }
        }
    }

    #[cfg(feature = "checkpoint")]
    pub async fn snapshot(&self) -> Result<SchedulerCheckpoint, SpiderError> {
        let visited_urls = dashmap::DashSet::new();
        for entry in self.visited.iter() {
            let (key, _) = entry;
            visited_urls.insert(key.as_ref().clone());
        }

        let mut request_queue = std::collections::VecDeque::new();
        let mut temp_requests = Vec::new();

        while let Some(request) = self.queue.pop() {
            temp_requests.push(request);
        }

        for request in temp_requests.into_iter() {
            request_queue.push_back(request.clone());
            if !self.is_shutting_down.load(Ordering::SeqCst) {
                self.queue.push(request);
            }
        }

        let mut salvaged_requests = std::collections::VecDeque::new();
        let mut temp_salvaged = Vec::new();

        while let Some(request) = self.salvaged.pop() {
            temp_salvaged.push(request);
        }

        for request in temp_salvaged.into_iter() {
            salvaged_requests.push_back(request.clone());
            if !self.is_shutting_down.load(Ordering::SeqCst) {
                self.salvaged.push(request);
            }
        }

        Ok(SchedulerCheckpoint {
            request_queue,
            visited_urls,
            salvaged_requests,
        })
    }

    #[cfg(not(feature = "checkpoint"))]
    pub async fn snapshot(&self) -> Result<(), SpiderError> {
        Ok(())
    }

    pub async fn enqueue_request(&self, request: Request) -> Result<(), SpiderError> {
        if !self.should_enqueue(&request) {
            trace!("Request already visited, skipping: {}", request.url);
            return Ok(());
        }

        let pending = self.pending.load(Ordering::SeqCst);
        if pending >= self.max_pending {
            warn!(
                "Maximum pending requests reached ({}), request dropped due to backpressure: {}",
                self.max_pending, request.url
            );
            return Err(SpiderError::GeneralError(
                "Scheduler at maximum capacity, request dropped due to backpressure.".into(),
            ));
        }

        trace!("Enqueuing request: {}", request.url);
        if self
            .tx
            .send(SchedulerMessage::Enqueue(Arc::new(request.clone())))
            .await
            .is_err()
        {
            if !self.is_shutting_down.load(Ordering::SeqCst) {
                error!(
                    "Scheduler internal message channel is closed. Salvaging request: {}",
                    request.url
                );
            }
            self.salvaged.push(request);
            return Err(SpiderError::GeneralError(
                "Scheduler internal channel closed, request salvaged.".into(),
            ));
        }

        trace!("Successfully enqueued request: {}", request.url);
        Ok(())
    }

    pub async fn shutdown(&self) -> Result<(), SpiderError> {
        self.is_shutting_down.store(true, Ordering::SeqCst);

        if !self.tx.is_closed() {
            self.tx
                .send(SchedulerMessage::Shutdown)
                .await
                .map_err(|e| {
                    SpiderError::GeneralError(format!(
                        "Scheduler: Failed to send shutdown signal: {}",
                        e
                    ))
                })
        } else {
            debug!("Scheduler internal channel already closed, skipping shutdown signal");
            Ok(())
        }
    }

    pub async fn mark_visited(&self, fingerprint: String) -> Result<(), SpiderError> {
        trace!(
            "Sending MarkAsVisited message for fingerprint: {}",
            fingerprint
        );
        self.tx
            .send(SchedulerMessage::MarkAsVisited(fingerprint))
            .await
            .map_err(|e| {
                if !self.is_shutting_down.load(Ordering::SeqCst) {
                    error!("Scheduler internal message channel is closed. Failed to mark URL as visited: {}", e);
                }
                SpiderError::GeneralError(format!(
                    "Scheduler: Failed to send MarkAsVisited message: {}",
                    e
                ))
            })
    }

    pub async fn mark_visited_batch(&self, fingerprints: Vec<String>) -> Result<(), SpiderError> {
        if fingerprints.is_empty() {
            return Ok(());
        }

        trace!(
            "Sending MarkAsVisitedBatch message for {} fingerprints",
            fingerprints.len()
        );
        self.tx
            .send(SchedulerMessage::MarkAsVisitedBatch(fingerprints))
            .await
            .map_err(|e| {
                if !self.is_shutting_down.load(Ordering::SeqCst) {
                    error!("Scheduler internal message channel is closed. Failed to mark URLs as visited in batch: {}", e);
                }
                SpiderError::GeneralError(format!(
                    "Scheduler: Failed to send MarkAsVisitedBatch message: {}",
                    e
                ))
            })
    }

    pub fn is_visited(&self, fingerprint: &str) -> bool {
        if !self.bloom.read().might_contain(fingerprint) {
            return false;
        }

        {
            let buffer = self.buffer.lock();
            if buffer.iter().any(|item| item == fingerprint) {
                return true;
            }
        }

        self.visited.contains_key(fingerprint)
    }

    fn flush_buffer_now(&self) {
        let mut buffer = self.buffer.lock();
        if !buffer.is_empty() {
            let items: Vec<String> = buffer.drain(..).collect();
            drop(buffer);

            let mut bloom = self.bloom.write();
            for item in items {
                bloom.add(&item);
            }
        }
    }

    async fn flush_buffer(
        &self,
        _buffer: Arc<Mutex<Vec<String>>>,
        notify: Arc<Notify>,
    ) {
        loop {
            tokio::select! {
                _ = notify.notified() => {
                    self.flush_buffer_now();
                }
                _ = tokio::time::sleep(tokio::time::Duration::from_millis(100)) => {
                    self.flush_buffer_now();
                }
            }

            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }
    }

    pub fn should_enqueue(&self, request: &Request) -> bool {
        let fingerprint = request.fingerprint();
        !self.is_visited(&fingerprint)
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.pending.load(Ordering::Acquire)
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub fn is_idle(&self) -> bool {
        self.is_empty()
    }
}
