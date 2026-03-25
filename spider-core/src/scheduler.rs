//! Request scheduling and duplicate detection.
//!
//! The scheduler is the crawler's frontier manager. It accepts requests,
//! applies backpressure when too many are outstanding, tracks visited
//! fingerprints, and hands accepted requests to the downloader side of the
//! runtime.
//!
//! ## Example
//!
//! ```rust,ignore
//! use spider_core::Scheduler;
//! use spider_util::request::Request;
//! use url::Url;
//!
//! let (scheduler, request_receiver) = Scheduler::new(None, 32);
//!
//! // Enqueue a request
//! let request = Request::new(Url::parse("https://example.com").unwrap());
//! scheduler.enqueue_request(request).await?;
//!
//! // Mark a URL as visited
//! scheduler.mark_visited("unique_fingerprint".to_string()).await?;
//! ```

#[cfg(feature = "checkpoint")]
use crate::SchedulerCheckpoint;
#[cfg(feature = "checkpoint")]
use spider_util::constants::DEFAULT_VISITED_CACHE_SIZE;

use crossbeam::queue::SegQueue;
use kanal::{AsyncReceiver, AsyncSender, bounded_async};
use log::{debug, error, info, trace, warn};
use moka::sync::Cache;
use spider_util::constants::{
    BLOOM_BUFFER_FLUSH_SIZE, BLOOM_FILTER_CAPACITY, BLOOM_FILTER_HASH_FUNCTIONS,
    BLOOM_FLUSH_INTERVAL_MS, MAX_PENDING_REQUESTS, VISITED_URL_CACHE_CAPACITY,
    VISITED_URL_CACHE_TTL_SECS,
};
use spider_util::error::SpiderError;
use spider_util::request::Request;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

/// Internal messages sent to the scheduler's event loop.
enum SchedulerMessage {
    /// Enqueue a new request for processing.
    Enqueue(Arc<Request>),
    /// Enqueue multiple new requests for processing.
    EnqueueBatch(Vec<Arc<Request>>),
    /// Re-enqueue a request without changing the pending counter.
    Requeue(Arc<Request>),
    /// Mark a URL fingerprint as visited.
    MarkAsVisited(String),
    /// Mark multiple URL fingerprints as visited in a batch.
    MarkAsVisitedBatch(Vec<String>),
    /// Signal the scheduler to shut down.
    Shutdown,
}

use spider_util::bloom::BloomFilter;

use parking_lot::Mutex;
use tokio::sync::Notify;

/// Manages the crawl frontier and tracks visited request fingerprints.
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
    buffer: Arc<Mutex<HashSet<String>>>,
    /// Notifier for triggering buffer flushes.
    notify: Arc<Notify>,
    /// Notifier for waking tasks blocked on scheduler capacity.
    capacity_notify: Arc<Notify>,
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
    /// ```rust,ignore
    /// # use spider_core::Scheduler;
    /// let (scheduler, request_rx) = Scheduler::new(None, 32);
    /// ```
    pub fn new(
        #[cfg(feature = "checkpoint")] initial_state: Option<SchedulerCheckpoint>,
        #[cfg(not(feature = "checkpoint"))] _initial_state: Option<()>,
        max_pending_requests: usize,
    ) -> (Arc<Self>, AsyncReceiver<Request>) {
        let max_pending = max_pending_requests.clamp(1, MAX_PENDING_REQUESTS);
        let (tx, rx_internal) = bounded_async(max_pending.saturating_mul(2).max(1));
        let output_capacity = (max_pending / 8).clamp(256, 2048);
        let (tx_out, rx_out) = bounded_async(output_capacity);

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
                visited = Cache::builder()
                    .max_capacity(DEFAULT_VISITED_CACHE_SIZE)
                    .build();
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

        let buffer = Arc::new(Mutex::new(HashSet::new()));
        let notify = Arc::new(Notify::new());
        let capacity_notify = Arc::new(Notify::new());

        let scheduler = Arc::new(Scheduler {
            queue,
            visited,
            bloom: std::sync::Arc::new(parking_lot::RwLock::new(BloomFilter::new(
                BLOOM_FILTER_CAPACITY,
                BLOOM_FILTER_HASH_FUNCTIONS,
            ))),
            buffer: buffer.clone(),
            notify: notify.clone(),
            capacity_notify: Arc::clone(&capacity_notify),
            tx,
            pending,
            salvaged,
            is_shutting_down: AtomicBool::new(false),
            max_pending,
        });

        let scheduler_bloom = Arc::clone(&scheduler);
        let notify_clone = notify.clone();
        tokio::spawn(async move {
            scheduler_bloom.flush_buffer(notify_clone).await;
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
            Ok(SchedulerMessage::EnqueueBatch(requests)) => {
                let count = requests.len();
                for request in requests {
                    self.queue.push(Arc::unwrap_or_clone(request));
                }
                self.pending.fetch_add(count, Ordering::AcqRel);
                true
            }
            Ok(SchedulerMessage::Requeue(arc_request)) => {
                let request = Arc::unwrap_or_clone(arc_request);
                trace!("Re-enqueuing request: {}", request.url);
                self.queue.push(request);
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
                    buffer.insert(fingerprint);
                    if buffer.len() >= BLOOM_BUFFER_FLUSH_SIZE {
                        self.notify.notify_one();
                    }
                }

                true
            }
            Ok(SchedulerMessage::MarkAsVisitedBatch(fingerprints)) => {
                let count = fingerprints.len();
                trace!("Marking {} URL fingerprints as visited in batch", count);

                // Insert all fingerprints into visited cache
                for fingerprint in &fingerprints {
                    self.visited.insert(fingerprint.clone(), true);
                }

                // Then extend buffer with the fingerprints (no clone needed)
                {
                    let mut buffer = self.buffer.lock();
                    buffer.extend(fingerprints);
                    if buffer.len() >= BLOOM_BUFFER_FLUSH_SIZE {
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

        loop {
            let pending = self.pending.load(Ordering::SeqCst);
            if pending < self.max_pending {
                break;
            }

            if self.is_shutting_down.load(Ordering::SeqCst) {
                return Err(SpiderError::GeneralError(
                    "Scheduler is shutting down.".into(),
                ));
            }

            trace!(
                "Scheduler capacity reached ({} pending), waiting to enqueue: {}",
                self.max_pending, request.url
            );
            self.capacity_notify.notified().await;
        }

        trace!("Enqueuing request: {}", request.url);
        let request_arc = Arc::new(request);
        if self
            .tx
            .send(SchedulerMessage::Enqueue(Arc::clone(&request_arc)))
            .await
            .is_err()
        {
            if !self.is_shutting_down.load(Ordering::SeqCst) {
                error!(
                    "Scheduler internal message channel is closed. Salvaging request: {}",
                    request_arc.url
                );
            }
            let salvaged_request =
                Arc::try_unwrap(request_arc).unwrap_or_else(|shared| shared.as_ref().clone());
            self.salvaged.push(salvaged_request);
            return Err(SpiderError::GeneralError(
                "Scheduler internal channel closed, request salvaged.".into(),
            ));
        }

        trace!("Successfully enqueued request: {}", request_arc.url);
        Ok(())
    }

    /// Enqueues multiple requests with a single scheduler message.
    pub async fn enqueue_requests_batch(
        &self,
        requests: Vec<Request>,
    ) -> Result<usize, SpiderError> {
        if requests.is_empty() {
            return Ok(0);
        }

        let mut filtered = Vec::with_capacity(requests.len());
        let mut seen_fingerprints = HashSet::with_capacity(requests.len());
        for request in requests {
            let fingerprint = request.fingerprint();
            if seen_fingerprints.insert(fingerprint) && self.should_enqueue(&request) {
                filtered.push(Arc::new(request));
            }
        }

        if filtered.is_empty() {
            return Ok(0);
        }

        let batch_len = filtered.len();
        loop {
            let pending = self.pending.load(Ordering::SeqCst);
            if pending.saturating_add(batch_len) <= self.max_pending {
                break;
            }

            if self.is_shutting_down.load(Ordering::SeqCst) {
                return Err(SpiderError::GeneralError(
                    "Scheduler is shutting down.".into(),
                ));
            }

            self.capacity_notify.notified().await;
        }

        if self
            .tx
            .send(SchedulerMessage::EnqueueBatch(filtered.clone()))
            .await
            .is_err()
        {
            if !self.is_shutting_down.load(Ordering::SeqCst) {
                error!(
                    "Scheduler internal message channel is closed. Salvaging batch request set."
                );
            }
            for request in filtered {
                let salvaged =
                    Arc::try_unwrap(request).unwrap_or_else(|shared| shared.as_ref().clone());
                self.salvaged.push(salvaged);
            }
            return Err(SpiderError::GeneralError(
                "Scheduler internal channel closed, request batch salvaged.".into(),
            ));
        }

        Ok(batch_len)
    }

    /// Re-enqueues a request while keeping the outstanding request count unchanged.
    ///
    /// This is intended for retries or request replacements that originate from an
    /// already in-flight request lifecycle.
    pub async fn requeue_request(&self, request: Request) -> Result<(), SpiderError> {
        if !self.should_enqueue(&request) {
            trace!(
                "Request already visited during requeue, skipping: {}",
                request.url
            );
            return Ok(());
        }

        let reserved_slot = self
            .pending
            .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
            .is_ok();

        trace!(
            "Re-enqueuing request without changing pending count: {}",
            request.url
        );
        let request_arc = Arc::new(request);
        if self
            .tx
            .send(SchedulerMessage::Requeue(Arc::clone(&request_arc)))
            .await
            .is_err()
        {
            if !self.is_shutting_down.load(Ordering::SeqCst) {
                error!(
                    "Scheduler internal message channel is closed. Salvaging re-queued request: {}",
                    request_arc.url
                );
            }
            let salvaged_request =
                Arc::try_unwrap(request_arc).unwrap_or_else(|shared| shared.as_ref().clone());
            self.salvaged.push(salvaged_request);
            if reserved_slot {
                self.complete_request();
            }
            return Err(SpiderError::GeneralError(
                "Scheduler internal channel closed, request salvaged.".into(),
            ));
        }

        Ok(())
    }

    /// Marks one outstanding request as fully completed.
    pub fn complete_request(&self) {
        let mut current = self.pending.load(Ordering::Acquire);
        loop {
            if current == 0 {
                warn!("Scheduler pending request counter underflow prevented.");
                return;
            }

            match self.pending.compare_exchange_weak(
                current,
                current - 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }

        self.capacity_notify.notify_waiters();
    }

    /// Signals the scheduler loop to stop processing new work.
    ///
    /// # Errors
    ///
    /// Returns an error when the shutdown message cannot be sent.
    pub async fn shutdown(&self) -> Result<(), SpiderError> {
        self.is_shutting_down.store(true, Ordering::SeqCst);

        if !self.tx.is_closed() {
            self.tx.send(SchedulerMessage::Shutdown).await.map_err(|e| {
                SpiderError::GeneralError(format!(
                    "Scheduler: Failed to send shutdown signal: {}",
                    e
                ))
            })
        } else {
            info!("Scheduler internal channel already closed, skipping shutdown signal");
            Ok(())
        }
    }

    /// Marks a single fingerprint as visited.
    ///
    /// # Errors
    ///
    /// Returns an error when the message cannot be delivered to the scheduler loop.
    pub async fn mark_visited(
        &self,
        fingerprint: impl Into<String>,
    ) -> Result<(), SpiderError> {
        let fingerprint = fingerprint.into();
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

    /// Marks multiple fingerprints as visited in one message.
    ///
    /// If `fingerprints` is empty, this method returns immediately.
    ///
    /// # Errors
    ///
    /// Returns an error when the batch message cannot be delivered to the scheduler loop.
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

    /// Returns `true` if `fingerprint` has already been visited.
    pub fn is_visited(&self, fingerprint: &str) -> bool {
        if !self.bloom.read().might_contain(fingerprint) {
            return false;
        }

        {
            let buffer = self.buffer.lock();
            if buffer.contains(fingerprint) {
                return true;
            }
        }

        self.visited.contains_key(fingerprint)
    }

    fn flush_buffer_now(&self) {
        let mut buffer = self.buffer.lock();
        if !buffer.is_empty() {
            let items: Vec<String> = buffer.drain().collect();
            drop(buffer);

            let mut bloom = self.bloom.write();
            for item in items {
                bloom.add(&item);
            }
        }
    }

    async fn flush_buffer(&self, notify: Arc<Notify>) {
        loop {
            tokio::select! {
                _ = notify.notified() => {
                    self.flush_buffer_now();
                }
                _ = tokio::time::sleep(tokio::time::Duration::from_millis(BLOOM_FLUSH_INTERVAL_MS)) => {
                    self.flush_buffer_now();
                }
            }
        }
    }

    /// Returns `true` when `request` has not been visited yet.
    pub fn should_enqueue(&self, request: &Request) -> bool {
        let fingerprint = request.fingerprint();
        !self.is_visited(&fingerprint)
    }

    /// Returns the number of pending requests in the scheduler.
    #[inline]
    pub fn len(&self) -> usize {
        self.pending.load(Ordering::Acquire)
    }

    /// Returns `true` if the scheduler has no pending requests.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns `true` when the scheduler is currently idle.
    #[inline]
    pub fn is_idle(&self) -> bool {
        self.is_empty()
    }
}
