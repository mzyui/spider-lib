use async_trait::async_trait;
use spider_core::builder::CrawlerBuilder;
use spider_core::config::CrawlerConfig;
use spider_core::engine::{
    SharedMiddlewareManager, process_request_through_middlewares, schedule_retry_for_test,
};
use spider_core::scheduler::Scheduler;
use spider_core::spider::Spider;
use spider_core::stats::StatCollector;
use spider_downloader::Downloader;
use spider_middleware::middleware::{Middleware, MiddlewareAction};
use spider_util::error::SpiderError;
use spider_util::item::{ParseOutput, ScrapedItem};
use spider_util::response::Response;
use std::any::Any;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::sync::Barrier;
use tokio::time::Instant;
use url::Url;

#[derive(Debug, Clone)]
struct TestItem;

impl ScrapedItem for TestItem {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn box_clone(&self) -> Box<dyn ScrapedItem + Send + Sync> {
        Box::new(self.clone())
    }

    fn to_json_value(&self) -> serde_json::Value {
        serde_json::Value::Null
    }
}

struct TestSpider;

#[async_trait]
impl Spider for TestSpider {
    type Item = TestItem;
    type State = ();

    fn start_requests(&self) -> Result<spider_core::spider::StartRequests<'_>, SpiderError> {
        Ok(spider_core::spider::StartRequests::Iter(Box::new(
            std::iter::empty(),
        )))
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
fn default_shutdown_grace_period_is_five_seconds() {
    let config = CrawlerConfig::default();

    assert_eq!(config.shutdown_grace_period, Duration::from_secs(5));
}

#[test]
fn shutdown_grace_period_must_be_non_zero() {
    let config = CrawlerConfig::default().with_shutdown_grace_period(Duration::ZERO);

    assert_eq!(
        config.validate(),
        Err("shutdown_grace_period must be greater than 0".to_string())
    );
}

#[test]
fn shutdown_grace_period_builder_sets_config_value() {
    let builder = CrawlerBuilder::new(TestSpider).shutdown_grace_period(Duration::from_secs(2));

    assert_eq!(
        builder.test_config().shutdown_grace_period,
        Duration::from_secs(2)
    );
}

#[test]
fn clearing_timing_caches_resets_aggregates() {
    let stats = StatCollector::default();
    stats.record_request_time("https://example.com/1", Duration::from_millis(10));
    stats.record_request_time("https://example.com/2", Duration::from_millis(20));
    stats.record_parsing_time(Duration::from_millis(5));

    stats.clear_request_times();
    stats.clear_parsing_times();

    assert_eq!(stats.request_time_count(), 0);
    assert_eq!(stats.average_request_time(), None);
    assert_eq!(stats.fastest_request_time(), None);
    assert_eq!(stats.slowest_request_time(), None);
    assert_eq!(stats.parsing_time_count(), 0);
    assert_eq!(stats.average_parsing_time(), None);
}

#[test]
fn live_report_uses_shared_colon_layout_and_sorted_statuses() {
    let stats = StatCollector::default();

    stats
        .requests_enqueued
        .store(1050, std::sync::atomic::Ordering::Release);
    stats
        .requests_sent
        .store(1050, std::sync::atomic::Ordering::Release);
    stats
        .requests_succeeded
        .store(1050, std::sync::atomic::Ordering::Release);
    stats
        .responses_received
        .store(1050, std::sync::atomic::Ordering::Release);
    stats
        .total_bytes_downloaded
        .store(20_910_000, std::sync::atomic::Ordering::Release);
    stats
        .items_scraped
        .store(1000, std::sync::atomic::Ordering::Release);
    stats
        .items_processed
        .store(1000, std::sync::atomic::Ordering::Release);
    stats
        .requests_scheduled_for_retry
        .store(7, std::sync::atomic::Ordering::Release);
    stats
        .retry_delay_in_flight_ms
        .store(1234, std::sync::atomic::Ordering::Release);
    stats.response_status_counts.insert(500, 3);
    stats.response_status_counts.insert(200, 1050);
    stats.response_status_counts.insert(404, 2);

    stats.record_request_time("https://example.com/1", Duration::from_millis(508));
    stats.record_request_time("https://example.com/2", Duration::from_millis(274));
    stats.record_request_time("https://example.com/3", Duration::from_millis(1_850));
    stats.record_parsing_time(Duration::from_millis(4));
    stats.record_parsing_time(Duration::from_millis(0));
    stats.record_parsing_time(Duration::from_millis(27));

    let report = stats.to_live_report_string();

    assert!(report.contains("Crawl Statistics\n----------------\nduration :"));
    assert!(report.contains("speed    : req/s "));
    assert!(report.contains("requests : enqueued 1050, sent 1050, pending 0, ok 1050, fail 0"));
    assert!(report.contains("retry    : retry 0, scheduled 7, drop 0"));
    assert!(report.contains("ratios   : success 100.00%, failure 0.00%, cache hit 0.00%"));
    assert!(report.contains("response : received 1050, cache 0, downloaded "));
    assert!(report.contains("delay    : retry in flight 1234 ms"));
    assert!(report.contains("req time : avg 877 ms, fastest 274 ms, slowest 1.85 s, total 3"));
    assert!(report.contains("parsing  : avg 10 ms, fastest 0 ms, slowest 27 ms, total 3"));
    assert!(report.ends_with("status   : 200: 1050, 404: 2, 500: 3"));
}

#[test]
fn pending_requests_excludes_dropped_requests_that_never_sent() {
    let stats = StatCollector::default();
    stats
        .requests_enqueued
        .store(170, std::sync::atomic::Ordering::Release);
    stats
        .requests_sent
        .store(145, std::sync::atomic::Ordering::Release);
    stats
        .requests_succeeded
        .store(145, std::sync::atomic::Ordering::Release);
    stats
        .requests_dropped
        .store(25, std::sync::atomic::Ordering::Release);

    let report = stats.to_live_report_string();

    assert!(report.contains("requests : enqueued 170, sent 145, pending 0, ok 145, fail 0"));
    assert!(report.contains("retry    : retry 0, scheduled 0, drop 25"));
}

#[test]
fn display_wraps_live_report_with_blank_lines() {
    let stats = StatCollector::default();
    let display = format!("{stats}");

    assert!(display.starts_with("\nCrawl Statistics\n"));
    assert!(display.ends_with('\n'));
}

#[test]
fn markdown_report_includes_derived_metrics() {
    let stats = StatCollector::default();
    stats
        .requests_enqueued
        .store(10, std::sync::atomic::Ordering::Release);
    stats
        .requests_sent
        .store(8, std::sync::atomic::Ordering::Release);
    stats
        .requests_succeeded
        .store(6, std::sync::atomic::Ordering::Release);
    stats
        .requests_failed
        .store(2, std::sync::atomic::Ordering::Release);
    stats
        .requests_scheduled_for_retry
        .store(3, std::sync::atomic::Ordering::Release);
    stats
        .retry_delay_in_flight_ms
        .store(450, std::sync::atomic::Ordering::Release);
    stats
        .responses_received
        .store(5, std::sync::atomic::Ordering::Release);
    stats
        .responses_from_cache
        .store(2, std::sync::atomic::Ordering::Release);
    stats
        .total_bytes_downloaded
        .store(2048, std::sync::atomic::Ordering::Release);

    let report = stats.to_markdown_string();

    assert!(report.contains("**Bytes Per Second**:"));
    assert!(report.contains("**Request Ratios**: success 75.00%, failure 25.00%"));
    assert!(report.contains("**Cache Hit Ratio**: 40.00%"));
    assert!(report.contains("| Pending    | 2"));
    assert!(report.contains("| Retry Scheduled | 3 |"));
    assert!(report.contains("| Retry Delay In Flight | 450 ms |"));
}

struct ConcurrentProbeMiddleware {
    barrier: Arc<Barrier>,
    delay: Duration,
}

#[async_trait]
impl Middleware<()> for ConcurrentProbeMiddleware {
    fn name(&self) -> &str {
        "ConcurrentProbeMiddleware"
    }

    async fn process_request(
        &self,
        _client: &(),
        request: spider_util::request::Request,
    ) -> Result<MiddlewareAction<spider_util::request::Request>, SpiderError> {
        self.barrier.wait().await;
        tokio::time::sleep(self.delay).await;
        Ok(MiddlewareAction::Continue(request))
    }
}

#[tokio::test]
async fn shared_manager_does_not_serialize_concurrent_requests() {
    let request_count = 4;
    let barrier = Arc::new(Barrier::new(request_count));
    let manager = SharedMiddlewareManager::new(vec![Box::new(ConcurrentProbeMiddleware {
        barrier,
        delay: Duration::from_millis(40),
    })]);

    let start = Instant::now();
    let mut tasks = Vec::new();
    for idx in 0..request_count {
        let manager = manager.clone();
        tasks.push(tokio::spawn(async move {
            let request = spider_util::request::Request::new(
                Url::parse(&format!("https://example.com/{idx}")).unwrap(),
            );
            manager.process_request(&(), request).await.unwrap()
        }));
    }

    for task in tasks {
        task.await.unwrap();
    }

    assert!(
        start.elapsed() < Duration::from_millis(120),
        "middleware processing was serialized: {:?}",
        start.elapsed()
    );
}

#[derive(Clone)]
struct TestClient;

struct NoopDownloader {
    client: TestClient,
}

#[async_trait]
impl Downloader for NoopDownloader {
    type Client = TestClient;

    async fn download(
        &self,
        _request: spider_util::request::Request,
    ) -> Result<Response, SpiderError> {
        panic!("downloader should not be called for request middleware failures");
    }

    fn client(&self) -> &Self::Client {
        &self.client
    }
}

struct BlockingMiddleware;

#[async_trait]
impl Middleware<TestClient> for BlockingMiddleware {
    fn name(&self) -> &str {
        "BlockingMiddleware"
    }

    async fn process_request(
        &self,
        _client: &TestClient,
        _request: spider_util::request::Request,
    ) -> Result<MiddlewareAction<spider_util::request::Request>, SpiderError> {
        Err(SpiderError::BlockedByRobotsTxt)
    }
}

struct ErroringMiddleware;

#[async_trait]
impl Middleware<TestClient> for ErroringMiddleware {
    fn name(&self) -> &str {
        "ErroringMiddleware"
    }

    async fn process_request(
        &self,
        _client: &TestClient,
        _request: spider_util::request::Request,
    ) -> Result<MiddlewareAction<spider_util::request::Request>, SpiderError> {
        Err(SpiderError::GeneralError(
            "middleware rejected request".to_string(),
        ))
    }
}

#[tokio::test]
async fn schedule_retry_noops_when_scheduler_is_already_shutting_down() {
    let (scheduler, _rx) = Scheduler::new(None, 32);
    scheduler.shutdown().await.unwrap();

    let stats = Arc::new(StatCollector::new());
    let request =
        spider_util::request::Request::new(Url::parse("https://example.com/retry").unwrap());

    schedule_retry_for_test(
        Arc::clone(&scheduler),
        request,
        Duration::from_millis(10),
        true,
        Arc::clone(&stats),
    )
    .await;

    tokio::time::sleep(Duration::from_millis(20)).await;

    assert_eq!(
        stats.requests_scheduled_for_retry.load(Ordering::Acquire),
        0
    );
    assert_eq!(stats.retry_delay_in_flight_ms.load(Ordering::Acquire), 0);
}

#[tokio::test]
async fn scheduled_retry_does_not_requeue_after_shutdown_begins() {
    let (scheduler, rx) = Scheduler::new(None, 32);
    let stats = Arc::new(StatCollector::new());
    let request =
        spider_util::request::Request::new(Url::parse("https://example.com/retry-late").unwrap());

    schedule_retry_for_test(
        Arc::clone(&scheduler),
        request,
        Duration::from_millis(30),
        true,
        Arc::clone(&stats),
    )
    .await;

    tokio::time::sleep(Duration::from_millis(5)).await;
    scheduler.shutdown().await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    assert!(rx.try_recv().ok().flatten().is_none());
    assert_eq!(
        stats.requests_scheduled_for_retry.load(Ordering::Acquire),
        1
    );
    assert_eq!(stats.retry_delay_in_flight_ms.load(Ordering::Acquire), 0);
}

#[tokio::test]
async fn blocked_request_middleware_counts_as_dropped_without_sending() {
    let (scheduler, _rx) = Scheduler::new(None, 32);
    let stats = Arc::new(StatCollector::new());
    let request =
        spider_util::request::Request::new(Url::parse("https://example.com/blocked").unwrap());
    let downloader: Arc<dyn Downloader<Client = TestClient> + Send + Sync> =
        Arc::new(NoopDownloader { client: TestClient });
    let middlewares = SharedMiddlewareManager::new(vec![
        Box::new(BlockingMiddleware) as Box<dyn Middleware<TestClient> + Send + Sync>
    ]);

    let result = process_request_through_middlewares::<TestSpider, TestClient>(
        request,
        &downloader,
        &middlewares,
        &scheduler,
        true,
        &stats,
    )
    .await;

    assert!(
        result
            .expect("request middleware failure should not abort flow")
            .is_none()
    );
    assert_eq!(stats.requests_dropped.load(Ordering::Acquire), 1);
    assert_eq!(stats.requests_sent.load(Ordering::Acquire), 0);
    assert_eq!(stats.requests_failed.load(Ordering::Acquire), 0);
}

#[tokio::test]
async fn generic_request_middleware_error_counts_as_dropped_without_sending() {
    let (scheduler, _rx) = Scheduler::new(None, 32);
    let stats = Arc::new(StatCollector::new());
    let request =
        spider_util::request::Request::new(Url::parse("https://example.com/error").unwrap());
    let downloader: Arc<dyn Downloader<Client = TestClient> + Send + Sync> =
        Arc::new(NoopDownloader { client: TestClient });
    let middlewares = SharedMiddlewareManager::new(vec![
        Box::new(ErroringMiddleware) as Box<dyn Middleware<TestClient> + Send + Sync>
    ]);

    let result = process_request_through_middlewares::<TestSpider, TestClient>(
        request,
        &downloader,
        &middlewares,
        &scheduler,
        true,
        &stats,
    )
    .await;

    assert!(
        result
            .expect("request middleware error should not abort flow")
            .is_none()
    );
    assert_eq!(stats.requests_dropped.load(Ordering::Acquire), 1);
    assert_eq!(stats.requests_sent.load(Ordering::Acquire), 0);
    assert_eq!(stats.requests_failed.load(Ordering::Acquire), 0);
}
