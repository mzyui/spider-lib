use async_trait::async_trait;
use spider_core::config::{CheckpointConfig, CrawlerConfig};
use spider_core::engine::{
    SharedMiddlewareManager, process_request_through_middlewares, spawn_parser_task,
};
use spider_core::scheduler::Scheduler;
use spider_core::spider::{Spider, StartRequests};
use spider_core::state::CrawlerState;
use spider_core::stats::StatCollector;
use spider_downloader::Downloader;
use spider_middleware::middleware::Middleware;
use spider_middleware::retry::RetryMiddleware;
use spider_util::error::{ReqwestError, SpiderError};
use spider_util::item::{ParseOutput, ScrapedItem};
use spider_util::request::Request;
use spider_util::response::Response;
use std::fs;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::Notify;
use url::Url;

#[spider_macro::scraped_item]
struct SeedItem {
    id: usize,
}

struct SeedSpider {
    path: String,
}

#[async_trait]
impl Spider for SeedSpider {
    type Item = SeedItem;
    type State = ();

    fn start_requests(&self) -> Result<StartRequests<'_>, SpiderError> {
        Ok(StartRequests::file(self.path.as_str()))
    }

    async fn parse(
        &self,
        _response: Response,
        _state: &Self::State,
    ) -> Result<ParseOutput<Self::Item>, SpiderError> {
        let _ = SeedItem { id: 1 };
        Ok(ParseOutput::new())
    }
}

fn temp_seed_path() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time before epoch")
        .as_nanos();
    std::env::temp_dir()
        .join(format!("spider_seed_{}_{}.txt", std::process::id(), nanos))
        .display()
        .to_string()
}

#[test]
fn start_requests_reads_seed_file_lazily() {
    let path = temp_seed_path();
    fs::write(
        &path,
        "# comment\n\nhttps://example.com\nbad-url\nhttps://example.org\n",
    )
    .expect("write seed file");

    let spider = SeedSpider { path: path.clone() };
    let requests = spider
        .start_requests()
        .expect("create start request source")
        .into_iter()
        .expect("resolve start request iterator");
    let items: Vec<_> = requests.collect();

    assert_eq!(items.len(), 3);
    assert!(matches!(&items[0], Ok(req) if req.url.as_str() == "https://example.com/"));
    assert!(matches!(&items[1], Err(SpiderError::ConfigurationError(_))));
    assert!(matches!(&items[2], Ok(req) if req.url.as_str() == "https://example.org/"));

    let _ = fs::remove_file(path);
}

#[test]
fn start_requests_fails_when_seed_file_missing() {
    let spider = SeedSpider {
        path: "/tmp/spider_seed_missing_file.txt".to_string(),
    };
    let result = spider.start_requests().and_then(StartRequests::into_iter);
    assert!(matches!(result, Err(SpiderError::IoError(_))));
}

#[test]
fn crawler_config_default() {
    let config = CrawlerConfig::default();
    let cpu_count = num_cpus::get();
    assert!(config.max_concurrent_downloads > 0);
    assert_eq!(config.max_pending_requests, (cpu_count * 4).clamp(16, 1024));
    assert!(config.parser_workers > 0);
    assert!(config.max_concurrent_pipelines > 0);
    assert!(config.channel_capacity > 0);
    assert!(config.output_batch_size > 0);
    assert!(config.response_backpressure_threshold > 0);
    assert!(config.item_backpressure_threshold > 0);
    assert!(config.retry_release_permit);
}

#[test]
fn crawler_config_builder() {
    let config = CrawlerConfig::new()
        .with_max_concurrent_downloads(20)
        .with_max_pending_requests(24)
        .with_parser_workers(8)
        .with_max_concurrent_pipelines(4)
        .with_channel_capacity(500)
        .with_output_batch_size(32)
        .with_response_backpressure_threshold(64)
        .with_item_backpressure_threshold(48)
        .with_retry_release_permit(false);

    assert_eq!(config.max_concurrent_downloads, 20);
    assert_eq!(config.max_pending_requests, 24);
    assert_eq!(config.parser_workers, 8);
    assert_eq!(config.max_concurrent_pipelines, 4);
    assert_eq!(config.channel_capacity, 500);
    assert_eq!(config.output_batch_size, 32);
    assert_eq!(config.response_backpressure_threshold, 64);
    assert_eq!(config.item_backpressure_threshold, 48);
    assert!(!config.retry_release_permit);
}

#[test]
fn crawler_config_validation() {
    let valid_config = CrawlerConfig::default();
    assert!(valid_config.validate().is_ok());

    let invalid_config = CrawlerConfig::new().with_max_concurrent_downloads(0);
    assert!(invalid_config.validate().is_err());
}

#[test]
fn checkpoint_config_builder() {
    let config = CheckpointConfig::builder()
        .path("./test.checkpoint")
        .interval(Duration::from_secs(30))
        .build();

    assert!(config.path.is_some());
    assert!(config.interval.is_some());
    assert!(config.is_enabled());
}

#[test]
fn checkpoint_config_disabled() {
    let config = CheckpointConfig::new();
    assert!(!config.is_enabled());
}

#[derive(Clone, Debug)]
struct ParserTestItem;

impl ScrapedItem for ParserTestItem {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn box_clone(&self) -> Box<dyn ScrapedItem + Send + Sync> {
        Box::new(self.clone())
    }

    fn to_json_value(&self) -> serde_json::Value {
        serde_json::Value::Null
    }
}

struct BlockingSpider {
    entered: Arc<AtomicUsize>,
    first_started: Arc<Notify>,
    release_first: Arc<Notify>,
}

#[async_trait]
impl Spider for BlockingSpider {
    type Item = ParserTestItem;
    type State = ();

    async fn parse(
        &self,
        _response: Response,
        _state: &Self::State,
    ) -> Result<ParseOutput<Self::Item>, SpiderError> {
        let call_index = self.entered.fetch_add(1, Ordering::AcqRel);
        if call_index == 0 {
            self.first_started.notify_one();
            self.release_first.notified().await;
        }

        Ok(ParseOutput::new())
    }
}

fn response(url: &str) -> Response {
    Response {
        url: Url::parse(url).expect("valid response url"),
        status: 200u16.try_into().expect("valid status"),
        headers: Default::default(),
        body: Default::default(),
        request_url: Url::parse(url).expect("valid request url"),
        meta: None,
        cached: false,
    }
}

#[tokio::test]
async fn queued_responses_keep_parser_state_non_idle() {
    let entered = Arc::new(AtomicUsize::new(0));
    let first_started = Arc::new(Notify::new());
    let release_first = Arc::new(Notify::new());
    let spider = Arc::new(BlockingSpider {
        entered: Arc::clone(&entered),
        first_started: Arc::clone(&first_started),
        release_first: Arc::clone(&release_first),
    });
    let state = CrawlerState::new();
    let stats = Arc::new(StatCollector::new());
    let (scheduler, _req_rx) = Scheduler::new(None, 32);
    let (res_tx, res_rx) = kanal::bounded_async(4);
    let (item_tx, item_rx) = kanal::bounded_async(4);
    drop(item_rx);

    let parser_handle = spawn_parser_task(
        Arc::clone(&scheduler),
        spider,
        Arc::new(()),
        Arc::clone(&state),
        res_rx,
        item_tx,
        1,
        32,
        2,
        stats,
    );

    res_tx
        .send(response("https://example.com/one"))
        .await
        .expect("first response should send");
    first_started.notified().await;

    res_tx
        .send(response("https://example.com/two"))
        .await
        .expect("second response should send");
    tokio::time::sleep(Duration::from_millis(20)).await;

    assert_eq!(state.parsing_responses.load(Ordering::Acquire), 2);
    assert!(!state.is_idle());

    release_first.notify_one();
    drop(res_tx);
    parser_handle
        .await
        .expect("parser should shut down cleanly");
    assert!(state.is_idle());
}

#[derive(Clone, Debug)]
struct RetryTestItem;

impl ScrapedItem for RetryTestItem {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn box_clone(&self) -> Box<dyn ScrapedItem + Send + Sync> {
        Box::new(self.clone())
    }

    fn to_json_value(&self) -> serde_json::Value {
        serde_json::Value::Null
    }
}

struct RetryTestSpider;

#[async_trait]
impl Spider for RetryTestSpider {
    type Item = RetryTestItem;
    type State = ();

    async fn parse(
        &self,
        _response: Response,
        _state: &Self::State,
    ) -> Result<ParseOutput<Self::Item>, SpiderError> {
        Ok(ParseOutput::new())
    }
}

#[derive(Clone)]
struct TestClient;

struct FailingDownloader {
    client: TestClient,
}

#[async_trait]
impl Downloader for FailingDownloader {
    type Client = TestClient;

    async fn download(&self, _request: Request) -> Result<Response, SpiderError> {
        Err(SpiderError::ReqwestError(ReqwestError {
            message: "timeout".to_string(),
            is_connect: false,
            is_timeout: true,
        }))
    }

    fn client(&self) -> &Self::Client {
        &self.client
    }
}

#[tokio::test]
async fn download_timeout_triggers_retry_error_middleware() {
    let (scheduler, request_rx) = Scheduler::new(None, 32);
    let stats = Arc::new(StatCollector::new());
    let middlewares = SharedMiddlewareManager::new(vec![Box::new(
        RetryMiddleware::new().max_retries(2).backoff_factor(0.0),
    )
        as Box<dyn Middleware<TestClient> + Send + Sync>]);
    let downloader: Arc<dyn Downloader<Client = TestClient> + Send + Sync> =
        Arc::new(FailingDownloader { client: TestClient });
    let request = Request::new(Url::parse("https://example.com/retry").expect("valid url"));

    let response = process_request_through_middlewares::<RetryTestSpider, TestClient>(
        request,
        &downloader,
        &middlewares,
        &scheduler,
        true,
        &stats,
    )
    .await
    .expect("middleware flow should not fail");

    assert!(response.is_none());
    let retried_request = request_rx.recv().await.expect("retried request");
    assert_eq!(retried_request.url.as_str(), "https://example.com/retry");
    assert_eq!(retried_request.get_retry_attempts(), 1);
    assert_eq!(stats.requests_retried.load(Ordering::Acquire), 1);
    assert_eq!(
        stats.requests_scheduled_for_retry.load(Ordering::Acquire),
        1
    );
    assert_eq!(stats.requests_failed.load(Ordering::Acquire), 0);
}

#[tokio::test]
async fn scheduler_batch_enqueue_respects_deduplication() {
    let (scheduler, request_rx) = Scheduler::new(None, 8);
    let req_one = Request::new(Url::parse("https://example.com/one").expect("valid url"));
    let req_two = Request::new(Url::parse("https://example.com/two").expect("valid url"));
    let duplicate_one = Request::new(Url::parse("https://example.com/one").expect("valid url"));

    let enqueued = scheduler
        .enqueue_requests_batch(vec![req_one, req_two.clone(), duplicate_one])
        .await
        .expect("batch enqueue should succeed");

    assert_eq!(enqueued, 2);
    let first = request_rx.recv().await.expect("first request dispatched");
    let second = request_rx.recv().await.expect("second request dispatched");
    assert_ne!(first.url, second.url);
}

#[tokio::test]
async fn scheduler_blocks_enqueue_until_outstanding_request_completes() {
    let (scheduler, request_rx) = Scheduler::new(None, 2);

    scheduler
        .enqueue_request(Request::new(
            Url::parse("https://example.com/one").expect("valid url"),
        ))
        .await
        .expect("first request should enqueue");
    scheduler
        .enqueue_request(Request::new(
            Url::parse("https://example.com/two").expect("valid url"),
        ))
        .await
        .expect("second request should enqueue");

    let first = request_rx.recv().await.expect("first dispatched request");
    let second = request_rx.recv().await.expect("second dispatched request");
    assert_eq!(first.url.as_str(), "https://example.com/one");
    assert_eq!(second.url.as_str(), "https://example.com/two");
    assert_eq!(scheduler.len(), 2);

    let scheduler_clone = Arc::clone(&scheduler);
    let third_url = Url::parse("https://example.com/three").expect("valid url");
    let blocked_enqueue = tokio::spawn(async move {
        scheduler_clone
            .enqueue_request(Request::new(third_url))
            .await
            .expect("third request should enqueue after capacity frees");
    });

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!blocked_enqueue.is_finished());

    scheduler.complete_request();
    blocked_enqueue
        .await
        .expect("blocked enqueue task should finish cleanly");

    let third = request_rx.recv().await.expect("third dispatched request");
    assert_eq!(third.url.as_str(), "https://example.com/three");
    assert_eq!(scheduler.len(), 2);

    scheduler.complete_request();
    scheduler.complete_request();
    assert_eq!(scheduler.len(), 0);
}
