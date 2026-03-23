use spider_lib::prelude::*;
use std::time::Duration;

#[cfg(feature = "middleware-proxy")]
use spider_middleware::proxy::{ProxyRotationStrategy, ProxySource};
#[cfg(feature = "middleware-user-agent")]
use spider_middleware::user_agent::{
    BuiltinUserAgentList, UserAgentRotationStrategy, UserAgentSource,
};

#[scraped_item]
struct ShowcaseItem {
    title: String,
    url: String,
    status: u16,
    body_bytes: usize,
    cached: bool,
    pages_seen: usize,
    total_bytes_seen: u64,
    first_visit: bool,
    note: Option<String>,
}

#[derive(Clone, Default)]
struct ShowcaseState {
    pages_seen: Counter,
    total_bytes_seen: Counter64,
    saw_cached_response: Flag,
    visited_urls: VisitedUrls,
    status_counts: ConcurrentMap<String, usize>,
    titles_seen: ConcurrentVec<String>,
    access_metrics: StateAccessMetrics,
}

impl ShowcaseState {
    fn record_response(&self, response: &Response, title: &str) {
        self.access_metrics.record_access_start();
        self.access_metrics.record_read();
        self.access_metrics.record_write();

        self.pages_seen.inc();
        self.total_bytes_seen.add(response.body.len() as u64);

        if response.cached {
            self.saw_cached_response.set(true);
        }

        let url = response.url.to_string();
        let was_visited = self.visited_urls.is_visited(&url);
        if !was_visited {
            self.visited_urls.mark(url);
        }

        let status_key = response.status.as_u16().to_string();
        let next_count = self.status_counts.get(&status_key).unwrap_or(0) + 1;
        self.status_counts.insert(status_key, next_count);

        if !title.is_empty() {
            self.titles_seen.push(title.to_string());
        }

        self.access_metrics.record_access_end();
    }

    fn summary(&self) -> String {
        format!(
            "pages={} bytes={} visited={} titles={} cached={} reads={} writes={} peak={}",
            self.pages_seen.get(),
            self.total_bytes_seen.get(),
            self.visited_urls.len(),
            self.titles_seen.len(),
            self.saw_cached_response.get(),
            self.access_metrics.read_count(),
            self.access_metrics.write_count(),
            self.access_metrics.concurrent_access_peak()
        )
    }
}

struct ShowcaseSpider;

#[async_trait]
impl Spider for ShowcaseSpider {
    type Item = ShowcaseItem;
    type State = ShowcaseState;

    fn start_requests(&self) -> Result<StartRequests<'_>, SpiderError> {
        Ok(StartRequests::iter(
            vec![Ok(Request::new("https://example.com/".parse()?))].into_iter(),
        ))
    }

    async fn parse(
        &self,
        response: Response,
        state: &Self::State,
    ) -> Result<ParseOutput<Self::Item>, SpiderError> {
        let html = response.to_html()?;
        let mut output = ParseOutput::new();

        let title = html
            .select(&"h1".to_selector()?)
            .next()
            .map(|node| node.text().collect::<String>())
            .unwrap_or_else(|| "Example Domain".to_string())
            .trim()
            .to_string();

        let url = response.url.to_string();
        let first_visit = !state.visited_urls.is_visited(&url);

        state.record_response(&response, &title);

        output.add_item(ShowcaseItem {
            title,
            url,
            status: response.status.as_u16(),
            body_bytes: response.body.len(),
            cached: response.cached,
            pages_seen: state.pages_seen.get(),
            total_bytes_seen: state.total_bytes_seen.get(),
            first_visit,
            note: Some(state.summary()),
        });

        Ok(output)
    }
}

#[cfg(feature = "middleware-proxy")]
fn build_proxy_middleware() -> Result<ProxyMiddleware, SpiderError> {
    ProxyMiddleware::builder()
        .source(ProxySource::List(vec![
            "http://127.0.0.1:8080".to_string(),
            "http://127.0.0.1:8081".to_string(),
        ]))
        .strategy(ProxyRotationStrategy::Sequential)
        .build()
}

#[cfg(feature = "middleware-cache")]
fn build_cache_middleware() -> Result<HttpCacheMiddleware, SpiderError> {
    HttpCacheMiddleware::builder()
        .cache_dir("output/http-cache".into())
        .build()
}

#[cfg(feature = "middleware-user-agent")]
fn build_user_agent_middleware() -> Result<UserAgentMiddleware, SpiderError> {
    UserAgentMiddleware::builder()
        .source(UserAgentSource::Builtin(BuiltinUserAgentList::Random))
        .strategy(UserAgentRotationStrategy::Random)
        .fallback_user_agent("spider-lib-showcase/1.0".to_string())
        .build()
}

#[cfg(feature = "pipeline-json")]
fn build_json_pipeline() -> Result<JsonPipeline<ShowcaseItem>, PipelineError> {
    JsonPipeline::new("output/showcase.json")
}

#[cfg(feature = "pipeline-jsonl")]
fn build_jsonl_pipeline() -> Result<JsonlPipeline<ShowcaseItem>, PipelineError> {
    JsonlPipeline::new("output/showcase.jsonl")
}

#[cfg(feature = "pipeline-csv")]
fn build_csv_pipeline() -> Result<CsvPipeline<ShowcaseItem>, PipelineError> {
    CsvPipeline::new("output/showcase.csv")
}

#[cfg(feature = "pipeline-sqlite")]
fn build_sqlite_pipeline() -> Result<SqlitePipeline<ShowcaseItem>, PipelineError> {
    SqlitePipeline::new("output/showcase.sqlite", "showcase_items")
}

#[cfg(feature = "pipeline-stream-json")]
fn build_stream_json_pipeline() -> Result<StreamJsonPipeline<ShowcaseItem>, PipelineError> {
    StreamJsonPipeline::new("output/showcase-stream.json")
}

#[tokio::main]
async fn main() -> Result<(), SpiderError> {
    create_dir("output")?;

    let builder = CrawlerBuilder::new(ShowcaseSpider)
        .limit(1)
        .max_concurrent_downloads(2)
        .max_parser_workers(2)
        .max_concurrent_pipelines(2)
        .channel_capacity(32)
        .output_batch_size(8)
        .response_backpressure_threshold(16)
        .item_backpressure_threshold(16)
        .retry_release_permit(true)
        .shutdown_grace_period(Duration::from_secs(2))
        .log_level(log::LevelFilter::Info)
        .add_middleware(RefererMiddleware::new().same_origin_only(false))
        .add_middleware(
            RateLimitMiddleware::builder()
                .use_token_bucket_limiter(2)
                .build(),
        )
        .add_middleware(
            RetryMiddleware::new()
                .max_retries(1)
                .backoff_factor(0.25)
                .max_delay(Duration::from_secs(2)),
        )
        .add_pipeline(
            TransformPipeline::new()
                .with_operation(TransformOperation::Trim {
                    field: "title".into(),
                })
                .with_operation(TransformOperation::SetDefault {
                    field: "note".into(),
                    value: serde_json::json!("generated by all_features example"),
                }),
        )
        .add_pipeline(
            ValidationPipeline::new()
                .with_rule("title", ValidationRule::Required)
                .with_rule("title", ValidationRule::NonEmptyString)
                .with_rule("status", ValidationRule::Type(JsonType::Number))
                .with_rule("body_bytes", ValidationRule::MinNumber(1.0)),
        )
        .add_pipeline(DeduplicationPipeline::new(&["url"]))
        .add_pipeline(ConsolePipeline::new());

    #[cfg(feature = "live-stats")]
    let builder = builder
        .live_stats(true)
        .live_stats_interval(Duration::from_millis(250))
        .live_stats_preview_fields(["title", "url", "status"]);

    #[cfg(not(feature = "live-stats"))]
    let builder = builder;

    #[cfg(feature = "middleware-autothrottle")]
    let builder = builder.add_middleware(
        AutoThrottleMiddleware::builder()
            .min_delay(Duration::from_millis(50))
            .max_delay(Duration::from_secs(2))
            .target_concurrency(1.0)
            .build(),
    );

    #[cfg(not(feature = "middleware-autothrottle"))]
    let builder = builder;

    #[cfg(feature = "middleware-cache")]
    let builder = builder.add_middleware(build_cache_middleware()?);

    #[cfg(not(feature = "middleware-cache"))]
    let builder = builder;

    #[cfg(feature = "middleware-proxy")]
    let builder = builder.add_middleware(build_proxy_middleware()?);

    #[cfg(not(feature = "middleware-proxy"))]
    let builder = builder;

    #[cfg(feature = "middleware-user-agent")]
    let builder = builder.add_middleware(build_user_agent_middleware()?);

    #[cfg(not(feature = "middleware-user-agent"))]
    let builder = builder;

    #[cfg(feature = "middleware-robots")]
    let builder =
        builder.add_middleware(RobotsTxtMiddleware::new().request_timeout(Duration::from_secs(2)));

    #[cfg(not(feature = "middleware-robots"))]
    let builder = builder;

    #[cfg(feature = "middleware-cookies")]
    let builder = builder.add_middleware(CookieMiddleware::new());

    #[cfg(not(feature = "middleware-cookies"))]
    let builder = builder;

    #[cfg(feature = "pipeline-json")]
    let builder = builder.add_pipeline(build_json_pipeline()?);

    #[cfg(not(feature = "pipeline-json"))]
    let builder = builder;

    #[cfg(feature = "pipeline-jsonl")]
    let builder = builder.add_pipeline(build_jsonl_pipeline()?);

    #[cfg(not(feature = "pipeline-jsonl"))]
    let builder = builder;

    #[cfg(feature = "pipeline-csv")]
    let builder = builder.add_pipeline(build_csv_pipeline()?);

    #[cfg(not(feature = "pipeline-csv"))]
    let builder = builder;

    #[cfg(feature = "pipeline-sqlite")]
    let builder = builder.add_pipeline(build_sqlite_pipeline()?);

    #[cfg(not(feature = "pipeline-sqlite"))]
    let builder = builder;

    #[cfg(feature = "pipeline-stream-json")]
    let builder = builder.add_pipeline(build_stream_json_pipeline()?);

    #[cfg(not(feature = "pipeline-stream-json"))]
    let builder = builder;

    #[cfg(feature = "checkpoint")]
    let builder = builder
        .with_checkpoint_path("output/showcase.checkpoint")
        .with_checkpoint_interval(Duration::from_secs(30));

    #[cfg(not(feature = "checkpoint"))]
    let builder = builder;

    #[cfg(feature = "cookie-store")]
    let builder = builder;

    #[cfg(not(feature = "cookie-store"))]
    let builder = builder;

    let crawler = builder.build().await?;
    let state = crawler.state_arc();
    crawler.start_crawl().await?;

    println!("showcase summary: {}", state.summary());

    Ok(())
}
