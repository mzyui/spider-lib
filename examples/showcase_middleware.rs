#[path = "showcase/support.rs"]
mod showcase;

use showcase::{ShowcaseSpider, prepare_output_dir};
use spider_lib::prelude::*;
use spider_middleware::proxy::{ProxyRotationStrategy, ProxySource};
use spider_middleware::user_agent::{
    BuiltinUserAgentList, UserAgentRotationStrategy, UserAgentSource,
};
use std::time::Duration;

fn build_proxy_middleware() -> Result<ProxyMiddleware, SpiderError> {
    ProxyMiddleware::builder()
        .source(ProxySource::List(vec![
            "http://127.0.0.1:8080".to_string(),
            "http://127.0.0.1:8081".to_string(),
        ]))
        .strategy(ProxyRotationStrategy::Sequential)
        .build()
}

fn build_cache_middleware() -> Result<HttpCacheMiddleware, SpiderError> {
    HttpCacheMiddleware::builder()
        .cache_dir("output/http-cache")
        .build()
}

fn build_user_agent_middleware() -> Result<UserAgentMiddleware, SpiderError> {
    UserAgentMiddleware::builder()
        .source(UserAgentSource::Builtin(BuiltinUserAgentList::Random))
        .strategy(UserAgentRotationStrategy::Random)
        .fallback_user_agent("spider-lib-showcase/1.0".to_string())
        .build()
}

#[tokio::main]
async fn main() -> Result<(), SpiderError> {
    prepare_output_dir()?;

    let crawler = CrawlerBuilder::new(ShowcaseSpider)
        .limit(1)
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
        .add_middleware(
            AutoThrottleMiddleware::builder()
                .min_delay(Duration::from_millis(50))
                .max_delay(Duration::from_secs(2))
                .target_concurrency(1.0)
                .build(),
        )
        .add_middleware(build_cache_middleware()?)
        .add_middleware(build_proxy_middleware()?)
        .add_middleware(build_user_agent_middleware()?)
        .add_middleware(RobotsTxtMiddleware::new().request_timeout(Duration::from_secs(2)))
        .add_middleware(CookieMiddleware::new())
        .build()
        .await?;

    let state = crawler.state_arc();
    crawler.start_crawl().await?;

    println!("showcase middleware summary: {}", state.summary());

    Ok(())
}
