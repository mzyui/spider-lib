# spider-lib

A modular Rust web scraping framework inspired by Scrapy.

`spider-lib` is the facade crate for this workspace. It re-exports crawler runtime, downloader, middleware, pipelines, utility types, and macros so you can start with one dependency and enable only the features you need.

## Table of Contents

- [Workspace Crates](#workspace-crates)
- [Architecture at a Glance](#architecture-at-a-glance)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Downloader Usage](#downloader-usage)
- [Middleware Usage](#middleware-usage)
- [Pipeline Usage](#pipeline-usage)
- [Feature Flag Cookbook](#feature-flag-cookbook)
- [Development](#development)
- [Documentation](#documentation)
- [License](#license)

## Workspace Crates

- [`spider-core`](./spider-core/README.md): crawler runtime, spider trait, scheduler, builder, state, and stats.
- [`spider-downloader`](./spider-downloader/README.md): downloader traits and reqwest-based downloader implementation.
- [`spider-macro`](./spider-macro/README.md): procedural macros such as `#[scraped_item]`.
- [`spider-middleware`](./spider-middleware/README.md): retry, rate limiting, robots, cookies, proxy, cache, and user-agent middleware.
- [`spider-pipeline`](./spider-pipeline/README.md): item processing and output pipelines (JSON, JSONL, CSV, SQLite, stream JSON).
- [`spider-util`](./spider-util/README.md): shared request/response/item/error types and helper utilities.

## Architecture at a Glance

`Spider` generates initial URLs and parses responses, while the crawler orchestrates request execution and data flow.

```text
Spider::start_urls
  -> Scheduler
  -> Downloader (default: reqwest)
  -> Middleware chain (before/after download)
  -> Spider::parse(Response) -> ParseOutput { requests, items }
  -> Pipeline chain (transform/validate/dedup/export)
```

## Installation

```toml
[dependencies]
spider-lib = "2.0.4"
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"
```

`serde` and `serde_json` are required when using `#[scraped_item]`.

## Quick Start

```rust,no_run
use spider_lib::prelude::*;

#[scraped_item]
struct QuoteItem {
    text: String,
    author: String,
}

#[derive(Clone, Default)]
struct QuoteState;

struct QuoteSpider;

#[async_trait]
impl Spider for QuoteSpider {
    type Item = QuoteItem;
    type State = QuoteState;

    fn start_urls(&self) -> Vec<&'static str> {
        vec!["https://quotes.toscrape.com/"]
    }

    async fn parse(
        &self,
        _response: Response,
        _state: &Self::State,
    ) -> Result<ParseOutput<Self::Item>, SpiderError> {
        Ok(ParseOutput::new())
    }
}

#[tokio::main]
async fn main() -> Result<(), SpiderError> {
    let crawler = CrawlerBuilder::new(QuoteSpider)
        .add_middleware(RateLimitMiddleware::default())
        .add_middleware(RetryMiddleware::new())
        .add_pipeline(ConsolePipeline::new())
        .build()
        .await?;

    crawler.start_crawl().await
}
```

Try maintained examples:

```bash
cargo run --example books
cargo run --example books_live --features live-stats
```

## Downloader Usage

### Default downloader

`CrawlerBuilder` uses a reqwest-based downloader by default, so no extra setup is needed for most projects.

### Build a Custom Downloader

Use a custom downloader when you need custom transport behavior (special auth, alternate HTTP stack, tracing, etc.).

```rust,ignore
use async_trait::async_trait;
use spider_lib::prelude::*;

struct SignedDownloader {
    client: reqwest::Client,
}

#[async_trait]
impl Downloader for SignedDownloader {
    type Client = reqwest::Client;

    async fn download(&self, request: Request) -> Result<Response, SpiderError> {
        let _req = request;
        // Sign request, execute HTTP call, map response.
        todo!()
    }

    fn client(&self) -> &Self::Client {
        &self.client
    }
}
```

Runtime integration:

```rust,ignore
let crawler = CrawlerBuilder::new(MySpider)
    .downloader(SignedDownloader {
        client: reqwest::Client::new(),
    })
    .build()
    .await?;
```

For trait details and lower-level integration, see [`spider-downloader`](./spider-downloader/README.md).

## Middleware Usage

Core middleware (always available):

- `RateLimitMiddleware`: controls request throughput.
- `RetryMiddleware`: retries failed/transient requests.
- `RefererMiddleware`: populates `Referer` header for follow-up requests.

Optional middleware (feature-gated):

- `HttpCacheMiddleware` (`middleware-cache`)
- `AutoThrottleMiddleware` (`middleware-autothrottle`)
- `ProxyMiddleware` (`middleware-proxy`)
- `UserAgentMiddleware` (`middleware-user-agent`)
- `RobotsTxtMiddleware` (`middleware-robots`)
- `CookieMiddleware` (`middleware-cookies`)

```rust,ignore
let crawler = CrawlerBuilder::new(MySpider)
    .add_middleware(RateLimitMiddleware::default())
    .add_middleware(RetryMiddleware::new())
    .add_middleware(RefererMiddleware::new())
    .build()
    .await?;
```

### Build a Custom Middleware

```rust,ignore
use spider_lib::prelude::*;

struct HeaderMiddleware;

#[async_trait]
impl Middleware<reqwest::Client> for HeaderMiddleware {
    fn name(&self) -> &str {
        "header_middleware"
    }

    async fn process_request(
        &mut self,
        _client: &reqwest::Client,
        request: Request,
    ) -> Result<MiddlewareAction<Request>, SpiderError> {
        Ok(MiddlewareAction::Continue(request))
    }
}
```

Runtime integration:

```rust,ignore
let crawler = CrawlerBuilder::new(MySpider)
    .add_middleware(HeaderMiddleware)
    .build()
    .await?;
```

See full per-feature middleware examples in [`spider-middleware`](./spider-middleware/README.md).

## Pipeline Usage

Core pipelines (always available):

- `TransformPipeline`
- `ValidationPipeline`
- `DeduplicationPipeline`
- `ConsolePipeline`

Optional output pipelines (feature-gated):

- `JsonPipeline` (`pipeline-json`)
- `JsonlPipeline` (`pipeline-jsonl`)
- `CsvPipeline` (`pipeline-csv`)
- `SqlitePipeline` (`pipeline-sqlite`)
- `StreamJsonPipeline` (`pipeline-stream-json`)

```rust,ignore
let crawler = CrawlerBuilder::new(MySpider)
    .add_pipeline(
        TransformPipeline::new()
            .with_operation(TransformOperation::Trim { field: "title".into() }),
    )
    .add_pipeline(
        ValidationPipeline::new()
            .with_rule("title", ValidationRule::Required)
            .with_rule("title", ValidationRule::NonEmptyString),
    )
    .add_pipeline(DeduplicationPipeline::new(&["url"]))
    .add_pipeline(ConsolePipeline::new())
    .build()
    .await?;
```

### Build a Custom Pipeline

```rust,ignore
use spider_lib::prelude::*;

struct MetricsPipeline;

#[async_trait]
impl Pipeline<MyItem> for MetricsPipeline {
    fn name(&self) -> &str {
        "metrics_pipeline"
    }

    async fn process_item(&self, item: MyItem) -> Result<Option<MyItem>, PipelineError> {
        // Record metrics, enrich, or filter items.
        Ok(Some(item))
    }
}
```

Runtime integration:

```rust,ignore
let crawler = CrawlerBuilder::new(MySpider)
    .add_pipeline(MetricsPipeline)
    .build()
    .await?;
```

See full per-feature pipeline examples in [`spider-pipeline`](./spider-pipeline/README.md).

## Feature Flag Cookbook

### Minimal core crawler

```toml
[dependencies]
spider-lib = "2.0.4"
```

### Robots + JSONL export

```toml
[dependencies]
spider-lib = { version = "2.0.4", features = ["middleware-robots", "pipeline-jsonl"] }
```

### Proxy + user-agent rotation + CSV output

```toml
[dependencies]
spider-lib = { version = "2.0.4", features = ["middleware-proxy", "middleware-user-agent", "pipeline-csv"] }
```

### Cache + autothrottle + SQLite output

```toml
[dependencies]
spider-lib = { version = "2.0.4", features = ["middleware-cache", "middleware-autothrottle", "pipeline-sqlite"] }
```

### Live stats and resume support

```toml
[dependencies]
spider-lib = { version = "2.0.4", features = ["live-stats", "checkpoint"] }
```

### Cookie-aware crawling

```toml
[dependencies]
spider-lib = { version = "2.0.4", features = ["cookie-store"] }
```

`cookie-store` enables `middleware-cookies` transitively.

## Development

```bash
cargo check --workspace --all-targets
cargo fmt --check
cargo clippy --all-features -- -D warnings
cargo test --all-features
make check-all-features
```

## Documentation

- API docs: <https://docs.rs/spider-lib>
- Contribution guide: [CONTRIBUTING.md](./CONTRIBUTING.md)

## License

MIT. See [LICENSE](./LICENSE).
