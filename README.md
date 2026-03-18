# spider-lib

[![crates.io](https://img.shields.io/crates/v/spider-lib.svg)](https://crates.io/crates/spider-lib)
[![docs.rs](https://docs.rs/spider-lib/badge.svg)](https://docs.rs/spider-lib)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](./LICENSE)

A modular Rust web scraping framework inspired by Scrapy.

`spider-lib` is the facade crate for this workspace. It re-exports the crawler runtime, downloader, middleware, pipelines, utility types, and macros so most users can get started with a single dependency and turn on extra features only when needed.

## Table of Contents

- [Why `spider-lib`](#why-spider-lib)
- [Workspace Crates](#workspace-crates)
- [Architecture at a Glance](#architecture-at-a-glance)
- [When to Use This Crate](#when-to-use-this-crate)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Feature Flag Cookbook](#feature-flag-cookbook)
- [Middleware Overview](#middleware-overview)
- [Pipeline Overview](#pipeline-overview)
- [Examples](#examples)
- [Development](#development)
- [Documentation](#documentation)
- [License](#license)

## Why `spider-lib`

Use `spider-lib` when you want the full framework surface:

- A `Spider` trait for crawl logic.
- A `CrawlerBuilder` for runtime composition.
- Built-in reqwest downloader support.
- Optional middleware for retries, throttling, robots.txt, cookies, proxies, cache, and user agents.
- Optional pipelines for validation, deduplication, console output, and file/database export.

If you only need one subsystem, the lower-level crates remain available and are documented individually below.

## Workspace Crates

- [`spider-core`](./spider-core/README.md): crawler runtime, spider trait, scheduler, builder, state, and stats.
- [`spider-downloader`](./spider-downloader/README.md): downloader traits plus the default reqwest-based downloader.
- [`spider-macro`](./spider-macro/README.md): procedural macros such as `#[scraped_item]`.
- [`spider-middleware`](./spider-middleware/README.md): retry, rate limiting, robots, cookies, proxy, cache, and user-agent middleware.
- [`spider-pipeline`](./spider-pipeline/README.md): item processing and export pipelines for JSON, JSONL, CSV, SQLite, and stream JSON.
- [`spider-util`](./spider-util/README.md): shared request/response/item/error types and helper utilities.

## Architecture at a Glance

`Spider` produces initial requests and parses responses, while the crawler coordinates request execution and item processing.

```text
Spider::start_requests
  -> Scheduler
  -> Downloader (default: ReqwestClientDownloader)
  -> Middleware chain
  -> Spider::parse(Response) -> ParseOutput { requests, items }
  -> Pipeline chain
```

## When to Use This Crate

Prefer `spider-lib` if you want:

- One dependency for the full framework.
- Prelude imports for the common runtime types.
- Built-in integration between core, middleware, pipelines, and macros.
- Feature-flag control over optional capabilities.

Use lower-level crates directly only when you are intentionally composing your own runtime or publishing an extension against one subsystem.

## Installation

```toml
[dependencies]
spider-lib = "3.0.1"
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"
```

`serde` and `serde_json` must be direct dependencies when using `#[scraped_item]`.

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

    fn start_requests(&self) -> Result<StartRequests<'_>, SpiderError> {
        Ok(StartRequests::Urls(vec!["https://quotes.toscrape.com/"]))
    }

    async fn parse(
        &self,
        response: Response,
        _state: &Self::State,
    ) -> Result<ParseOutput<Self::Item>, SpiderError> {
        let html = response.to_html()?;
        let mut output = ParseOutput::new();

        for quote in html.select(&".quote".to_selector()?) {
            let text = quote
                .select(&".text".to_selector()?)
                .next()
                .map(|node| node.text().collect::<String>())
                .unwrap_or_default();

            let author = quote
                .select(&".author".to_selector()?)
                .next()
                .map(|node| node.text().collect::<String>())
                .unwrap_or_default();

            output.add_item(QuoteItem { text, author });
        }

        Ok(output)
    }
}

#[tokio::main]
async fn main() -> Result<(), SpiderError> {
    let crawler = CrawlerBuilder::new(QuoteSpider)
        .limit(10)
        .add_middleware(RateLimitMiddleware::default())
        .add_middleware(RetryMiddleware::new())
        .add_pipeline(ConsolePipeline::new())
        .build()
        .await?;

    crawler.start_crawl().await
}
```

This example shows the common flow:

- `start_requests` seeds the crawl.
- `parse` converts a `Response` into `ParseOutput`.
- middleware runs around request execution.
- pipelines handle items after parsing.

## Feature Flag Cookbook

### Minimal crawler

```toml
[dependencies]
spider-lib = "3.0.1"
```

### Robots.txt + JSONL export

```toml
[dependencies]
spider-lib = { version = "3.0.1", features = ["middleware-robots", "pipeline-jsonl"] }
```

### Proxy + user-agent rotation + CSV export

```toml
[dependencies]
spider-lib = { version = "3.0.1", features = ["middleware-proxy", "middleware-user-agent", "pipeline-csv"] }
```

### Cache + autothrottle + SQLite export

```toml
[dependencies]
spider-lib = { version = "3.0.1", features = ["middleware-cache", "middleware-autothrottle", "pipeline-sqlite"] }
```

### Live stats + checkpoint support

```toml
[dependencies]
spider-lib = { version = "3.0.1", features = ["live-stats", "checkpoint"] }
```

### Cookie-aware crawling

```toml
[dependencies]
spider-lib = { version = "3.0.1", features = ["cookie-store"] }
```

`cookie-store` enables cookie store support in `spider-core` and pulls in cookie middleware transitively.

## Middleware Overview

Core middleware:

- `RateLimitMiddleware`
- `RetryMiddleware`
- `RefererMiddleware`

Optional middleware:

- `HttpCacheMiddleware` via `middleware-cache`
- `AutoThrottleMiddleware` via `middleware-autothrottle`
- `ProxyMiddleware` via `middleware-proxy`
- `UserAgentMiddleware` via `middleware-user-agent`
- `RobotsTxtMiddleware` via `middleware-robots`
- `CookieMiddleware` via `middleware-cookies`

See the per-feature examples in [`spider-middleware`](./spider-middleware/README.md).

## Pipeline Overview

Core pipelines:

- `TransformPipeline`
- `ValidationPipeline`
- `DeduplicationPipeline`
- `ConsolePipeline`

Optional output pipelines:

- `JsonPipeline` via `pipeline-json`
- `JsonlPipeline` via `pipeline-jsonl`
- `CsvPipeline` via `pipeline-csv`
- `SqlitePipeline` via `pipeline-sqlite`
- `StreamJsonPipeline` via `pipeline-stream-json`

See exporter examples and pipeline composition notes in [`spider-pipeline`](./spider-pipeline/README.md).

## Examples

Runnable examples in this repository:

```bash
cargo run --example books
cargo run --example books_live --features live-stats,pipeline-csv
cargo run --example kusonime --features live-stats,pipeline-stream-json
```

## Development

```bash
cargo check --workspace --all-targets
cargo fmt --all
cargo clippy --workspace --all-features -- -D warnings
make check-all-features
```

## Documentation

- API docs: <https://docs.rs/spider-lib>
- Contribution guide: [CONTRIBUTING.md](./CONTRIBUTING.md)

## License

MIT. See [LICENSE](./LICENSE).
