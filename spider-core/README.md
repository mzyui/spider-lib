# spider-core

Core crawling engine for `spider-lib`: the spider trait, crawler runtime, scheduler, builder, state, and stats.

Most users should start with [`spider-lib`](../README.md). Use `spider-core` directly when you want lower-level control over runtime composition while staying inside the same ecosystem.

## When to Use This Crate Directly

Use `spider-core` if you need one or more of these:

- You want to build against the crawler runtime without the facade crate.
- You are integrating a custom downloader, middleware stack, or pipeline stack.
- You are publishing lower-level extensions that should depend on the core runtime API.

If you just want to build a spider quickly, prefer `spider-lib`.

## Installation

```toml
[dependencies]
spider-core = "2.0.1"
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"
```

`serde` and `serde_json` are still required if you use `#[scraped_item]`.

## Core Runtime Lifecycle

At a high level, `spider-core` drives this loop:

1. `Spider::start_requests` seeds the crawl.
2. The scheduler admits and de-duplicates requests.
3. The downloader executes HTTP requests.
4. Middleware can inspect or modify requests and responses.
5. `Spider::parse` turns a `Response` into `ParseOutput`.
6. Pipelines process emitted items.

## Main Components

- `Spider`: trait for crawl logic.
- `Crawler`: runtime engine that drives requests and parsing.
- `CrawlerBuilder`: runtime configuration and composition.
- `Scheduler`: request queueing and dedup behavior.
- `CrawlerState`: shared runtime state.
- `StatCollector`: runtime statistics.

## Minimal Usage

```rust,ignore
use spider_core::{async_trait, CrawlerBuilder, Spider};
use spider_util::{error::SpiderError, item::ParseOutput, response::Response};

#[spider_macro::scraped_item]
struct Item {
    title: String,
}

#[derive(Clone, Default)]
struct State;

struct MySpider;

#[async_trait]
impl Spider for MySpider {
    type Item = Item;
    type State = State;

    fn start_requests(&self) -> Result<spider_core::StartRequests<'_>, SpiderError> {
        Ok(spider_core::StartRequests::Urls(vec!["https://example.com"]))
    }

    async fn parse(
        &self,
        _response: Response,
        _state: &Self::State,
    ) -> Result<ParseOutput<Self::Item>, SpiderError> {
        Ok(ParseOutput::new())
    }
}

async fn run() -> Result<(), SpiderError> {
    let crawler = CrawlerBuilder::new(MySpider)
        .limit(1)
        .build()
        .await?;

    crawler.start_crawl().await
}
```

`CrawlerBuilder::limit(n)` stops the crawl after `n` scraped items have been admitted for processing, which is useful for previews and smoke runs.

## Feature Flags

| Feature | Purpose |
| --- | --- |
| `core` | Base crawler runtime. Enabled by default. |
| `live-stats` | In-place terminal statistics updates. |
| `checkpoint` | Checkpoint and resume support. |
| `cookie-store` | `cookie_store` integration for runtime state. |

```toml
[dependencies]
spider-core = { version = "2.0.1", features = ["checkpoint"] }
```

## Related Extension Crates

Use these when extending the runtime:

- Custom downloader guide: [`spider-downloader`](../spider-downloader/README.md)
- Custom middleware guide: [`spider-middleware`](../spider-middleware/README.md)
- Custom pipeline guide: [`spider-pipeline`](../spider-pipeline/README.md)

## Related Crates

- [`spider-lib`](../README.md)
- [`spider-downloader`](../spider-downloader/README.md)
- [`spider-middleware`](../spider-middleware/README.md)
- [`spider-pipeline`](../spider-pipeline/README.md)
- [`spider-util`](../spider-util/README.md)

## License

MIT. See [LICENSE](../LICENSE).
