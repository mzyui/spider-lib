# spider-core

`spider-core` is the runtime heart of the workspace. It owns the crawling loop, the `Spider` trait, the builder used to compose a crawler, the scheduler, shared state, and runtime stats.

Most applications should still start with [`spider-lib`](../README.md), because the facade crate re-exports the common pieces. `spider-core` is the crate to reach for when you want tighter control over runtime composition or when you are building extensions against the lower-level API.

## When it makes sense to depend on this crate

Use `spider-core` directly if you are:

- building on the runtime without the root facade crate
- integrating a custom downloader, middleware stack, or pipeline stack
- publishing reusable extensions that should depend on the runtime contracts rather than the application-facing facade

If your goal is simply “write a spider and run it”, `spider-lib` is usually more convenient.

## Installation

```toml
[dependencies]
spider-core = "2.0.2"
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"
```

You only need `serde` and `serde_json` when you use `#[scraped_item]`.

## What lives here

The main exports are:

- `Spider` for crawl logic
- `Crawler` for the runtime handle
- `CrawlerBuilder` for composition and configuration
- `Scheduler` for request admission and deduplication
- shared state primitives such as counters and concurrent maps
- `StatCollector` for runtime statistics

The runtime loop is intentionally simple:

1. `Spider::start_requests` seeds the crawl.
2. Requests go through scheduling and deduplication.
3. The downloader fetches responses.
4. Middleware can alter requests, responses, or retry behavior.
5. `Spider::parse` returns a `ParseOutput` containing items and follow-up requests.
6. Pipelines process emitted items.

## API landmarks

If you are skimming docs.rs, these are the most useful entry points:

- `Spider`: define crawl behavior
- `StartRequests`: describe how the crawl is seeded
- `CrawlerBuilder`: tune concurrency and attach middleware/pipelines
- `Crawler`: start and monitor the running crawl
- `StatCollector`: inspect runtime stats
- `state::*`: thread-safe primitives for shared parse-time state

## Minimal example

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

`limit(1)` is handy for previews and smoke runs because it stops after the first admitted item.

## Where decisions usually belong

- Use `Spider::start_urls` for simple static seeds.
- Use `Spider::start_requests` when seeds need full `Request` values, metadata, or file-backed loading.
- Use middleware for HTTP lifecycle policy.
- Use pipelines for item lifecycle policy.
- Use a custom downloader when transport execution itself must change.

## Feature flags

| Feature | Purpose |
| --- | --- |
| `core` | Base runtime support. Enabled by default. |
| `live-stats` | In-place terminal statistics display. |
| `checkpoint` | Checkpoint and resume support. |
| `cookie-store` | `cookie_store` integration in core state. |

```toml
[dependencies]
spider-core = { version = "2.0.2", features = ["checkpoint"] }
```

## Practical note

If you want a full working example instead of a runtime skeleton, the repository-level [`books` example](../README.md#run-the-examples) is the best reference point. It uses the facade crate, but the runtime flow is the same one `spider-core` drives.

## Related crates

- [`spider-lib`](../README.md)
- [`spider-downloader`](../spider-downloader/README.md)
- [`spider-middleware`](../spider-middleware/README.md)
- [`spider-pipeline`](../spider-pipeline/README.md)
- [`spider-util`](../spider-util/README.md)

## License

MIT. See [LICENSE](../LICENSE).
