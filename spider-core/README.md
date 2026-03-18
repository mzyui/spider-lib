# spider-core

Core crawling engine for `spider-lib`: spider trait, crawler runtime, scheduler, builder, state, and stats.

Most users should start with `spider-lib`. Use `spider-core` directly when you want lower-level control over runtime composition.

## Installation

```toml
[dependencies]
spider-core = "2.0.0"
```

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

    fn start_requests(&self) -> Result<spider_core::spider::StartRequests<'_>, SpiderError> {
        let req = spider_util::request::Request::new("https://example.com".parse()?);
        Ok(spider_core::spider::StartRequests::Iter(Box::new(std::iter::once(Ok(req)))))
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

- `core` (default)
- `live-stats`: enables in-place terminal stat updates.
- `checkpoint`: enables checkpoint/resume support.
- `cookie-store`: enables `cookie_store` integration.

```toml
[dependencies]
spider-core = { version = "2.0.0", features = ["checkpoint"] }
```

## Custom Extension Guides

For extension points built around crawler composition, see:

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
