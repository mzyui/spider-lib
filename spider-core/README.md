# spider-core

Core crawling engine for `spider-lib`: spider trait, crawler runtime, scheduler, builder, state, and stats.

Most users should start with `spider-lib`. Use `spider-core` directly when you want lower-level control over runtime composition.

## Installation

```toml
[dependencies]
spider-core = "1.0.4"
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

    fn start_urls(&self) -> Vec<&'static str> {
        vec!["https://example.com"]
    }

    async fn parse(
        &self,
        _response: Response,
        _state: &Self::State,
    ) -> Result<ParseOutput<Self::Item>, SpiderError> {
        Ok(ParseOutput::new())
    }
}

```

## Feature Flags

- `core` (default)
- `live-stats`: enables in-place terminal stat updates.
- `checkpoint`: enables checkpoint/resume support.
- `cookie-store`: enables `cookie_store` integration.

```toml
[dependencies]
spider-core = { version = "1.0.4", features = ["checkpoint"] }
```

## Related Crates

- [`spider-lib`](../README.md)
- [`spider-middleware`](../spider-middleware/README.md)
- [`spider-pipeline`](../spider-pipeline/README.md)
- [`spider-util`](../spider-util/README.md)

## License

MIT. See [LICENSE](../LICENSE).
