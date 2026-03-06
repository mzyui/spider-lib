# spider-core

Core engine for `spider-lib`: spider trait, crawler runtime, scheduler, builder, and shared crawl state.

## Install

```toml
[dependencies]
spider-core = "1.0.3"
```

Usually you will use this through `spider-lib`, but this crate is useful if you want lower-level control.

## Main Components

- `Spider`: trait for crawl logic.
- `Crawler`: runtime engine.
- `CrawlerBuilder`: crawler configuration and composition.
- `Scheduler`: request queueing and dedup behavior.
- `StatCollector`: runtime statistics.

## Minimal Usage

```rust,no_run
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
- `live-stats`: enables in-place live stats terminal updates.
- `checkpoint`: enables checkpoint/resume support.
- `cookie-store`: enables `cookie_store` integration.

```toml
[dependencies]
spider-core = { version = "1.0.4", features = ["checkpoint"] }
```

## Related Crates

- [`spider-downloader`](../spider-downloader/README.md)
- [`spider-middleware`](../spider-middleware/README.md)
- [`spider-pipeline`](../spider-pipeline/README.md)
- [`spider-util`](../spider-util/README.md)

## License

MIT. See [LICENSE](./LICENSE).
