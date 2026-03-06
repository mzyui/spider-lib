# spider-lib

A Rust web scraping framework inspired by Scrapy, built as a modular workspace.

`spider-lib` is the facade crate that re-exports the core engine, downloader, middleware, pipelines, utilities, and macros so you can start with one dependency and enable only the features you need.

## Workspace Crates

- [`spider-core`](./spider-core/README.md): crawler engine, scheduler, spider trait, builder, state, stats.
- [`spider-downloader`](./spider-downloader/README.md): downloader traits and reqwest-based downloader.
- [`spider-macro`](./spider-macro/README.md): procedural macros like `#[scraped_item]`.
- [`spider-middleware`](./spider-middleware/README.md): retry, rate limit, robots, cookies, proxy, cache, user-agent.
- [`spider-pipeline`](./spider-pipeline/README.md): output and post-processing pipelines.
- [`spider-util`](./spider-util/README.md): shared request/response/error/item/types and helpers.

## Install

```toml
[dependencies]
spider-lib = "2.0.3"
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
    let crawler = CrawlerBuilder::new(QuoteSpider).build().await?;
    crawler.start_crawl().await
}
```

Run the included example:

```bash
cargo run --example books
```

## Feature Flags

Default feature: `core`.

Middleware features:

- `middleware-cache`
- `middleware-autothrottle`
- `middleware-proxy`
- `middleware-user-agent`
- `middleware-robots`
- `middleware-cookies`

Pipeline features:

- `pipeline-csv`
- `pipeline-json`
- `pipeline-jsonl`
- `pipeline-sqlite`
- `pipeline-stream-json`

Core features:

- `checkpoint`
- `cookie-store` (also enables `middleware-cookies`)

Example:

```toml
[dependencies]
spider-lib = { version = "2.0.3", features = ["middleware-robots", "pipeline-jsonl"] }
```

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
- Contributing: [CONTRIBUTING.md](./CONTRIBUTING.md)

## License

MIT. See [LICENSE](./LICENSE).
