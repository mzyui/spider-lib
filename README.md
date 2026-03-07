# spider-lib

A modular Rust web scraping framework inspired by Scrapy.

`spider-lib` is the facade crate for the workspace. It re-exports core crawling, downloader, middleware, pipeline, utility, and macro APIs so you can start with one dependency and enable only the features you need.

## Workspace Crates

- [`spider-core`](./spider-core/README.md): crawler runtime, spider trait, scheduler, builder, state, and stats.
- [`spider-downloader`](./spider-downloader/README.md): downloader traits and reqwest-based downloader implementation.
- [`spider-macro`](./spider-macro/README.md): procedural macros such as `#[scraped_item]`.
- [`spider-middleware`](./spider-middleware/README.md): retry, rate limiting, robots, cookies, proxy, cache, and user-agent middleware.
- [`spider-pipeline`](./spider-pipeline/README.md): item processing and output pipelines (JSON, JSONL, CSV, SQLite, stream JSON).
- [`spider-util`](./spider-util/README.md): shared request/response/item/error types and helper utilities.

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

Try the maintained examples:

```bash
cargo run --example books
cargo run --example books_live --features live-stats
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

- `live-stats`: enables in-place terminal stat updates.
- `checkpoint`: enables checkpoint/resume support.
- `cookie-store`: enables cookie store integration (also enables `middleware-cookies`).

Example:

```toml
[dependencies]
spider-lib = { version = "2.0.4", features = ["middleware-robots", "pipeline-jsonl"] }
```

## Using Workspace Crates Directly

Use `spider-lib` when you want the integrated API surface.

Use sub-crates directly if you need tighter dependency control or only one subsystem (for example, custom downloader integration with `spider-downloader`, or utility types from `spider-util`).

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
