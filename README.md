# spider-lib

[![crates.io](https://img.shields.io/crates/v/spider-lib.svg)](https://crates.io/crates/spider-lib)
[![crates.io downloads](https://img.shields.io/crates/d/spider-lib.svg)](https://crates.io/crates/spider-lib)
[![rust edition](https://img.shields.io/badge/edition-2024-orange.svg)](https://doc.rust-lang.org/edition-guide/rust-2024/index.html)
[![stable docs](https://img.shields.io/docsrs/spider-lib?label=stable%20docs)](https://docs.rs/spider-lib)
[![latest docs](https://img.shields.io/badge/latest-docs-blue)](https://mzyui.github.io/spider-lib)
[![license](https://img.shields.io/crates/l/spider-lib.svg)](https://github.com/mzyui/spider-lib/blob/main/LICENSE)

`spider-lib` is an async web scraping framework for Rust with a layout that will feel familiar if you have used Scrapy before: spiders define crawl logic, the runtime schedules and downloads requests, middleware can shape traffic, and pipelines handle extracted items.

The workspace is split into small crates, but the root crate is the easiest place to start. It re-exports the common pieces through `spider_lib::prelude::*`, so a normal application does not need to wire the lower-level crates by hand.

## Why this crate exists

`spider-lib` is meant for projects that need more structure than a one-off `reqwest + scraper` script:

- multiple follow-up requests from each page
- shared crawl state
- middleware for retries, rate limiting, cookies, robots, or proxying
- pipelines for validation, deduplication, and output
- typed item schemas that can drive validation and export mapping
- a runtime that keeps the crawling loop organized

If you only need to fetch one or two pages, the lower ceremony of plain `reqwest` may still be a better fit.

By default, the built-in reqwest downloader now sends a balanced set of
browser-like headers when a request does not already define them. That helps
HTML crawling behave more like a normal browser without taking control away
from spiders or middleware that set headers explicitly.

## Installation

```toml
[dependencies]
spider-lib = "3.0.2"
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"
```

`serde` and `serde_json` are required when you use `#[scraped_item]`.

## Recommended path

For most projects, the smoothest path is:

1. start with `use spider_lib::prelude::*;`
2. implement `Spider`
3. build a runtime with `CrawlerBuilder`
4. add middleware for HTTP behavior
5. add pipelines for item shaping and output

Only drop to the lower-level crates when you need deeper runtime control or
want to publish reusable extensions.

If you are coming from Scrapy, start with the dedicated migration guide in
[`MIGRATION.md`](MIGRATION.md) before porting an existing spider.

## Typed data workflow

`#[scraped_item]` now generates typed schema metadata in addition to the
existing `ScrapedItem` implementation. That schema can drive validation,
export ordering, and schema-version tagging without forcing you to hand-maintain
JSON field lists.

```rust,ignore
use spider_lib::prelude::*;

#[scraped_item]
struct Quote {
    text: String,
    author: String,
    tags: Vec<String>,
    source_url: Option<String>,
}

let crawler = CrawlerBuilder::new(MySpider)
    .crawl_shape_preset(CrawlShapePreset::ApiHeavy)
    .add_pipeline(SchemaValidationPipeline::<Quote>::new().expect_schema_version(1))
    .add_pipeline(
        CsvPipeline::new("output/quotes.csv")?.with_schema_export_config(
            SchemaExportConfig::new().with_field_alias("source_url", "url"),
        ),
    )
    .build()
    .await?;
```

If you are crawling APIs or want a minimal request shape, disable the default
header profile with:

```rust,ignore
let crawler = CrawlerBuilder::new(MySpider)
    .browser_like_headers(false)
    .build()
    .await?;
```

## Quick start

This is the smallest useful shape of a spider in the current API:

```rust,ignore
use spider_lib::prelude::*;

#[scraped_item]
struct Quote {
    text: String,
    author: String,
}

struct QuotesSpider;

#[async_trait]
impl Spider for QuotesSpider {
    type Item = Quote;
    type State = ();

    fn start_requests(&self) -> Result<StartRequests<'_>, SpiderError> {
        Ok(StartRequests::Urls(vec!["https://quotes.toscrape.com/"]))
    }

    async fn parse(
        &self,
        response: Response,
        _state: &Self::State,
    ) -> Result<ParseOutput<Self::Item>, SpiderError> {
        let mut output = ParseOutput::new();

        for quote in response.css(".quote")? {
            let text = quote
                .css(".text::text")?
                .get()
                .unwrap_or_default();

            let author = quote
                .css(".author::text")?
                .get()
                .unwrap_or_default();

            output.add_item(Quote { text, author });
        }

        Ok(output)
    }
}

#[tokio::main]
async fn main() -> Result<(), SpiderError> {
    let crawler = CrawlerBuilder::new(QuotesSpider).build().await?;
    crawler.start_crawl().await
}
```

## Run the examples

The repository ships with maintained examples that you can run as-is:

```bash
cargo run --example minimal
cargo run --example books
cargo run --example sitemap
cargo run --example request_priority
```

`minimal` is the quickest smoke example.

`sitemap` shows runtime-managed sitemap discovery and page metadata extraction.

That example crawls `books.toscrape.com` and prints the final page and item counts.

`request_priority` is a local scheduler demo that shows higher-priority
requests being dequeued first without needing network access.

There are also several feature-gated showcase examples:

```bash
cargo run --example showcase_state
cargo run --example showcase_middleware --features "middleware-autothrottle middleware-cache middleware-proxy middleware-user-agent middleware-robots middleware-cookies cookie-store"
cargo run --example showcase_pipelines --features "pipeline-json pipeline-jsonl pipeline-csv pipeline-sqlite pipeline-stream-json"
cargo run --example showcase_runtime --features "live-stats checkpoint"
cargo run --example books_live --features "live-stats pipeline-csv"
cargo run --example kusonime --features "live-stats pipeline-stream-json"
```

`showcase_state` demonstrates facade usage plus shared state primitives.

`showcase_middleware` focuses on request/response middleware composition.

`showcase_pipelines` writes the same scraped item through the maintained output pipelines.

`showcase_runtime` focuses on builder tuning, live stats, and checkpoint configuration.

`books_live` writes CSV output to `output/books_live.csv`.

`kusonime` writes streaming JSON output to `output/kusonime-stream.json`.

These examples depend on public sites being reachable, so they are good smoke runs but still network-dependent.

## How the crawl loop fits together

At a high level:

1. `Spider::start_requests` seeds the crawl.
2. The scheduler accepts and deduplicates requests.
3. The downloader performs the HTTP work.
4. Middleware can inspect or modify requests and responses.
5. `Spider::parse` turns a `Response` into `ParseOutput`.
6. Pipelines process emitted items.

That separation is what makes the workspace easier to extend than a single-file scraper.

## Where to add behavior

- Put page extraction logic in `Spider::parse`.
- Put shared crawl state in `Spider::State`.
- Put cross-cutting request/response behavior in middleware.
- Put item cleanup, validation, deduplication, and output in pipelines.
- Put transport-specific behavior in a custom downloader only when middleware is too high-level.

## Feature flags

Root crate features mirror the lower-level crates:

| Feature | What it enables |
| --- | --- |
| `core` | Base runtime support. Enabled by default. |
| `live-stats` | Live terminal crawl stats. |
| `middleware-cache` | HTTP response cache middleware. |
| `middleware-autothrottle` | Adaptive throttling middleware. |
| `middleware-proxy` | Proxy middleware. |
| `middleware-user-agent` | User-agent middleware. |
| `middleware-robots` | `robots.txt` middleware. |
| `middleware-cookies` | Cookie middleware. |
| `pipeline-csv` | CSV output pipeline. |
| `pipeline-json` | JSON array output pipeline. |
| `pipeline-jsonl` | JSON Lines output pipeline. |
| `pipeline-sqlite` | SQLite output pipeline. |
| `pipeline-stream-json` | Streaming JSON output pipeline. |
| `checkpoint` | Checkpoint and resume support. |
| `cookie-store` | Cookie store integration in core state. |

## Runtime discovery

The crawler can now add follow-up requests without manual spider boilerplate for
common discovery flows:

- `DiscoveryMode::HtmlLinks` for same-site page links
- `DiscoveryMode::HtmlAndMetadata` for page links plus injected page metadata
- `DiscoveryMode::FullResources` for scripts, stylesheets, images, and other resources
- `DiscoveryMode::SitemapOnly` for sitemap-driven crawling

Example:

```rust,ignore
let crawler = CrawlerBuilder::new(MySpider)
    .discovery_mode(DiscoveryMode::SitemapOnly)
    .enable_sitemaps(true)
    .extract_page_metadata(true)
    .build()
    .await?;
```

Runtime discovery can also be filtered more aggressively when you want a
rule-like crawl shape:

```rust,ignore
let crawler = CrawlerBuilder::new(MySpider)
    .discovery_mode(DiscoveryMode::HtmlLinks)
    .discover_allow_domains(["books.toscrape.com"])
    .discover_allow_path_prefixes(["/catalogue/"])
    .discover_deny_patterns(["*/page-*.html"])
    .discover_allowed_tags(["a"])
    .discover_allowed_attributes(["href"])
    .build()
    .await?;
```

For more structured crawling, you can define named discovery rules and route
parse logic without manual metadata matching:

```rust,ignore
let listing_rule = DiscoveryRule::new("listing")
    .with_allow_path_prefixes(["/catalogue/"])
    .with_allowed_tags(["a"])
    .with_allowed_attributes(["href"])
    .with_follow_allow_path_prefixes(["/catalogue/"]);

let crawler = CrawlerBuilder::new(MySpider)
    .discovery_mode(DiscoveryMode::HtmlLinks)
    .add_discovery_rule(listing_rule)
    .build()
    .await?;

// later in parse:
route_by_rule!(
    response,
    "listing" => self.parse_listing(response, state).await,
    _ => self.parse_default(response, state).await,
);
```

Example:

```toml
[dependencies]
spider-lib = { version = "3.0.2", features = ["live-stats", "pipeline-csv"] }
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"
```

## Workspace map

- [`spider-core`](spider-core/README.md): crawler runtime, builder, scheduler, state, and stats
- [`spider-downloader`](spider-downloader/README.md): downloader trait and the default reqwest implementation
- [`spider-macro`](spider-macro/README.md): `#[scraped_item]`
- [`spider-middleware`](spider-middleware/README.md): built-in middleware implementations
- [`spider-pipeline`](spider-pipeline/README.md): item pipelines and output backends
- [`spider-util`](spider-util/README.md): shared request, response, item, and error types

## When to use the lower-level crates directly

Stay on `spider-lib` if you are building an application spider.

Reach for individual crates when you are:

- publishing reusable middleware, pipeline, or downloader extensions
- composing the runtime more explicitly
- depending on shared types without pulling in the whole facade crate

The most common next step down is [`spider-core`](spider-core/README.md), which
keeps the runtime API but drops the facade re-exports.

## Status

The current workspace builds successfully with:

```bash
cargo check --workspace --all-targets
```

That is a useful baseline when updating docs or examples.

## License

MIT. See [LICENSE](LICENSE).
