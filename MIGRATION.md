# Migrating from Scrapy to spider-lib

`spider-lib` follows a crawl model that should feel familiar if you have used Scrapy before: spiders define crawl logic, the runtime schedules requests, middleware shapes HTTP behavior, and pipelines process extracted items.

This guide shows how to translate that mental model into the current `spider-lib` API.

## The short version

If you already know Scrapy, the main translation is:

- define items as Rust structs with `#[scraped_item]`
- implement `Spider` for crawl logic
- seed the crawl with `start_urls()` or `start_requests()`
- return a `ParseOutput` from `parse()`
- call `output.add_item(...)` instead of `yield item`
- call `output.add_request(...)` instead of `yield Request(...)`
- configure middleware and pipelines through `CrawlerBuilder`
- put mutable crawl state in `Spider::State`, not on the spider itself

## Core concept mapping

| Scrapy | spider-lib |
| --- | --- |
| `scrapy.Item` or plain dict | `#[scraped_item] struct` |
| `class MySpider(scrapy.Spider)` | `struct MySpider;` plus `impl Spider for MySpider` |
| `name`, `allowed_domains`, `start_urls` | spider struct plus `start_urls()` or `start_requests()` |
| `start_requests()` | `fn start_requests(&self) -> Result<StartRequests<'_>, SpiderError>` |
| `parse(self, response)` | `async fn parse(&self, response, state) -> Result<ParseOutput<_>, SpiderError>` |
| `yield item` | `output.add_item(item)` |
| `yield scrapy.Request(...)` | `output.add_request(Request::new(...))` |
| `cb_kwargs` or `meta` | request metadata via `with_meta(...)` or `with_meta_value(...)` |
| downloader middleware | middleware added with `CrawlerBuilder::add_middleware(...)` |
| item pipelines | pipelines added with `CrawlerBuilder::add_pipeline(...)` |
| settings-driven tuning | builder methods and Cargo features |
| mutable spider fields | `Spider::State` with thread-safe primitives |

## Minimal spider: Scrapy vs spider-lib

### Scrapy

```python
import scrapy


class QuoteSpider(scrapy.Spider):
    name = "quotes"
    start_urls = ["https://quotes.toscrape.com/"]

    def parse(self, response):
        for quote in response.css(".quote"):
            yield {
                "text": quote.css(".text::text").get(default="").strip(),
                "author": quote.css(".author::text").get(default="").strip(),
            }

        next_href = response.css(".next a::attr(href)").get()
        if next_href:
            yield response.follow(next_href, callback=self.parse)
```

### spider-lib

```rust,ignore
use spider_lib::prelude::*;

#[scraped_item]
struct QuoteItem {
    text: String,
    author: String,
}

struct QuoteSpider;

#[async_trait]
impl Spider for QuoteSpider {
    type Item = QuoteItem;
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
                .unwrap_or_default()
                .trim()
                .to_string();

            let author = quote
                .css(".author::text")?
                .get()
                .unwrap_or_default()
                .trim()
                .to_string();

            output.add_item(QuoteItem { text, author });
        }

        if let Some(next_href) = response.css(".next a::attr(href)")?.get() {
            let next_url = response.url.join(&next_href)?;
            output.add_request(Request::new(next_url));
        }

        Ok(output)
    }
}

#[tokio::main]
async fn main() -> Result<(), SpiderError> {
    let crawler = CrawlerBuilder::new(QuoteSpider).build().await?;
    crawler.start_crawl().await
}
```

## Items

In Scrapy, you might emit dicts or `Item` objects. In `spider-lib`, you usually define a Rust struct and annotate it with `#[scraped_item]`.

```rust,ignore
use spider_lib::prelude::*;

#[scraped_item]
struct ProductItem {
    title: String,
    price: String,
    url: String,
}
```

That macro wires up the traits needed by the runtime and keeps item handling typed from the start.

If you already have a stable Scrapy item schema, port that schema first. It gives you a clean anchor for the rest of the migration.

## Spiders and parse flow

The shape of a spider stays familiar:

- a spider defines seed requests
- the runtime downloads responses
- `parse()` extracts items and follow-up requests

The biggest practical difference is that `parse()` returns a `ParseOutput<Self::Item>` instead of yielding values one by one.

```rust,ignore
let mut output = ParseOutput::new();
output.add_item(item);
output.add_request(request);
Ok(output)
```

Think of `ParseOutput` as the explicit handoff object that replaces Scrapy's generator-style `yield`.

## Start URLs and start requests

Use `start_urls()` when plain static URLs are enough:

```rust,ignore
fn start_urls(&self) -> Vec<&'static str> {
    vec!["https://example.com"]
}
```

Use `start_requests()` when you need full request objects, file-backed seeds, headers, methods, metadata, or bodies:

```rust,ignore
fn start_requests(&self) -> Result<StartRequests<'_>, SpiderError> {
    Ok(StartRequests::iter(
        vec![
            Request::try_new("https://example.com/page/1")?
                .with_meta("source", serde_json::json!("seed")),
            Request::try_new("https://example.com/api/search")?
                .with_method(Method::Post)
                .with_json(serde_json::json!({ "query": "rust" })),
        ]
        .into_iter()
        .map(Ok),
    ))
}
```

If your Scrapy spider builds custom `scrapy.Request(...)` objects in `start_requests()`, this is the closest migration point.

## Follow-up requests

This is the common translation:

### Scrapy

```python
yield response.follow(next_href, callback=self.parse)
```

### spider-lib

```rust,ignore
let next_url = response.url.join(next_href)?;
output.add_request(Request::new(next_url));
```

For custom requests, build them directly:

```rust,ignore
let request = Request::try_new("https://example.com/api/items")?
    .with_method(Method::Post)
    .with_header("Accept", "application/json")?
    .with_json(serde_json::json!({ "page": 2 }))
    .with_meta("source", serde_json::json!("pagination"));

output.add_request(request);
```

## Request metadata

Scrapy users often rely on `meta`, `cb_kwargs`, or ad-hoc request context.

`spider-lib` gives you request metadata helpers:

```rust,ignore
let request = Request::try_new("https://example.com/detail/42")?
    .with_meta("category", serde_json::json!("books"))
    .with_meta("page", serde_json::json!(3));
```

You can read metadata later from the request object when middleware or lower-level extensions need it. For simple spiders, many users prefer to encode context in the URL path or query first, then add metadata only when the crawl logic needs it.

## Shared state

This is the biggest mindset change.

In Scrapy, users often keep counters, caches, or temporary state on the spider instance. In `spider-lib`, `parse()` takes `&self`, so you should treat the spider itself as immutable and move mutable shared state into `Spider::State`.

```rust,ignore
use spider_lib::prelude::*;

#[derive(Clone, Default)]
struct MyState {
    pages_seen: Counter,
    visited: ConcurrentMap<String, bool>,
}

#[scraped_item]
struct Item {
    title: String,
}

struct MySpider;

#[async_trait]
impl Spider for MySpider {
    type Item = Item;
    type State = MyState;

    async fn parse(
        &self,
        response: Response,
        state: &Self::State,
    ) -> Result<ParseOutput<Self::Item>, SpiderError> {
        state.pages_seen.increment();
        state.visited.insert(response.url.to_string(), true);

        Ok(ParseOutput::new())
    }
}
```

If your Scrapy spider mutates `self.some_cache`, `self.page_count`, or `self.seen_urls`, move that logic into `State` first.

## Middleware

Scrapy downloader middleware maps well to `spider-lib` middleware added through the builder:

```rust,ignore
let crawler = CrawlerBuilder::new(MySpider)
    .add_middleware(RateLimitMiddleware::default())
    .add_middleware(RetryMiddleware::new())
    .build()
    .await?;
```

Use middleware for cross-cutting HTTP behavior such as:

- retry policy
- throttling or rate limiting
- cookies
- proxies
- user-agent rotation
- `robots.txt` handling

Some middleware lives behind Cargo feature flags. Enable the features you need in `Cargo.toml`, then add the middleware in `CrawlerBuilder`.

## Pipelines

Scrapy item pipelines map to `spider-lib` pipelines:

```rust,ignore
let crawler = CrawlerBuilder::new(MySpider)
    .add_pipeline(ValidationPipeline::new())
    .add_pipeline(DeduplicationPipeline::new(["url"]))
    .add_pipeline(ConsolePipeline::new())
    .build()
    .await?;
```

Use pipelines for item lifecycle concerns such as:

- validation
- deduplication
- transformation
- export to JSON, JSONL, CSV, SQLite, or streaming JSON

As with middleware, some output pipelines require feature flags.

## Builder configuration

Scrapy users often expect behavior to live in settings. In `spider-lib`, many runtime choices live on `CrawlerBuilder`.

Typical examples:

```rust,ignore
let crawler = CrawlerBuilder::new(MySpider)
    .limit(100)
    .log_level(log::LevelFilter::Info)
    .browser_like_headers(false)
    .crawl_shape_preset(CrawlShapePreset::ApiHeavy)
    .build()
    .await?;
```

Read the builder as the place where you compose runtime behavior for one crawler instance.

## A practical migration path

Port your Scrapy project in this order:

1. Convert your item definitions into `#[scraped_item]` structs.
2. Create one Rust spider and port `start_urls` or `start_requests`.
3. Port one `parse()` path and return a `ParseOutput`.
4. Add pagination and detail-page requests with `output.add_request(...)`.
5. Move mutable spider fields into `Spider::State`.
6. Reintroduce middleware and pipelines after the crawl flow works.
7. Add output pipelines once the item schema looks stable.

This order keeps the migration boring. You establish the crawl loop first, then add policy and output around it.

## Common gotchas for Scrapy users

- `parse()` does not yield values directly. You collect items and requests in `ParseOutput`.
- The spider instance is not your mutable state bag. Put mutable data in `Spider::State`.
- Request customization happens on `Request`, not through many global settings.
- Middleware and pipelines are builder composition concerns, not just project-wide configuration.
- Feature-gated middleware and pipelines require Cargo feature flags before you can use them.
- You will write more explicit types up front, but you get stronger guarantees once the spider compiles.

## Where to look next

After this guide, the best references in this repository are:

- [`README.md`](README.md) for the top-level workflow
- [`examples/minimal.rs`](examples/minimal.rs) for the smallest complete spider
- [`examples/books.rs`](examples/books.rs) for pagination, detail pages, and shared state
- [`examples/showcase_pipelines.rs`](examples/showcase_pipelines.rs) for output pipelines

If you migrate one Scrapy spider at a time and keep the first Rust version small, the move is much easier than porting every Scrapy feature at once.
