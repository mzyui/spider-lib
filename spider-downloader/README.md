# spider-downloader

Downloader traits and the default reqwest-based downloader for `spider-lib`.

Use this crate directly when you want downloader-level control while staying compatible with the crawler runtime interfaces used by the rest of the workspace.

## When to Use This Crate Directly

Use `spider-downloader` if you need one or more of these:

- Custom authentication or request signing.
- Non-default transport behavior beyond the built-in reqwest setup.
- Instrumentation or tracing at the request execution boundary.
- Integration with external HTTP infrastructure while preserving `spider-*` request and response types.

If you want the integrated framework experience, prefer [`spider-lib`](../README.md).

## Installation

```toml
[dependencies]
spider-downloader = "1.0.2"
reqwest = "0.13"
```

## Key Exports

- `Downloader`: trait for request execution.
- `HttpClient`: lightweight HTTP client trait re-export.
- `ReqwestClientDownloader`: the built-in reqwest-based downloader.

## Downloader Contract

A `Downloader` implementation is responsible for:

- receiving a `spider_util::request::Request`
- executing the HTTP transaction
- returning a `spider_util::response::Response` on success
- mapping transport or protocol failures into `SpiderError`

That boundary keeps the rest of the runtime predictable for middleware and parser logic.

## Build a Custom Downloader

```rust,ignore
use async_trait::async_trait;
use spider_downloader::Downloader;
use spider_util::{error::SpiderError, request::Request, response::Response};

struct MyDownloader {
    client: reqwest::Client,
}

#[async_trait]
impl Downloader for MyDownloader {
    type Client = reqwest::Client;

    async fn download(&self, request: Request) -> Result<Response, SpiderError> {
        let _req = request;

        // 1) map spider Request into an HTTP request
        // 2) execute with self.client
        // 3) map the HTTP response back into spider Response
        // 4) convert transport errors into SpiderError
        todo!()
    }

    fn client(&self) -> &Self::Client {
        &self.client
    }
}
```

## Using the Built-In Downloader

For most projects, `CrawlerBuilder` already uses the reqwest downloader by default. If you want to make that dependency explicit:

```rust,ignore
use spider_core::CrawlerBuilder;
use spider_downloader::ReqwestClientDownloader;

let crawler = CrawlerBuilder::new(MySpider)
    .downloader(ReqwestClientDownloader::new())
    .build()
    .await?;
```

If you provide your own downloader, wire it in the same place:

```rust,ignore
let crawler = CrawlerBuilder::new(MySpider)
    .downloader(MyDownloader {
        client: reqwest::Client::new(),
    })
    .build()
    .await?;
```

## Common Gotchas

- `spider-downloader` gives you the trait boundary, not a full crawler on its own.
- Custom downloaders must preserve request metadata if the runtime depends on it later.
- If you only need the default behavior, using `spider-lib` or `spider-core` directly is usually simpler.

## Related Crates

- [`spider-lib`](../README.md)
- [`spider-core`](../spider-core/README.md)
- [`spider-util`](../spider-util/README.md)

## License

MIT. See [LICENSE](../LICENSE).
