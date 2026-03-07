# spider-downloader

Downloader traits and reqwest-based downloader implementation for `spider-lib`.

Use this crate directly when you need downloader-level control while staying compatible with the crawler runtime interfaces.

## Installation

```toml
[dependencies]
spider-downloader = "1.0.0"
```

## When to Use This Crate Directly

Use `spider-downloader` directly if you need one or more of these:

- Custom authentication/signing logic.
- Transport customization beyond default reqwest behavior.
- Request/response instrumentation at downloader boundary.
- Integration with external HTTP infrastructure while preserving `spider-*` types.

If you want the integrated framework surface, prefer `spider-lib`.

## Key Exports

- `Downloader`: trait for request execution.
- `HttpClient`: lightweight HTTP client trait re-export.
- `ReqwestClientDownloader`: default reqwest-based downloader.

## Downloader Contract

`Downloader` implementors should:

- Accept a `spider_util::request::Request`.
- Execute an HTTP transaction with your client.
- Return a `spider_util::response::Response` on success.
- Return `SpiderError` for network/protocol/serialization failures.

The crawler runtime depends on this contract to keep middleware and parser behavior predictable.

## Build a Custom Downloader

Use custom downloader implementations for transport-level extension.

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

        // 1) map spider Request to HTTP request
        // 2) execute with self.client
        // 3) map HTTP response back into spider Response
        // 4) map transport and parsing errors into SpiderError
        todo!()
    }

    fn client(&self) -> &Self::Client {
        &self.client
    }
}
```

## Runtime Integration Pattern

In full crawler setups, this trait implementation is consumed by the runtime path that executes scheduled requests before parser callbacks run.

```rust,ignore
use spider_core::CrawlerBuilder;

let _crawler = CrawlerBuilder::new(MySpider)
    // use downloader-compatible runtime composition
    .build()
    .await?;
```

## Related Crates

- [`spider-lib`](../README.md)
- [`spider-core`](../spider-core/README.md)
- [`spider-util`](../spider-util/README.md)

## License

MIT. See [LICENSE](../LICENSE).
