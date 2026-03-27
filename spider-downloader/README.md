# spider-downloader

`spider-downloader` defines the downloader boundary used by the rest of the workspace and includes the default reqwest-based implementation, `ReqwestClientDownloader`.

This crate is mostly for people who want to control HTTP execution more directly without giving up the request and response types used across `spider-lib`.

## When to use it directly

Reach for `spider-downloader` if you need:

- custom authentication or request signing
- special transport behavior that should happen below middleware
- downloader-level tracing or instrumentation
- a custom HTTP stack that still plugs into the existing crawler runtime

If you are happy with the default HTTP behavior, the root crate or `spider-core` will usually be simpler.

## Installation

```toml
[dependencies]
spider-downloader = "1.1.0"
reqwest = "0.13"
```

## Main exports

- `Downloader`: trait for request execution
- `HttpClient`: shared client abstraction re-export
- `ReqwestClientDownloader`: default downloader implementation

## The contract

A downloader receives a `spider_util::request::Request`, performs the HTTP transaction, and returns a `spider_util::response::Response` or a `SpiderError`.

That sounds small, but it is an important seam: middleware and parsers can stay predictable because the transport logic is isolated here.

## Custom downloader example

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
        let _request = request;

        // Convert the spider Request into your transport layer,
        // execute it, then map the result back into Response.
        todo!()
    }

    fn client(&self) -> &Self::Client {
        &self.client
    }
}
```

## Using the built-in downloader

`CrawlerBuilder` already uses the reqwest downloader by default, so in many projects you never need to mention this crate explicitly.

If you want to make the dependency obvious in your runtime setup:

```rust,ignore
use spider_core::CrawlerBuilder;
use spider_downloader::ReqwestClientDownloader;

let crawler = CrawlerBuilder::new(MySpider)
    .downloader(ReqwestClientDownloader::new())
    .build()
    .await?;
```

The built-in downloader currently supports request bodies, request metadata, timeouts, and proxy-aware client selection.

## When custom downloader is the right tool

Reach for a custom downloader only when behavior belongs below middleware, such as:

- a non-standard HTTP client stack
- request signing tightly coupled to transport execution
- downloader-level tracing or instrumentation
- alternate protocol behavior that still maps into `Request` and `Response`

If normal HTTP concerns can be expressed as request/response policy, middleware is usually the better seam.

## Good to know

- This crate is only the downloader layer, not a full crawler.
- If your runtime relies on request metadata, a custom downloader should preserve it when building the returned `Response`.
- If all you need is “download pages and parse them”, [`spider-lib`](../README.md) remains the better starting point.

## Related crates

- [`spider-lib`](../README.md)
- [`spider-core`](../spider-core/README.md)
- [`spider-util`](../spider-util/README.md)

## License

MIT. See [LICENSE](../LICENSE).
