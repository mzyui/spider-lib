# spider-downloader

Downloader traits and reqwest-based downloader implementation for `spider-lib`.

Use this crate directly when you want custom request execution while keeping compatibility with the spider runtime interfaces.

## Installation

```toml
[dependencies]
spider-downloader = "0.4.4"
```

## Key Exports

- `Downloader`: trait for request execution.
- `HttpClient`: lightweight HTTP client trait re-export.
- `ReqwestClientDownloader`: default reqwest-based downloader.

## Usage

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

    async fn download(&self, _request: Request) -> Result<Response, SpiderError> {
        todo!()
    }

    fn client(&self) -> &Self::Client {
        &self.client
    }
}
```

## Related Crates

- [`spider-lib`](../README.md)
- [`spider-core`](../spider-core/README.md)
- [`spider-util`](../spider-util/README.md)

## License

MIT. See [LICENSE](../LICENSE).
