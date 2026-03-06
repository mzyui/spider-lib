# spider-downloader

Downloader traits and reqwest-based downloader implementation for `spider-lib`.

## Install

```toml
[dependencies]
spider-downloader = "0.4.4"
```

## Exports

- `Downloader`: trait for request execution.
- `HttpClient`: lightweight HTTP client trait re-export.
- `ReqwestClientDownloader`: default reqwest implementation.

## Usage

```rust,no_run
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

## License

MIT. See [LICENSE](./LICENSE).
