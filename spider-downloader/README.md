# spider-downloader

Provides traits and implementations for HTTP downloaders in the `spider-lib` framework.

## Overview

The `spider-downloader` crate defines the foundational traits for handling HTTP requests and responses within the web crawling framework. It abstracts the underlying HTTP client implementation, allowing for flexibility in choosing different HTTP libraries while maintaining a consistent interface for the crawling engine.

## Key Components

- **Downloader Trait**: Interface for components that execute web requests and produce `Response` objects. Implementations typically wrap HTTP client libraries like `reqwest`.
- **HttpClient Trait**: Basic interface for performing simple GET requests, used for internal utility functions or when a full `Request` object isn't necessary.

## Architecture

The downloader system is designed to be pluggable, allowing users to implement custom downloaders with different behaviors (e.g., with different retry strategies, proxy support, or caching mechanisms).

## Usage

```rust
use spider_downloader::{Downloader, HttpClient};
use spider_util::{request::Request, response::Response, error::SpiderError};
use async_trait::async_trait;

struct MyDownloader {
    client: reqwest::Client,
}

#[async_trait]
impl Downloader for MyDownloader {
    type Client = reqwest::Client;

    async fn download(&self, request: Request) -> Result<Response, SpiderError> {
        // Implementation for downloading web pages
        todo!()
    }

    fn client(&self) -> &Self::Client {
        &self.client
    }
}
```

## License

This project is licensed under the MIT License - see the [LICENSE](./LICENSE) file for details.
