# spider-middleware

Built-in middleware implementations for `spider-lib` crawlers.

## Install

```toml
[dependencies]
spider-middleware = "0.3.3"
```

## Built-in Middleware

Core (always available):

- `RateLimitMiddleware`
- `RetryMiddleware`
- `RefererMiddleware`

Optional (feature-gated):

- `middleware-cache` -> `HttpCacheMiddleware`
- `middleware-autothrottle` -> `AutoThrottleMiddleware`
- `middleware-proxy` -> `ProxyMiddleware`
- `middleware-user-agent` -> `UserAgentMiddleware`
- `middleware-robots` -> `RobotsTxtMiddleware`
- `middleware-cookies` -> `CookieMiddleware`

## Usage

```rust,no_run
use spider_middleware::{rate_limit::RateLimitMiddleware, retry::RetryMiddleware};

let crawler = spider_core::CrawlerBuilder::new(MySpider)
    .add_middleware(RateLimitMiddleware::default())
    .add_middleware(RetryMiddleware::new())
    .build()
    .await?;
```

## Feature Flags

- `core` (default)
- `middleware-cache`
- `middleware-autothrottle`
- `middleware-proxy`
- `middleware-user-agent`
- `middleware-robots`
- `middleware-cookies`

```toml
[dependencies]
spider-middleware = { version = "0.3.3", features = ["middleware-robots", "middleware-user-agent"] }
```

When using via `spider-lib`, enable root features with the same names.

## License

MIT. See [LICENSE](./LICENSE).
