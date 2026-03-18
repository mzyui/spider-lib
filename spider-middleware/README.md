# spider-middleware

Built-in middleware implementations for `spider-lib` crawlers.

Most users enable middleware through [`spider-lib`](../README.md) features. Use this crate directly when composing middleware in custom runtime setups or when publishing middleware extensions against the lower-level API.

## When to Use This Crate Directly

Use `spider-middleware` if you want to:

- work directly against the middleware trait
- compose middleware without pulling in the facade crate
- build or publish custom middleware for `spider-core`

If you only need to enable built-in middleware for an application, `spider-lib` is usually the better entry point.

## Installation

```toml
[dependencies]
spider-middleware = "0.3.5"
```

## Middleware Catalog

### Core middleware

| Type | Purpose |
| --- | --- |
| `RateLimitMiddleware` | Controls request throughput and smooths burst traffic. |
| `RetryMiddleware` | Retries transient failures with retry policy. |
| `RefererMiddleware` | Populates `Referer` for follow-up requests. |

### Optional middleware

| Feature | Type | Primary use case |
| --- | --- | --- |
| `middleware-cache` | `HttpCacheMiddleware` | Reuse cached responses. |
| `middleware-autothrottle` | `AutoThrottleMiddleware` | Adapt request pace based on observed conditions. |
| `middleware-proxy` | `ProxyMiddleware` | Route requests through proxies. |
| `middleware-user-agent` | `UserAgentMiddleware` | Set or rotate user agents. |
| `middleware-robots` | `RobotsTxtMiddleware` | Respect robots.txt policy. |
| `middleware-cookies` | `CookieMiddleware` | Persist and attach cookies. |

## Core Usage

```rust,ignore
use spider_middleware::{
    rate_limit::RateLimitMiddleware,
    referer::RefererMiddleware,
    retry::RetryMiddleware,
};

let crawler = spider_core::CrawlerBuilder::new(MySpider)
    .add_middleware(RateLimitMiddleware::default())
    .add_middleware(RetryMiddleware::new())
    .add_middleware(RefererMiddleware::new())
    .build()
    .await?;
```

## Build a Custom Middleware

```rust,ignore
use async_trait::async_trait;
use spider_middleware::middleware::{Middleware, MiddlewareAction};
use spider_util::{error::SpiderError, request::Request};

struct BlocklistMiddleware;

#[async_trait]
impl<C: Send + Sync> Middleware<C> for BlocklistMiddleware {
    fn name(&self) -> &str {
        "blocklist"
    }

    async fn process_request(
        &mut self,
        _client: &reqwest::Client,
        request: Request,
    ) -> Result<MiddlewareAction<Request>, SpiderError> {
        if request.url.domain() == Some("blocked.example") {
            return Ok(MiddlewareAction::Drop);
        }

        Ok(MiddlewareAction::Continue(request))
    }
}
```

Runtime integration:

```rust,ignore
let crawler = spider_core::CrawlerBuilder::new(MySpider)
    .add_middleware(BlocklistMiddleware)
    .build()
    .await?;
```

## Feature Flags

```toml
[dependencies]
spider-middleware = { version = "0.3.5", features = ["middleware-robots", "middleware-user-agent"] }
```

When used through `spider-lib`, enable the same feature names on the root crate.

## Middleware Ordering Guidance

A practical default order for many spiders:

1. `RefererMiddleware`
2. `UserAgentMiddleware`
3. `ProxyMiddleware`
4. `RateLimitMiddleware`
5. `AutoThrottleMiddleware`
6. `RetryMiddleware`
7. `HttpCacheMiddleware`
8. `RobotsTxtMiddleware`
9. `CookieMiddleware`

Treat this as a sensible default, not a hard rule. The best order depends on whether you want policy, transport, retry, cache, or stateful concerns to happen first.

## Common Gotchas

- Optional middleware is feature-gated and will not exist unless its feature is enabled.
- Middleware ordering affects behavior, especially for retries, cache, robots, and cookies.
- If you only want built-in middleware in an application, configuring it through `spider-lib` is simpler.

## Related Crates

- [`spider-lib`](../README.md)
- [`spider-core`](../spider-core/README.md)
- [`spider-util`](../spider-util/README.md)

## License

MIT. See [LICENSE](../LICENSE).
