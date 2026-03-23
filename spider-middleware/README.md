# spider-middleware

`spider-middleware` contains the middleware layer used by the crawler runtime. This is where request and response behavior can be adjusted without pushing transport concerns into the downloader or page-specific logic into the spider.

For most application code, middleware is enabled through [`spider-lib`](../README.md) feature flags. This crate becomes more useful when you want to work against the middleware trait directly or publish reusable middleware.

## When to use it directly

Use `spider-middleware` if you want to:

- compose middleware without the facade crate
- implement custom middleware against the shared runtime contract
- publish middleware for other `spider-core` users

If your only goal is to enable built-in middleware in an app, the root crate is still the smoother path.

## Installation

```toml
[dependencies]
spider-middleware = "0.3.6"
```

## Built-in middleware

Always available:

| Type | Purpose |
| --- | --- |
| `RateLimitMiddleware` | Smooths request throughput. |
| `RetryMiddleware` | Retries failed requests according to retry policy. |
| `RefererMiddleware` | Sets `Referer` for follow-up requests. |

Feature-gated modules:

| Feature | Type | Use case |
| --- | --- | --- |
| `middleware-cache` | `HttpCacheMiddleware` | Reuse cached responses. |
| `middleware-autothrottle` | `AutoThrottleMiddleware` | Adapt crawl pace to observed conditions. |
| `middleware-proxy` | `ProxyMiddleware` | Route traffic through proxies. |
| `middleware-user-agent` | `UserAgentMiddleware` | Set or rotate user agents. |
| `middleware-robots` | `RobotsTxtMiddleware` | Respect `robots.txt`. |
| `middleware-cookies` | `CookieMiddleware` | Store and attach cookies. |

## Runtime usage

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

## Custom middleware example

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
        &self,
        _client: &C,
        request: Request,
    ) -> Result<MiddlewareAction<Request>, SpiderError> {
        if request.url.domain() == Some("blocked.example") {
            return Ok(MiddlewareAction::Drop);
        }

        Ok(MiddlewareAction::Continue(request))
    }
}
```

Wire it into the runtime with `CrawlerBuilder::add_middleware(...)`.

## Hook lifecycle

Middleware is easier to reason about if you treat it as three distinct hooks:

1. `process_request` runs before download and can rewrite, drop, or short-circuit a request.
2. `process_response` runs after a successful download and can rewrite, drop, or retry.
3. `handle_error` runs on download failure and can propagate, drop, or retry.

In practice, request-shaping concerns belong in `process_request`, status/body-based policy belongs in `process_response`, and recovery policy belongs in `handle_error`.

## Feature flags

```toml
[dependencies]
spider-middleware = { version = "0.3.6", features = ["middleware-robots", "middleware-user-agent"] }
```

When you depend on the root crate instead, enable the same feature names on `spider-lib`.

## Ordering matters

A reasonable default order for many crawlers is:

1. `RefererMiddleware`
2. `UserAgentMiddleware`
3. `ProxyMiddleware`
4. `RateLimitMiddleware`
5. `AutoThrottleMiddleware`
6. `RetryMiddleware`
7. `HttpCacheMiddleware`
8. `RobotsTxtMiddleware`
9. `CookieMiddleware`

That is only a starting point. Retry, cache, robots, and cookie behavior all depend on order, so it is worth being intentional.

## Related crates

- [`spider-lib`](../README.md)
- [`spider-core`](../spider-core/README.md)
- [`spider-util`](../spider-util/README.md)

## License

MIT. See [LICENSE](../LICENSE).
