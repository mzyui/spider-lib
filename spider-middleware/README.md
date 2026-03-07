# spider-middleware

Built-in middleware implementations for `spider-lib` crawlers.

Most users enable middleware through `spider-lib` features. Use this crate directly when composing middleware in custom runtime setups.

## Installation

```toml
[dependencies]
spider-middleware = "0.3.4"
```

## Middleware Catalog

### Core (always available)

- `RateLimitMiddleware`: controls request rate and smooths burst traffic.
- `RetryMiddleware`: retries failed requests with retry policy.
- `RefererMiddleware`: sets `Referer` headers for follow-up requests.

### Optional (feature-gated)

- `middleware-cache` -> `HttpCacheMiddleware`
- `middleware-autothrottle` -> `AutoThrottleMiddleware`
- `middleware-proxy` -> `ProxyMiddleware`
- `middleware-user-agent` -> `UserAgentMiddleware`
- `middleware-robots` -> `RobotsTxtMiddleware`
- `middleware-cookies` -> `CookieMiddleware`

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

## Optional Middleware Usage (One by One)

### `middleware-cache` (`HttpCacheMiddleware`)

```toml
[dependencies]
spider-lib = { version = "2.0.4", features = ["middleware-cache"] }
```

```rust,ignore
use spider_lib::prelude::*;

let crawler = CrawlerBuilder::new(MySpider)
    .add_middleware(HttpCacheMiddleware::new())
    .build()
    .await?;
```

### `middleware-autothrottle` (`AutoThrottleMiddleware`)

```toml
[dependencies]
spider-lib = { version = "2.0.4", features = ["middleware-autothrottle"] }
```

```rust,ignore
use spider_lib::prelude::*;

let crawler = CrawlerBuilder::new(MySpider)
    .add_middleware(AutoThrottleMiddleware::default())
    .build()
    .await?;
```

### `middleware-proxy` (`ProxyMiddleware`)

```toml
[dependencies]
spider-lib = { version = "2.0.4", features = ["middleware-proxy"] }
```

```rust,ignore
use spider_lib::prelude::*;

let crawler = CrawlerBuilder::new(MySpider)
    .add_middleware(ProxyMiddleware::new())
    .build()
    .await?;
```

### `middleware-user-agent` (`UserAgentMiddleware`)

```toml
[dependencies]
spider-lib = { version = "2.0.4", features = ["middleware-user-agent"] }
```

```rust,ignore
use spider_lib::prelude::*;

let crawler = CrawlerBuilder::new(MySpider)
    .add_middleware(UserAgentMiddleware::new())
    .build()
    .await?;
```

### `middleware-robots` (`RobotsTxtMiddleware`)

```toml
[dependencies]
spider-lib = { version = "2.0.4", features = ["middleware-robots"] }
```

```rust,ignore
use spider_lib::prelude::*;

let crawler = CrawlerBuilder::new(MySpider)
    .add_middleware(RobotsTxtMiddleware::new())
    .build()
    .await?;
```

### `middleware-cookies` (`CookieMiddleware`)

```toml
[dependencies]
spider-lib = { version = "2.0.4", features = ["middleware-cookies"] }
```

```rust,ignore
use spider_lib::prelude::*;

let crawler = CrawlerBuilder::new(MySpider)
    .add_middleware(CookieMiddleware::new())
    .build()
    .await?;
```

## Middleware Ordering Guidance

A practical default order for many spiders:

1. `RefererMiddleware`
2. `UserAgentMiddleware` (optional)
3. `ProxyMiddleware` (optional)
4. `RateLimitMiddleware`
5. `AutoThrottleMiddleware` (optional)
6. `RetryMiddleware`
7. `HttpCacheMiddleware` (optional)
8. `RobotsTxtMiddleware` (optional)
9. `CookieMiddleware` (optional)

Adjust ordering based on your request policy and target-site constraints.

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
spider-middleware = { version = "0.3.4", features = ["middleware-robots", "middleware-user-agent"] }
```

When using via `spider-lib`, enable root features with the same names.

## Related Crates

- [`spider-lib`](../README.md)
- [`spider-core`](../spider-core/README.md)
- [`spider-util`](../spider-util/README.md)

## License

MIT. See [LICENSE](../LICENSE).
