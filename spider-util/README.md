# spider-util

`spider-util` is the shared types-and-helpers crate for the workspace. It is used internally by the other `spider-*` crates, and it is also the crate extension authors usually reach for first when they need the framework's request, response, item, or error types.

This is not the crawler runtime itself. Think of it as the common language the rest of the workspace speaks.

## When to use it directly

Use `spider-util` when you need:

- request and response models shared across the runtime
- `ScrapedItem` and the `ParseOutput` sink used during parsing
- framework-native errors such as `SpiderError` and `PipelineError`
- selector and utility helpers already used across the workspace

If you are only writing an application spider, these types are often easier to access through `spider-lib` or `spider-core`.

## Installation

```toml
[dependencies]
spider-util = "0.4.0"
```

## What it contains

- `request`: request model used by spiders, middleware, and downloaders
- `response`: response model returned by downloaders and consumed by parsers
- `item`: `ScrapedItem` and `ParseOutput`
- `error`: `SpiderError` and `PipelineError`
- `selector`: HTML selector helpers
- `util`, `formatters`, `metrics`, and other support modules used across the ecosystem

## Example

```rust,ignore
use spider_util::{error::SpiderError, item::ParseOutput, request::Request};

async fn emit_example(output: &ParseOutput<String>) -> Result<(), SpiderError> {
    let request = Request::try_new("https://example.com")?.with_priority(10);

    output.add_request(request).await?;
    output.add_item("example".to_string()).await?;
    Ok(())
}
```

Requests default to priority `0`. Higher values are scheduled before lower
values, while requests with the same priority remain FIFO.

## You will probably want this crate if

- you are implementing a custom `Downloader`
- you are implementing a custom `Middleware`
- you are implementing a custom `Pipeline`
- you want shared framework types without depending on the application-facing facade

## Related crates

- [`spider-lib`](../README.md)
- [`spider-core`](../spider-core/README.md)
- [`spider-downloader`](../spider-downloader/README.md)
- [`spider-middleware`](../spider-middleware/README.md)
- [`spider-pipeline`](../spider-pipeline/README.md)

## License

MIT. See [LICENSE](../LICENSE).
