# spider-util

Shared utility crate for `spider-lib` types and helpers.

This crate underpins the rest of the workspace. It is used internally by all `spider-*` crates and is also the main shared-types crate for extension authors building custom downloaders, middleware, or pipelines.

## When to Use This Crate Directly

Use `spider-util` if you need:

- shared request and response models
- `ScrapedItem` and `ParseOutput`
- framework error types such as `SpiderError` and `PipelineError`
- helper modules used across custom runtime extensions

If you are just writing a spider application, you will usually reach these types through `spider-lib` or `spider-core`.

## Installation

```toml
[dependencies]
spider-util = "0.3.4"
```

## What It Provides

Core modules and common use cases:

- `request`: request model used by spiders, middleware, and downloaders
- `response`: response model returned by downloaders and consumed by parsers
- `item`: `ScrapedItem` and `ParseOutput`
- `error`: `SpiderError` and `PipelineError`
- `selector` and HTML helpers: utilities for parsing HTML content
- normalization and helper modules: reusable utility functions across the runtime

## Usage

```rust,ignore
use spider_util::{item::ParseOutput, request::Request};
use url::Url;

let request = Request::new(Url::parse("https://example.com")?);
let mut output = ParseOutput::<String>::new();

output.add_request(request);
output.add_item("example".to_string());
```

## You Likely Need This Crate If

- you are implementing `Downloader`
- you are implementing `Middleware`
- you are implementing `Pipeline`
- you want framework-native request, response, item, or error types without the full facade crate

## Custom Extension Entry Points

Build custom runtime components with:

- [`spider-downloader`](../spider-downloader/README.md) for `Downloader`
- [`spider-middleware`](../spider-middleware/README.md) for `Middleware`
- [`spider-pipeline`](../spider-pipeline/README.md) for `Pipeline`

## Common Gotchas

- `spider-util` is a shared types crate, not a full crawler runtime.
- Many users do not need to depend on it directly if they already use `spider-lib`.
- It becomes most useful when writing reusable extensions against the lower-level ecosystem.

## Related Crates

- [`spider-lib`](../README.md)
- [`spider-core`](../spider-core/README.md)
- [`spider-downloader`](../spider-downloader/README.md)

## License

MIT. See [LICENSE](../LICENSE).
