# spider-util

Shared utility crate for `spider-lib` types and helpers.

This crate is used internally by all workspace crates and is also suitable for custom extensions built on top of the ecosystem.

## Installation

```toml
[dependencies]
spider-util = "0.3.3"
```

## What It Provides

- Request and response models: `request`, `response`
- Core item types: `item::ScrapedItem`, `item::ParseOutput`
- Error types: `error::SpiderError`, `error::PipelineError`
- Helpers: selectors, normalization utilities, metrics, bloom filter, and general helpers

## Usage

```rust,ignore
use spider_util::{item::ParseOutput, request::Request};
use url::Url;

let _request = Request::new(Url::parse("https://example.com")?);
let mut output = ParseOutput::<String>::new();
output.add_item("example".to_string());
```

## Related Crates

- [`spider-lib`](../README.md)
- [`spider-core`](../spider-core/README.md)
- [`spider-downloader`](../spider-downloader/README.md)

## License

MIT. See [LICENSE](../LICENSE).
