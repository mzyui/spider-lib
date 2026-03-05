# spider-util

Shared utility crate for `spider-lib` types and helpers.

## Install

```toml
[dependencies]
spider-util = "0.3.2"
```

## What It Provides

- Request/response models: `request`, `response`
- Core data types: `item::ScrapedItem`, `item::ParseOutput`
- Error types: `error::SpiderError`, `error::PipelineError`
- Helpers: selectors, formatters, metrics, bloom filter, utility functions

## Usage

```rust
use spider_util::{item::ParseOutput, request::Request};

let mut output = ParseOutput::<String>::new();
output.add_item("example".to_string());
```

This crate is used internally by all workspace crates and can also be used directly for custom extensions.

## License

MIT. See [LICENSE](./LICENSE).
