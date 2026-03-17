# spider-macro

Procedural macros for `spider-lib`.

## Installation

```toml
[dependencies]
spider-macro = "0.1.10"
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"
```

`serde` and `serde_json` must be direct dependencies when using `#[scraped_item]`.

## Macros

- `#[scraped_item]`: derives `Serialize`, `Deserialize`, `Clone`, `Debug`, and implements `ScrapedItem`.

## Usage

```rust,ignore
use spider_macro::scraped_item;

#[scraped_item]
struct Product {
    name: String,
    price: f64,
}
```

Most users can access this macro via `spider-lib` prelude:

```rust,ignore
use spider_lib::prelude::*;
```

For custom downloader, middleware, and pipeline implementations, use:

- [`spider-downloader`](../spider-downloader/README.md)
- [`spider-middleware`](../spider-middleware/README.md)
- [`spider-pipeline`](../spider-pipeline/README.md)

## Related Crates

- [`spider-lib`](../README.md)
- [`spider-core`](../spider-core/README.md)
- [`spider-util`](../spider-util/README.md)

## License

MIT. See [LICENSE](../LICENSE).
