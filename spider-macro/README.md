# spider-macro

Procedural macros for `spider-lib`.

## Install

```toml
[dependencies]
spider-macro = "0.1.10"
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"
```

`serde` and `serde_json` must be direct dependencies when using `#[scraped_item]`.

## Macro

- `#[scraped_item]`: derives `Serialize`, `Deserialize`, `Clone`, `Debug`, and implements `ScrapedItem`.

## Usage

```rust
use spider_macro::scraped_item;

#[scraped_item]
struct Product {
    name: String,
    price: f64,
}
```

Most users can access this macro from `spider-lib` prelude:

```rust
use spider_lib::prelude::*;
```

## License

MIT. See [LICENSE](./LICENSE).
