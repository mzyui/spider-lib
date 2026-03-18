# spider-macro

Procedural macros for `spider-lib`.

This crate currently provides `#[scraped_item]`, a small macro that removes boilerplate when defining item types emitted by spiders and consumed by pipelines.

## When to Use This Crate Directly

Use `spider-macro` directly if you want to import the macro without depending on the facade crate. Most users can instead access the same macro through `spider_lib::prelude::*`.

## Installation

```toml
[dependencies]
spider-macro = "0.1.11"
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"
```

`serde` and `serde_json` must be direct dependencies when using `#[scraped_item]`.

## Macro Reference

### `#[scraped_item]`

Applies to a struct and generates:

- `Serialize`
- `Deserialize`
- `Clone`
- `Debug`
- an implementation of `ScrapedItem`

This is the expected item trait used by the rest of the `spider-*` ecosystem.

## Usage

```rust,ignore
use spider_macro::scraped_item;

#[scraped_item]
struct Product {
    name: String,
    price: f64,
}
```

Most applications can import it from the facade crate instead:

```rust,ignore
use spider_lib::prelude::*;

#[scraped_item]
struct Product {
    name: String,
    price: f64,
}
```

## What the Macro Saves You From

Without `#[scraped_item]`, you would need to derive serde traits manually and implement the `ScrapedItem` contract yourself. The macro keeps item definitions short and consistent across spiders, middleware, and pipelines.

## Common Gotchas

- The macro only applies to structs.
- Missing direct `serde` or `serde_json` dependencies will cause compile errors in downstream crates.
- If you are already using `spider-lib`, importing from the prelude is usually the least surprising option.

## Related Crates

- [`spider-lib`](../README.md)
- [`spider-core`](../spider-core/README.md)
- [`spider-util`](../spider-util/README.md)

## License

MIT. See [LICENSE](../LICENSE).
