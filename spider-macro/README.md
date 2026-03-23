# spider-macro

`spider-macro` contains the procedural macros used by the rest of the workspace. Right now the main one is `#[scraped_item]`, which is the small convenience macro most spiders end up using.

The job of this crate is straightforward: keep scraped item definitions short and remove the repetitive trait impls you would otherwise have to write by hand.

## When to depend on it directly

Use `spider-macro` directly if you want the macro without bringing in the root facade crate.

If you are already using `spider-lib`, the usual path is still:

```rust,ignore
use spider_lib::prelude::*;
```

## Installation

```toml
[dependencies]
spider-macro = "0.1.12"
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"
```

`serde` and `serde_json` need to be direct dependencies in the downstream crate.

## `#[scraped_item]`

Apply the attribute to a struct and it will generate:

- `Serialize`
- `Deserialize`
- `Clone`
- `Debug`
- an implementation of `ScrapedItem`

That is the item contract expected by pipelines and the rest of the `spider-*` ecosystem.

## Example

```rust,ignore
use spider_macro::scraped_item;

#[scraped_item]
struct Product {
    name: String,
    price: f64,
}
```

The same macro is available through the facade prelude:

```rust,ignore
use spider_lib::prelude::*;

#[scraped_item]
struct Product {
    name: String,
    price: f64,
}
```

## What it saves you from

Without the macro, every item type would need manual serde derives plus a hand-written `ScrapedItem` implementation. That is not hard, but it gets old quickly once a project has more than a few item structs.

## Good to know

- The macro is for structs.
- Missing direct `serde` or `serde_json` dependencies will break downstream compilation.
- If you are writing a normal spider app, importing the macro through [`spider-lib`](../README.md) is usually the least surprising setup.

## Related crates

- [`spider-lib`](../README.md)
- [`spider-core`](../spider-core/README.md)
- [`spider-util`](../spider-util/README.md)

## License

MIT. See [LICENSE](../LICENSE).
