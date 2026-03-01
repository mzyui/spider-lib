# spider-macro

Provides procedural macros for the `spider-lib` framework to reduce boilerplate code.

## Overview

The `spider-macro` crate contains procedural macros that automate the implementation of common traits and patterns used in the spider framework. These macros significantly reduce the amount of boilerplate code required when defining custom data structures for scraped items.

## Key Macros

- **`#[scraped_item]`**: Derives the `ScrapedItem` trait along with necessary implementations for serialization, deserialization, cloning, and type conversions. This macro automatically implements all required traits for a struct to be used as a scraped item in the framework.

## Features

- **Automatic Trait Derivation**: Implements `Serialize`, `Deserialize`, `Clone`, and `Debug` traits automatically
- **ScrapedItem Implementation**: Provides the complete implementation of the `ScrapedItem` trait required by the framework
- **Type Safety**: Maintains type safety while reducing boilerplate
- **Performance**: Generates efficient code without runtime overhead

## Usage

```rust
use spider_macro::scraped_item;

#[scraped_item]
struct Article {
    title: String,
    content: String,
    author: String,
    published_date: String,
}

// The macro generates all necessary implementations automatically
// including serialization, deserialization, and the ScrapedItem trait
```

## How It Works

The `#[scraped_item]` macro generates:

- `#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]` annotations
- Complete implementation of the `ScrapedItem` trait with:
  - `as_any()` method for type erasure
  - `box_clone()` method for creating boxed clones
  - `to_json_value()` method for converting to JSON values

## License

This project is licensed under the MIT License - see the [LICENSE](./LICENSE) file for details.
