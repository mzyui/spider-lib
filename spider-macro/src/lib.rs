//! # spider-macro
//!
//! Procedural macros used by the `spider-*` workspace.
//!
//! Right now this crate is intentionally small: it mainly provides
//! [`scraped_item`], the attribute macro used to turn plain structs into item
//! types that fit the crawler and pipeline APIs.
//!
//! ## Dependencies
//!
//! ```toml
//! [dependencies]
//! spider-macro = "0.1.11"
//! serde = { version = "1.0", features = ["derive"] }
//! serde_json = "1.0"
//! ```
//!
//! ## Example
//!
//! ```rust,ignore
//! use spider_macro::scraped_item;
//!
//! #[scraped_item]
//! struct Article {
//!     title: String,
//!     content: String,
//! }
//!
//! // `Article` now implements Serialize, Deserialize, Clone, Debug,
//! // and the ScrapedItem trait expected by the rest of the workspace.
//! ```

extern crate proc_macro;

use proc_macro::TokenStream;
use quote::quote;
use syn::{ItemStruct, parse_macro_input};

/// Attribute macro for defining a scraped item type.
///
/// This macro:
/// 1. Implements `ScrapedItem`
/// 2. Adds `Serialize` and `Deserialize`
/// 3. Adds `Clone` and `Debug`
///
/// # Dependencies
///
/// Your project must include `serde` and `serde_json` as direct dependencies:
///
/// ```toml
/// [dependencies]
/// serde = { version = "1.0", features = ["derive"] }
/// serde_json = "1.0"
/// ```
#[proc_macro_attribute]
pub fn scraped_item(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(item as ItemStruct);
    let name = &ast.ident;

    let expanded = quote! {
        #[derive(
            ::serde::Serialize,
            ::serde::Deserialize,
            Clone,
            Debug
        )]
        #ast

        impl ScrapedItem for #name {
            fn as_any(&self) -> &dyn ::std::any::Any {
                self
            }

            fn box_clone(&self) -> Box<dyn ScrapedItem + Send + Sync> {
                Box::new(self.clone())
            }

            fn to_json_value(&self) -> ::serde_json::Value {
                match ::serde_json::to_value(self) {
                    Ok(value) => value,
                    Err(err) => panic!("failed to serialize ScrapedItem '{}': {}", stringify!(#name), err),
                }
            }
        }
    };

    TokenStream::from(expanded)
}
