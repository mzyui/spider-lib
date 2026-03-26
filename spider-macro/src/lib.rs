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
//! spider-macro = "0.1.12"
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
use syn::{Fields, ItemStruct, Type, parse_macro_input};

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
    let item_name = name.to_string();
    let fields = match &ast.fields {
        Fields::Named(fields) => fields.named.iter().collect::<Vec<_>>(),
        _ => {
            return syn::Error::new_spanned(
                &ast,
                "#[scraped_item] only supports structs with named fields",
            )
            .to_compile_error()
            .into();
        }
    };

    let schema_fields = fields.iter().map(|field| {
        let field_ident = field.ident.as_ref().expect("named field");
        let field_name = field_ident.to_string();
        let rust_type = quote!(#field.ty).to_string().replace(' ', "");
        let nullable = is_option_type(&field.ty);
        let value_type_tokens = field_value_type_tokens(&field.ty);

        quote! {
            ::spider_util::item::ItemFieldSchema {
                name: #field_name.to_string(),
                rust_type: #rust_type.to_string(),
                value_type: #value_type_tokens,
                nullable: #nullable,
            }
        }
    });

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

            fn item_schema(&self) -> ::std::option::Option<::spider_util::item::ItemSchema> {
                ::std::option::Option::Some(<Self as ::spider_util::item::TypedItemSchema>::schema())
            }

            fn item_schema_version(&self) -> u32 {
                <Self as ::spider_util::item::TypedItemSchema>::schema_version()
            }
        }

        impl ::spider_util::item::TypedItemSchema for #name {
            fn schema() -> ::spider_util::item::ItemSchema {
                ::spider_util::item::ItemSchema {
                    item_name: #item_name.to_string(),
                    version: Self::schema_version(),
                    fields: vec![#(#schema_fields),*],
                }
            }
        }
    };

    TokenStream::from(expanded)
}

fn is_option_type(ty: &Type) -> bool {
    match ty {
        Type::Path(type_path) => type_path
            .path
            .segments
            .last()
            .map(|segment| segment.ident == "Option")
            .unwrap_or(false),
        _ => false,
    }
}

fn field_value_type_tokens(ty: &Type) -> proc_macro2::TokenStream {
    let core_ty = unwrap_option_type(ty).unwrap_or(ty);

    match core_ty {
        Type::Path(type_path) => {
            let segment = match type_path.path.segments.last() {
                Some(segment) => segment,
                None => {
                    return quote!(::spider_util::item::FieldValueType::Unknown);
                }
            };
            let ident = segment.ident.to_string();
            match ident.as_str() {
                "bool" => quote!(::spider_util::item::FieldValueType::Bool),
                "String" | "str" => quote!(::spider_util::item::FieldValueType::String),
                "i8" | "i16" | "i32" | "i64" | "i128" | "isize" | "u8" | "u16" | "u32" | "u64"
                | "u128" | "usize" => quote!(::spider_util::item::FieldValueType::Integer),
                "f32" | "f64" => quote!(::spider_util::item::FieldValueType::Float),
                "Vec" | "VecDeque" | "HashSet" | "BTreeSet" => {
                    quote!(::spider_util::item::FieldValueType::Sequence)
                }
                "HashMap" | "BTreeMap" => quote!(::spider_util::item::FieldValueType::Map),
                "Value" => quote!(::spider_util::item::FieldValueType::Json),
                _ => quote!(::spider_util::item::FieldValueType::Unknown),
            }
        }
        Type::Array(_) | Type::Slice(_) => quote!(::spider_util::item::FieldValueType::Sequence),
        Type::Tuple(_) => quote!(::spider_util::item::FieldValueType::Sequence),
        _ => quote!(::spider_util::item::FieldValueType::Unknown),
    }
}

fn unwrap_option_type(ty: &Type) -> Option<&Type> {
    let Type::Path(type_path) = ty else {
        return None;
    };
    let segment = type_path.path.segments.last()?;
    if segment.ident != "Option" {
        return None;
    }

    let syn::PathArguments::AngleBracketed(args) = &segment.arguments else {
        return None;
    };
    let Some(syn::GenericArgument::Type(inner)) = args.args.first() else {
        return None;
    };
    Some(inner)
}
