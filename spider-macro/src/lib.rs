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
//! spider-macro = "0.1.13"
//! spider-util = "0.4.0"
//! serde = { version = "1.0", features = ["derive"] }
//! serde_json = "1.0"
//! ```
//!
//! ## Example
//!
//! ```rust,ignore
//! use spider_macro::scraped_item;
//! use spider_util::item::ScrapedItem;
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
use proc_macro_crate::{FoundCrate, crate_name};
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
/// spider-util = "0.4.0"
/// serde = { version = "1.0", features = ["derive"] }
/// serde_json = "1.0"
/// ```
#[proc_macro_attribute]
pub fn scraped_item(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(item as ItemStruct);
    let name = &ast.ident;
    let item_name = name.to_string();
    let scraped_item_trait = item_type_tokens("ScrapedItem");
    let item_field_schema = item_type_tokens("ItemFieldSchema");
    let item_schema = item_type_tokens("ItemSchema");
    let typed_item_schema = item_type_tokens("TypedItemSchema");
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
            #item_field_schema {
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

        impl #scraped_item_trait for #name {
            fn as_any(&self) -> &dyn ::std::any::Any {
                self
            }

            fn box_clone(&self) -> Box<dyn #scraped_item_trait + Send + Sync> {
                Box::new(self.clone())
            }

            fn to_json_value(&self) -> ::serde_json::Value {
                match ::serde_json::to_value(self) {
                    Ok(value) => value,
                    Err(err) => panic!("failed to serialize ScrapedItem '{}': {}", stringify!(#name), err),
                }
            }

            fn item_schema(&self) -> ::std::option::Option<#item_schema> {
                ::std::option::Option::Some(<Self as #typed_item_schema>::schema())
            }

            fn item_schema_version(&self) -> u32 {
                <Self as #typed_item_schema>::schema_version()
            }
        }

        impl #typed_item_schema for #name {
            fn schema() -> #item_schema {
                #item_schema {
                    item_name: #item_name.to_string(),
                    version: Self::schema_version(),
                    fields: vec![#(#schema_fields),*],
                }
            }
        }
    };

    TokenStream::from(expanded)
}

fn item_type_tokens(type_name: &str) -> proc_macro2::TokenStream {
    let ident = syn::Ident::new(type_name, proc_macro2::Span::call_site());

    match runtime_crate() {
        RuntimeCrate::SpiderLib(path) => quote!(#path::#ident),
        RuntimeCrate::SpiderUtil(path) => quote!(#path::item::#ident),
    }
}

fn runtime_crate() -> RuntimeCrate {
    if let Some(path) = facade_crate_tokens("spider-lib", true) {
        return RuntimeCrate::SpiderLib(path);
    }

    if let Some(path) = facade_crate_tokens("spider-util", false) {
        return RuntimeCrate::SpiderUtil(path);
    }

    RuntimeCrate::SpiderUtil(
        syn::Error::new(
            proc_macro2::Span::call_site(),
            "#[scraped_item] requires either `spider-lib` or `spider-util` as a dependency",
        )
        .to_compile_error(),
    )
}

fn facade_crate_tokens(crate_key: &str, use_prelude: bool) -> Option<proc_macro2::TokenStream> {
    let found = crate_name(crate_key).ok()?;

    Some(match found {
        FoundCrate::Itself => {
            let crate_name = crate_key.replace('-', "_");
            let ident = syn::Ident::new(&crate_name, proc_macro2::Span::call_site());
            if use_prelude {
                quote!(::#ident::prelude)
            } else {
                quote!(::#ident)
            }
        }
        FoundCrate::Name(name) => {
            let ident = syn::Ident::new(&name, proc_macro2::Span::call_site());
            if use_prelude {
                quote!(::#ident::prelude)
            } else {
                quote!(::#ident)
            }
        }
    })
}

enum RuntimeCrate {
    SpiderLib(proc_macro2::TokenStream),
    SpiderUtil(proc_macro2::TokenStream),
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
    let field_value_type = item_type_tokens("FieldValueType");
    let core_ty = unwrap_option_type(ty).unwrap_or(ty);

    match core_ty {
        Type::Path(type_path) => {
            let segment = match type_path.path.segments.last() {
                Some(segment) => segment,
                None => {
                    return quote!(#field_value_type::Unknown);
                }
            };
            let ident = segment.ident.to_string();
            match ident.as_str() {
                "bool" => quote!(#field_value_type::Bool),
                "String" | "str" => quote!(#field_value_type::String),
                "i8" | "i16" | "i32" | "i64" | "i128" | "isize" | "u8" | "u16" | "u32" | "u64"
                | "u128" | "usize" => quote!(#field_value_type::Integer),
                "f32" | "f64" => quote!(#field_value_type::Float),
                "Vec" | "VecDeque" | "HashSet" | "BTreeSet" => {
                    quote!(#field_value_type::Sequence)
                }
                "HashMap" | "BTreeMap" => quote!(#field_value_type::Map),
                "Value" => quote!(#field_value_type::Json),
                _ => quote!(#field_value_type::Unknown),
            }
        }
        Type::Array(_) | Type::Slice(_) => quote!(#field_value_type::Sequence),
        Type::Tuple(_) => quote!(#field_value_type::Sequence),
        _ => quote!(#field_value_type::Unknown),
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
