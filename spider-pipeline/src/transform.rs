//! Item Pipeline for transforming scraped items.
//!
//! This module provides `TransformPipeline`, which applies declarative
//! transformation operations and custom closures to item JSON payloads.

use crate::pipeline::Pipeline;
use async_trait::async_trait;
use log::{debug, warn};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use spider_util::{error::PipelineError, item::ScrapedItem};
use std::marker::PhantomData;
use std::sync::Arc;

type TransformFn = dyn Fn(&mut Value) -> Result<(), String> + Send + Sync + 'static;

/// Built-in transformation operations for top-level object fields.
#[derive(Debug, Clone)]
pub enum TransformOperation {
    Trim { field: String },
    Lowercase { field: String },
    Uppercase { field: String },
    Rename { from: String, to: String },
    Remove { field: String },
    Set { field: String, value: Value },
    SetDefault { field: String, value: Value },
}

/// A pipeline that transforms items and forwards transformed items downstream.
pub struct TransformPipeline<I>
where
    I: ScrapedItem + Serialize + DeserializeOwned,
{
    operations: Vec<TransformOperation>,
    transforms: Vec<Arc<TransformFn>>,
    _phantom: PhantomData<I>,
}

impl<I> TransformPipeline<I>
where
    I: ScrapedItem + Serialize + DeserializeOwned,
{
    /// Creates a new empty `TransformPipeline`.
    pub fn new() -> Self {
        Self {
            operations: Vec::new(),
            transforms: Vec::new(),
            _phantom: PhantomData,
        }
    }

    /// Adds a built-in transformation operation.
    pub fn with_operation(mut self, operation: TransformOperation) -> Self {
        self.operations.push(operation);
        self
    }

    /// Adds a custom transformation closure.
    pub fn with_transform<F>(mut self, transform: F) -> Self
    where
        F: Fn(&mut Value) -> Result<(), String> + Send + Sync + 'static,
    {
        self.transforms.push(Arc::new(transform));
        self
    }

    fn apply_operation(value: &mut Value, operation: &TransformOperation) -> Result<(), String> {
        let map = value
            .as_object_mut()
            .ok_or_else(|| "Item must be a JSON object for transformation.".to_string())?;

        match operation {
            TransformOperation::Trim { field } => {
                if let Some(raw) = map.get_mut(field) {
                    let text = raw
                        .as_str()
                        .ok_or_else(|| format!("Field '{}' must be a string for Trim.", field))?;
                    *raw = Value::String(text.trim().to_string());
                }
                Ok(())
            }
            TransformOperation::Lowercase { field } => {
                if let Some(raw) = map.get_mut(field) {
                    let text = raw.as_str().ok_or_else(|| {
                        format!("Field '{}' must be a string for Lowercase.", field)
                    })?;
                    *raw = Value::String(text.to_lowercase());
                }
                Ok(())
            }
            TransformOperation::Uppercase { field } => {
                if let Some(raw) = map.get_mut(field) {
                    let text = raw.as_str().ok_or_else(|| {
                        format!("Field '{}' must be a string for Uppercase.", field)
                    })?;
                    *raw = Value::String(text.to_uppercase());
                }
                Ok(())
            }
            TransformOperation::Rename { from, to } => {
                if let Some(value) = map.remove(from) {
                    map.insert(to.clone(), value);
                }
                Ok(())
            }
            TransformOperation::Remove { field } => {
                map.remove(field);
                Ok(())
            }
            TransformOperation::Set { field, value } => {
                map.insert(field.clone(), value.clone());
                Ok(())
            }
            TransformOperation::SetDefault { field, value } => {
                if !map.contains_key(field) {
                    map.insert(field.clone(), value.clone());
                }
                Ok(())
            }
        }
    }
}

impl<I> Default for TransformPipeline<I>
where
    I: ScrapedItem + Serialize + DeserializeOwned,
{
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl<I> Pipeline<I> for TransformPipeline<I>
where
    I: ScrapedItem + Serialize + DeserializeOwned,
{
    fn name(&self) -> &str {
        "TransformPipeline"
    }

    async fn process_item(&self, item: I) -> Result<Option<I>, PipelineError> {
        debug!("TransformPipeline processing item.");

        let mut json = item.to_json_value();

        for operation in &self.operations {
            if let Err(err) = Self::apply_operation(&mut json, operation) {
                warn!("Transform operation failed, dropping item: {}", err);
                return Ok(None);
            }
        }

        for transform in &self.transforms {
            if let Err(err) = transform(&mut json) {
                warn!("Custom transform failed, dropping item: {}", err);
                return Ok(None);
            }
        }

        match serde_json::from_value::<I>(json) {
            Ok(transformed) => Ok(Some(transformed)),
            Err(err) => {
                warn!("Failed to deserialize transformed item, dropping item: {}", err);
                Ok(None)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};
    use serde_json::json;
    use spider_util::item::ScrapedItem;
    use std::any::Any;

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct ProductItem {
        title: String,
        slug: String,
        stock: i32,
    }

    impl ScrapedItem for ProductItem {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn box_clone(&self) -> Box<dyn ScrapedItem + Send + Sync> {
            Box::new(self.clone())
        }

        fn to_json_value(&self) -> Value {
            serde_json::to_value(self).expect("serialize test item")
        }
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct TitleOnlyItem {
        title: String,
    }

    impl ScrapedItem for TitleOnlyItem {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn box_clone(&self) -> Box<dyn ScrapedItem + Send + Sync> {
            Box::new(self.clone())
        }

        fn to_json_value(&self) -> Value {
            serde_json::to_value(self).expect("serialize test item")
        }
    }

    #[tokio::test]
    async fn applies_string_operations() {
        let pipeline = TransformPipeline::<ProductItem>::new()
            .with_operation(TransformOperation::Trim {
                field: "title".to_string(),
            })
            .with_operation(TransformOperation::Lowercase {
                field: "slug".to_string(),
            });

        let out = pipeline
            .process_item(ProductItem {
                title: "  Book  ".to_string(),
                slug: "HELLO-WORLD".to_string(),
                stock: 1,
            })
            .await
            .expect("pipeline should not fail")
            .expect("item should pass");

        assert_eq!(out.title, "Book");
        assert_eq!(out.slug, "hello-world");
    }

    #[tokio::test]
    async fn applies_rename_remove_set_and_default() {
        let pipeline = TransformPipeline::<TitleOnlyItem>::new()
            .with_operation(TransformOperation::Rename {
                from: "title".to_string(),
                to: "title".to_string(),
            })
            .with_operation(TransformOperation::SetDefault {
                field: "title".to_string(),
                value: json!("fallback"),
            })
            .with_operation(TransformOperation::Set {
                field: "title".to_string(),
                value: json!("final"),
            })
            .with_operation(TransformOperation::Remove {
                field: "missing".to_string(),
            });

        let out = pipeline
            .process_item(TitleOnlyItem {
                title: "old".to_string(),
            })
            .await
            .expect("pipeline should not fail")
            .expect("item should pass");

        assert_eq!(out.title, "final");
    }

    #[tokio::test]
    async fn applies_custom_transform() {
        let pipeline = TransformPipeline::<ProductItem>::new().with_transform(|json| {
            let map = json
                .as_object_mut()
                .ok_or_else(|| "object expected".to_string())?;
            map.insert("stock".to_string(), json!(42));
            Ok(())
        });

        let out = pipeline
            .process_item(ProductItem {
                title: "A".to_string(),
                slug: "b".to_string(),
                stock: 0,
            })
            .await
            .expect("pipeline should not fail")
            .expect("item should pass");

        assert_eq!(out.stock, 42);
    }

    #[tokio::test]
    async fn drops_on_deserialize_failure_after_transform() {
        let pipeline = TransformPipeline::<ProductItem>::new().with_transform(|json| {
            let map = json
                .as_object_mut()
                .ok_or_else(|| "object expected".to_string())?;
            map.insert("stock".to_string(), json!("not_a_number"));
            Ok(())
        });

        let out = pipeline
            .process_item(ProductItem {
                title: "A".to_string(),
                slug: "b".to_string(),
                stock: 0,
            })
            .await
            .expect("pipeline should not fail");

        assert!(out.is_none());
    }

    #[tokio::test]
    async fn missing_field_operation_is_noop() {
        let pipeline = TransformPipeline::<ProductItem>::new().with_operation(
            TransformOperation::Uppercase {
                field: "missing".to_string(),
            },
        );

        let out = pipeline
            .process_item(ProductItem {
                title: "A".to_string(),
                slug: "b".to_string(),
                stock: 1,
            })
            .await
            .expect("pipeline should not fail")
            .expect("item should pass");

        assert_eq!(out.title, "A");
        assert_eq!(out.slug, "b");
    }
}
