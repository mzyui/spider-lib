//! Item Pipeline for validating scraped items.
//!
//! This module provides `ValidationPipeline`, a configurable pipeline that
//! validates items using declarative field rules and custom validator closures.

use crate::pipeline::Pipeline;
use async_trait::async_trait;
use log::{debug, warn};
use serde_json::Value;
use spider_util::{error::PipelineError, item::ScrapedItem};
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;

type ValidatorFn<I> = dyn Fn(&I, &Value) -> Result<(), String> + Send + Sync + 'static;

/// JSON value type matcher for field validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JsonType {
    Null,
    Bool,
    Number,
    String,
    Array,
    Object,
}

/// Declarative rules for validating fields in an item.
#[derive(Debug, Clone)]
pub enum ValidationRule {
    Required,
    NonEmptyString,
    Type(JsonType),
    MinLen(usize),
    MaxLen(usize),
    MinNumber(f64),
    MaxNumber(f64),
}

/// A pipeline that validates items and drops invalid entries.
pub struct ValidationPipeline<I: ScrapedItem> {
    rules: HashMap<String, Vec<ValidationRule>>,
    validators: Vec<Arc<ValidatorFn<I>>>,
    _phantom: PhantomData<I>,
}

impl<I: ScrapedItem> ValidationPipeline<I> {
    /// Creates a new empty `ValidationPipeline`.
    pub fn new() -> Self {
        Self {
            rules: HashMap::new(),
            validators: Vec::new(),
            _phantom: PhantomData,
        }
    }

    /// Adds a field rule for the given top-level field name.
    pub fn with_rule(mut self, field: impl Into<String>, rule: ValidationRule) -> Self {
        self.rules.entry(field.into()).or_default().push(rule);
        self
    }

    /// Adds a custom validator closure.
    pub fn with_validator<F>(mut self, validator: F) -> Self
    where
        F: Fn(&I, &Value) -> Result<(), String> + Send + Sync + 'static,
    {
        self.validators.push(Arc::new(validator));
        self
    }

    fn validate_type(value: &Value, expected: &JsonType) -> bool {
        match expected {
            JsonType::Null => value.is_null(),
            JsonType::Bool => value.is_boolean(),
            JsonType::Number => value.is_number(),
            JsonType::String => value.is_string(),
            JsonType::Array => value.is_array(),
            JsonType::Object => value.is_object(),
        }
    }

    fn validate_item(&self, json: &Value) -> Result<(), String> {
        let map = json
            .as_object()
            .ok_or_else(|| "Item must be a JSON object for validation.".to_string())?;

        for (field, rules) in &self.rules {
            let value = map.get(field);
            for rule in rules {
                match rule {
                    ValidationRule::Required => {
                        if value.is_none() {
                            return Err(format!("Missing required field '{}'.", field));
                        }
                    }
                    ValidationRule::NonEmptyString => {
                        if let Some(v) = value {
                            match v.as_str() {
                                Some(s) if !s.trim().is_empty() => {}
                                Some(_) => {
                                    return Err(format!(
                                        "Field '{}' must be a non-empty string.",
                                        field
                                    ));
                                }
                                None => {
                                    return Err(format!("Field '{}' must be a string.", field));
                                }
                            }
                        }
                    }
                    ValidationRule::Type(expected) => {
                        if let Some(v) = value
                            && !Self::validate_type(v, expected)
                        {
                            return Err(format!(
                                "Field '{}' has invalid type. Expected {:?}.",
                                field, expected
                            ));
                        }
                    }
                    ValidationRule::MinLen(min) => {
                        if let Some(v) = value {
                            if let Some(s) = v.as_str() {
                                if s.len() < *min {
                                    return Err(format!(
                                        "Field '{}' length {} is less than {}.",
                                        field,
                                        s.len(),
                                        min
                                    ));
                                }
                            } else if let Some(arr) = v.as_array() {
                                if arr.len() < *min {
                                    return Err(format!(
                                        "Field '{}' array length {} is less than {}.",
                                        field,
                                        arr.len(),
                                        min
                                    ));
                                }
                            } else {
                                return Err(format!(
                                    "Field '{}' must be string or array for MinLen.",
                                    field
                                ));
                            }
                        }
                    }
                    ValidationRule::MaxLen(max) => {
                        if let Some(v) = value {
                            if let Some(s) = v.as_str() {
                                if s.len() > *max {
                                    return Err(format!(
                                        "Field '{}' length {} is greater than {}.",
                                        field,
                                        s.len(),
                                        max
                                    ));
                                }
                            } else if let Some(arr) = v.as_array() {
                                if arr.len() > *max {
                                    return Err(format!(
                                        "Field '{}' array length {} is greater than {}.",
                                        field,
                                        arr.len(),
                                        max
                                    ));
                                }
                            } else {
                                return Err(format!(
                                    "Field '{}' must be string or array for MaxLen.",
                                    field
                                ));
                            }
                        }
                    }
                    ValidationRule::MinNumber(min) => {
                        if let Some(v) = value {
                            let num = v.as_f64().ok_or_else(|| {
                                format!("Field '{}' must be numeric for MinNumber.", field)
                            })?;
                            if num < *min {
                                return Err(format!(
                                    "Field '{}' number {} is less than {}.",
                                    field, num, min
                                ));
                            }
                        }
                    }
                    ValidationRule::MaxNumber(max) => {
                        if let Some(v) = value {
                            let num = v.as_f64().ok_or_else(|| {
                                format!("Field '{}' must be numeric for MaxNumber.", field)
                            })?;
                            if num > *max {
                                return Err(format!(
                                    "Field '{}' number {} is greater than {}.",
                                    field, num, max
                                ));
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

impl<I: ScrapedItem> Default for ValidationPipeline<I> {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl<I: ScrapedItem> Pipeline<I> for ValidationPipeline<I> {
    fn name(&self) -> &str {
        "ValidationPipeline"
    }

    async fn process_item(&self, item: I) -> Result<Option<I>, PipelineError> {
        debug!("ValidationPipeline processing item.");
        let json = item.to_json_value();

        if let Err(err) = self.validate_item(&json) {
            warn!("Validation failed, dropping item: {}", err);
            return Ok(None);
        }

        for validator in &self.validators {
            if let Err(err) = validator(&item, &json) {
                warn!("Custom validation failed, dropping item: {}", err);
                return Ok(None);
            }
        }

        Ok(Some(item))
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
    struct TestItem {
        title: String,
        price: f64,
    }

    impl ScrapedItem for TestItem {
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
    async fn passes_valid_item() {
        let pipeline = ValidationPipeline::<TestItem>::new()
            .with_rule("title", ValidationRule::Required)
            .with_rule("title", ValidationRule::NonEmptyString)
            .with_rule("price", ValidationRule::MinNumber(1.0))
            .with_rule("price", ValidationRule::MaxNumber(100.0));

        let item = TestItem {
            title: "Book".to_string(),
            price: 20.0,
        };

        let out = pipeline
            .process_item(item)
            .await
            .expect("pipeline should not fail");
        assert!(out.is_some());
    }

    #[tokio::test]
    async fn drops_missing_required_field() {
        let pipeline = ValidationPipeline::<TestItem>::new()
            .with_rule("missing", ValidationRule::Required);
        let item = TestItem {
            title: "Book".to_string(),
            price: 20.0,
        };

        let out = pipeline
            .process_item(item)
            .await
            .expect("pipeline should not fail");
        assert!(out.is_none());
    }

    #[tokio::test]
    async fn drops_on_custom_validator_error() {
        let pipeline = ValidationPipeline::<TestItem>::new()
            .with_validator(|_item, json| match json.get("title").and_then(Value::as_str) {
                Some("Book") => Ok(()),
                _ => Err("title mismatch".to_string()),
            });

        let item = TestItem {
            title: "Other".to_string(),
            price: 20.0,
        };

        let out = pipeline
            .process_item(item)
            .await
            .expect("pipeline should not fail");
        assert!(out.is_none());
    }

    #[tokio::test]
    async fn drops_on_invalid_type_rule() {
        let pipeline = ValidationPipeline::<TestItem>::new()
            .with_rule("title", ValidationRule::Type(JsonType::Number));
        let item = TestItem {
            title: "Book".to_string(),
            price: 20.0,
        };

        let out = pipeline
            .process_item(item)
            .await
            .expect("pipeline should not fail");
        assert!(out.is_none());
    }

    #[tokio::test]
    async fn handles_multiple_rules() {
        let pipeline = ValidationPipeline::<TestItem>::new()
            .with_rule("title", ValidationRule::MinLen(2))
            .with_rule("title", ValidationRule::MaxLen(10))
            .with_validator(|_, _| Ok(()));
        let item = TestItem {
            title: "ok".to_string(),
            price: 5.0,
        };
        let out = pipeline
            .process_item(item)
            .await
            .expect("pipeline should not fail");
        assert_eq!(out.expect("item should pass").to_json_value(), json!({"title":"ok","price":5.0}));
    }
}
