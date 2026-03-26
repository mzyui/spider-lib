//! Schema-aware item workflows and export helpers.

use crate::pipeline::Pipeline;
use async_trait::async_trait;
use serde_json::{Map, Value};
use spider_util::error::PipelineError;
use spider_util::item::{
    FieldValueType, ItemFieldSchema, ItemSchema, ScrapedItem, TypedItemSchema,
};
use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::sync::Arc;

type SchemaValidatorFn<I> = dyn Fn(&I, &ItemSchema, &Value) -> Result<(), String> + Send + Sync;
type SchemaTransformFn<I> = dyn Fn(I) -> Result<I, String> + Send + Sync;

/// Validation failure details for schema-aware pipelines.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaViolation {
    pub field: Option<String>,
    pub message: String,
}

/// Export configuration derived from typed item schema metadata.
#[derive(Debug, Clone, Default)]
pub struct SchemaExportConfig {
    field_aliases: BTreeMap<String, String>,
    schema_version_field: Option<String>,
    inject_nulls_for_missing_optional: bool,
}

impl SchemaExportConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_field_alias(
        mut self,
        field_name: impl Into<String>,
        export_name: impl Into<String>,
    ) -> Self {
        self.field_aliases
            .insert(field_name.into(), export_name.into());
        self
    }

    pub fn with_schema_version_field(mut self, field_name: impl Into<String>) -> Self {
        self.schema_version_field = Some(field_name.into());
        self
    }

    pub fn inject_nulls_for_missing_optional(mut self, enabled: bool) -> Self {
        self.inject_nulls_for_missing_optional = enabled;
        self
    }

    pub fn export_name_for<'a>(&'a self, field_name: &'a str) -> &'a str {
        self.field_aliases
            .get(field_name)
            .map(String::as_str)
            .unwrap_or(field_name)
    }
}

/// Schema-aware validation for typed items.
pub struct SchemaValidationPipeline<I: ScrapedItem + TypedItemSchema> {
    validators: Vec<Arc<SchemaValidatorFn<I>>>,
    expected_schema_version: Option<u32>,
    _phantom: PhantomData<I>,
}

impl<I: ScrapedItem + TypedItemSchema> SchemaValidationPipeline<I> {
    pub fn new() -> Self {
        Self {
            validators: Vec::new(),
            expected_schema_version: None,
            _phantom: PhantomData,
        }
    }

    pub fn expect_schema_version(mut self, version: u32) -> Self {
        self.expected_schema_version = Some(version);
        self
    }

    pub fn with_validator<F>(mut self, validator: F) -> Self
    where
        F: Fn(&I, &ItemSchema, &Value) -> Result<(), String> + Send + Sync + 'static,
    {
        self.validators.push(Arc::new(validator));
        self
    }
}

impl<I: ScrapedItem + TypedItemSchema> Default for SchemaValidationPipeline<I> {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl<I: ScrapedItem + TypedItemSchema> Pipeline<I> for SchemaValidationPipeline<I> {
    fn name(&self) -> &str {
        "SchemaValidationPipeline"
    }

    async fn process_item(&self, item: I) -> Result<Option<I>, PipelineError> {
        let schema = I::schema();
        if let Some(expected_version) = self.expected_schema_version
            && schema.version != expected_version
        {
            return Ok(None);
        }

        let json = item.to_json_value();
        if validate_value_against_schema(&schema, &json).is_err() {
            return Ok(None);
        }

        for validator in &self.validators {
            if validator(&item, &schema, &json).is_err() {
                return Ok(None);
            }
        }

        Ok(Some(item))
    }
}

/// Typed transform pipeline for item-to-item transforms before export.
pub struct SchemaTransformPipeline<I: ScrapedItem + TypedItemSchema> {
    transforms: Vec<Arc<SchemaTransformFn<I>>>,
    _phantom: PhantomData<I>,
}

impl<I: ScrapedItem + TypedItemSchema> SchemaTransformPipeline<I> {
    pub fn new() -> Self {
        Self {
            transforms: Vec::new(),
            _phantom: PhantomData,
        }
    }

    pub fn with_transform<F>(mut self, transform: F) -> Self
    where
        F: Fn(I) -> Result<I, String> + Send + Sync + 'static,
    {
        self.transforms.push(Arc::new(transform));
        self
    }
}

impl<I: ScrapedItem + TypedItemSchema> Default for SchemaTransformPipeline<I> {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl<I: ScrapedItem + TypedItemSchema> Pipeline<I> for SchemaTransformPipeline<I> {
    fn name(&self) -> &str {
        "SchemaTransformPipeline"
    }

    async fn process_item(&self, mut item: I) -> Result<Option<I>, PipelineError> {
        for transform in &self.transforms {
            item = transform(item).map_err(PipelineError::ItemError)?;
        }

        Ok(Some(item))
    }
}

pub fn export_schema_for_item<I: ScrapedItem>(
    item: &I,
    config: Option<&SchemaExportConfig>,
) -> Option<Vec<ItemFieldSchema>> {
    let schema = item.item_schema()?;
    Some(
        schema
            .fields
            .iter()
            .map(|field| ItemFieldSchema {
                name: config
                    .map(|cfg| cfg.export_name_for(&field.name).to_string())
                    .unwrap_or_else(|| field.name.clone()),
                rust_type: field.rust_type.clone(),
                value_type: field.value_type.clone(),
                nullable: field.nullable,
            })
            .collect(),
    )
}

pub fn map_item_for_export<I: ScrapedItem>(item: &I, config: Option<&SchemaExportConfig>) -> Value {
    let raw = item.to_json_value();
    let Some(schema) = item.item_schema() else {
        return raw;
    };
    let Some(source) = raw.as_object() else {
        return raw;
    };

    let mut output = Map::new();
    for field in &schema.fields {
        let export_name = config
            .map(|cfg| cfg.export_name_for(&field.name).to_string())
            .unwrap_or_else(|| field.name.clone());
        match source.get(&field.name) {
            Some(value) => {
                output.insert(export_name, value.clone());
            }
            None if field.nullable
                && config
                    .map(|cfg| cfg.inject_nulls_for_missing_optional)
                    .unwrap_or(false) =>
            {
                output.insert(export_name, Value::Null);
            }
            None => {}
        }
    }

    if let Some(version_field) = config.and_then(|cfg| cfg.schema_version_field.as_ref()) {
        output.insert(
            version_field.clone(),
            Value::from(item.item_schema_version()),
        );
    }

    Value::Object(output)
}

pub fn sqlite_type_for_field(field: &ItemFieldSchema) -> &'static str {
    match field.value_type {
        FieldValueType::Bool | FieldValueType::Integer => "INTEGER",
        FieldValueType::Float => "REAL",
        FieldValueType::String
        | FieldValueType::Json
        | FieldValueType::Sequence
        | FieldValueType::Map
        | FieldValueType::Unknown => "TEXT",
    }
}

fn validate_value_against_schema(schema: &ItemSchema, json: &Value) -> Result<(), SchemaViolation> {
    let map = json.as_object().ok_or_else(|| SchemaViolation {
        field: None,
        message: "Item must serialize to a JSON object for schema validation.".to_string(),
    })?;

    for field in &schema.fields {
        let value = map.get(&field.name);
        if value.is_none() && !field.nullable {
            return Err(SchemaViolation {
                field: Some(field.name.clone()),
                message: format!("Missing non-nullable field '{}'.", field.name),
            });
        }

        if let Some(value) = value {
            if value.is_null() && !field.nullable {
                return Err(SchemaViolation {
                    field: Some(field.name.clone()),
                    message: format!("Field '{}' cannot be null.", field.name),
                });
            }

            if !matches_field_type(field, value) {
                return Err(SchemaViolation {
                    field: Some(field.name.clone()),
                    message: format!(
                        "Field '{}' does not match declared schema type '{}'.",
                        field.name, field.rust_type
                    ),
                });
            }
        }
    }

    Ok(())
}

fn matches_field_type(field: &ItemFieldSchema, value: &Value) -> bool {
    if value.is_null() {
        return field.nullable;
    }

    match field.value_type {
        FieldValueType::Bool => value.is_boolean(),
        FieldValueType::Integer => value.as_i64().is_some() || value.as_u64().is_some(),
        FieldValueType::Float => value.is_number(),
        FieldValueType::String => value.is_string(),
        FieldValueType::Json => true,
        FieldValueType::Sequence => value.is_array(),
        FieldValueType::Map => value.is_object(),
        FieldValueType::Unknown => true,
    }
}
