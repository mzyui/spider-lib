//! Item Pipeline for deduplicating scraped items.
//!
//! This module provides the `DeduplicationPipeline`, an essential component
//! in ensuring the uniqueness of scraped data. This pipeline intercepts items
//! after they have been scraped by a spider and before they are passed to
//! subsequent pipelines or exporters.
//!
//! The pipeline works by:
//! - Configuring a set of `unique_fields` within an item (e.g., product SKU, article URL).
//! - Generating a unique hash for each item based on the values of these fields.
//! - Storing previously seen hashes to identify and drop duplicate items,
//!   preventing redundant data from being processed or stored.
//! - Supporting state persistence, allowing the set of seen hashes to be
//!   saved and restored across crawler runs.

use crate::pipeline::Pipeline;
use async_trait::async_trait;
use dashmap::DashSet;
use seahash::SeaHasher;
use serde_json::Value;
use spider_util::{error::PipelineError, item::ScrapedItem};
use std::collections::HashSet;
use std::hash::Hasher;
use std::marker::PhantomData;
use log::{debug, info};

/// A pipeline that filters out duplicate items based on a configurable set of fields.
pub struct DeduplicationPipeline<I: ScrapedItem> {
    unique_fields: Vec<String>,
    seen_hashes: DashSet<u64>,
    _phantom: PhantomData<I>,
}

impl<I: ScrapedItem> DeduplicationPipeline<I> {
    /// Creates a new `DeduplicationPipeline` with a specified set of unique fields.
    pub fn new(unique_fields: &[&str]) -> Self {
        info!(
            "Initializing DeduplicationPipeline with unique fields: {:?}",
            unique_fields
        );
        DeduplicationPipeline {
            unique_fields: unique_fields.iter().map(|&s| s.to_string()).collect(),
            seen_hashes: DashSet::new(),
            _phantom: PhantomData,
        }
    }

    /// Generates a hash for an item based on its unique fields.
    fn generate_hash(&self, item: &I) -> Result<u64, PipelineError> {
        let item_value = item.to_json_value();
        let mut hasher = SeaHasher::new();

        if let Some(map) = item_value.as_object() {
            for field_name in &self.unique_fields {
                if let Some(value) = map.get(field_name) {
                    hasher.write(field_name.as_bytes());

                    if let Some(str_val) = value.as_str() {
                        hasher.write(str_val.as_bytes());
                    } else {
                        hasher.write(value.to_string().as_bytes());
                    };
                } else {
                    hasher.write(field_name.as_bytes());
                    hasher.write("".as_bytes());
                }
            }
        } else {
            return Err(PipelineError::ItemError(
                "Item for deduplication must be a JSON object.".to_string(),
            ));
        }
        Ok(hasher.finish())
    }
}

#[async_trait]
impl<I: ScrapedItem> Pipeline<I> for DeduplicationPipeline<I> {
    fn name(&self) -> &str {
        "DeduplicationPipeline"
    }

    async fn process_item(&self, item: I) -> Result<Option<I>, PipelineError> {
        debug!("DeduplicationPipeline processing item.");

        let item_hash = self.generate_hash(&item)?;

        if self.seen_hashes.insert(item_hash) {
            debug!("Unique item, passing through: {:?}", item);
            Ok(Some(item))
        } else {
            debug!("Duplicate item detected, dropping: {:?}", item);
            Ok(None)
        }
    }

    async fn get_state(&self) -> Result<Option<Value>, PipelineError> {
        let hashes: HashSet<u64> = self.seen_hashes.iter().map(|r| *r).collect();
        let state = serde_json::to_value(hashes).map_err(|e| {
            PipelineError::Other(format!("Failed to serialize deduplication state: {}", e))
        })?;
        Ok(Some(state))
    }

    async fn restore_state(&self, state: Value) -> Result<(), PipelineError> {
        let hashes: HashSet<u64> = serde_json::from_value(state).map_err(|e| {
            PipelineError::Other(format!("Failed to deserialize deduplication state: {}", e))
        })?;

        self.seen_hashes.clear();
        for hash in hashes {
            self.seen_hashes.insert(hash);
        }

        info!(
            "Restored {} seen items in DeduplicationPipeline.",
            self.seen_hashes.len()
        );

        Ok(())
    }
}
