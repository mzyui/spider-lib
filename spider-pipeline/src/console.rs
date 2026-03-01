//! Item Pipeline for writing scraped items to the console.
//!
//! This module provides the `ConsolePipeline`, a basic and useful
//! item pipeline for debugging and immediate inspection of scraped data.
//! When integrated into a crawler, this pipeline simply logs the received
//! `ScrapedItem`s to the console (or configured tracing output).
//!
//! It serves as a straightforward way to verify that spiders are extracting
//! data correctly and that items are flowing through the pipeline as expected.
use spider_util::{error::PipelineError, item::ScrapedItem};
use crate::pipeline::Pipeline;
use async_trait::async_trait;
use log::info;

/// A pipeline that prints scraped items to the console.
pub struct ConsolePipeline;

impl ConsolePipeline {
    /// Creates a new `ConsolePipeline`.
    pub fn new() -> Self {
        Self
    }
}

impl Default for ConsolePipeline {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl<I: ScrapedItem> Pipeline<I> for ConsolePipeline {
    fn name(&self) -> &str {
        "ConsolePipeline"
    }

    async fn process_item(&self, item: I) -> Result<Option<I>, PipelineError> {
        info!("Pipeline processing item: {:?}", item);
        Ok(Some(item))
    }
}