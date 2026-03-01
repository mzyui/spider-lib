//! Item Pipeline for exporting scraped items to JSON Lines (.jsonl) files.
//!
//! This module provides the `JsonlPipeline`, an item pipeline designed
//! for efficient storage of scraped data in a line-delimited JSON format.
//! Each `ScrapedItem` processed by this pipeline is serialized into a single
//! JSON object, followed by a newline character, and appended to the specified
//! output file.
//!
//! This format is particularly useful for streaming data, processing with
//! command-line tools (like `jq`), and integrating with big data platforms.
//! The pipeline ensures that the output directory exists and handles file
//! writing asynchronously to avoid blocking the main event loop.

use crate::pipeline::Pipeline;
use async_trait::async_trait;
use spider_util::{error::PipelineError, item::ScrapedItem};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::marker::PhantomData;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::Mutex;
use log::{debug, info};

/// A pipeline that writes each scraped item to a JSON Lines (.jsonl) file.
/// Each item is written as a JSON object on a new line.
pub struct JsonlPipeline<I: ScrapedItem> {
    file: Arc<Mutex<File>>,
    _phantom: PhantomData<I>,
}

impl<I: ScrapedItem> JsonlPipeline<I> {
    /// Creates a new `JsonlPipeline` that writes to the specified file path.
    pub fn new(file_path: impl AsRef<Path>) -> Result<Self, PipelineError> {
        spider_util::util::validate_output_dir(&file_path)
            .map_err(|e: spider_util::error::SpiderError| PipelineError::Other(e.to_string()))?;
        let path_buf = file_path.as_ref().to_path_buf();
        info!("Initializing JsonlPipeline for file: {:?}", path_buf);

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path_buf)?;

        Ok(JsonlPipeline {
            file: Arc::new(Mutex::new(file)),
            _phantom: PhantomData,
        })
    }
}

#[async_trait]
impl<I: ScrapedItem> Pipeline<I> for JsonlPipeline<I> {
    fn name(&self) -> &str {
        "JsonlPipeline"
    }

    async fn process_item(&self, item: I) -> Result<Option<I>, PipelineError> {
        debug!("JsonlPipeline processing item.");
        let json_value = item.to_json_value();
        let serialized_item = serde_json::to_string(&json_value)?;

        let file_clone = Arc::clone(&self.file);

        tokio::task::spawn_blocking(move || {
            let mut file_lock = file_clone.blocking_lock();
            writeln!(file_lock, "{}", serialized_item)
        })
        .await
        .map_err(|e| PipelineError::Other(format!("spawn_blocking failed: {}", e)))??;

        Ok(Some(item))
    }
}
