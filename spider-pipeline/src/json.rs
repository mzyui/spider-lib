//! Item Pipeline for exporting scraped items to a single JSON file.
//!
//! This module provides the `JsonPipeline`, an item pipeline designed
//! to collect all `ScrapedItem`s processed by the crawler and then, upon
//! the pipeline's closure (e.g., at the end of a crawl), write them as a
//! single, pretty-printed JSON array to a specified output file.
//!
//! Key features include:
//! - Aggregation of all scraped items into an in-memory collection.
//! - Asynchronous writing of the complete JSON array to disk, performed
//!   on a dedicated blocking thread to prevent event loop blocking.
//! - Support for state persistence, allowing the pipeline to save and
//!   restore its collected items for checkpointing.
//! - Produces a well-formed JSON document suitable for direct consumption
//!   by other applications or for human readability.

use crate::pipeline::Pipeline;
use async_trait::async_trait;
use kanal::bounded_async;
use log::{debug, error, info};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use spider_util::error::PipelineError;
use spider_util::item::ScrapedItem;
use std::fs::File;
use std::io::Write;
use std::marker::PhantomData;
use std::path::Path;

#[derive(Serialize, Deserialize)]
struct JsonState {
    items: Vec<Value>,
}

enum JsonCommand {
    Write(Value),
    GetState(kanal::AsyncSender<Result<Option<Value>, PipelineError>>),
    RestoreState {
        state: Value,
        responder: kanal::AsyncSender<Result<(), PipelineError>>,
    },
    Shutdown(kanal::AsyncSender<Result<(), PipelineError>>),
}

/// A pipeline that writes all scraped items to a single JSON file as a JSON array.
/// Items are collected in a blocking task and written to disk when the pipeline is closed.
pub struct JsonPipeline<I: ScrapedItem> {
    command_sender: kanal::AsyncSender<JsonCommand>,
    _phantom: PhantomData<I>,
}

impl<I: ScrapedItem> JsonPipeline<I> {
    const COMMAND_CHANNEL_CAPACITY: usize = 1024;

    /// Creates a new `JsonPipeline`.
    ///
    /// # Errors
    ///
    /// Returns an error when the output directory cannot be created.
    pub fn new(file_path: impl AsRef<Path>) -> Result<Self, PipelineError> {
        spider_util::util::validate_output_dir(&file_path)
            .map_err(|e: spider_util::error::SpiderError| PipelineError::Other(e.to_string()))?;
        let (command_sender, command_receiver) =
            bounded_async::<JsonCommand>(Self::COMMAND_CHANNEL_CAPACITY);
        let path_buf = file_path.as_ref().to_path_buf();

        tokio::task::spawn(async move {
            let mut items: Vec<Value> = Vec::new();
            info!("JsonPipeline async task started for file: {:?}", path_buf);

            while let Ok(command) = command_receiver.recv().await {
                match command {
                    JsonCommand::Write(value) => {
                        items.push(value);
                    }
                    JsonCommand::GetState(responder) => {
                        let result = (|| {
                            if items.is_empty() {
                                return Ok(None);
                            }
                            let state = JsonState {
                                items: items.clone(),
                            };
                            let value = serde_json::to_value(state)?;
                            Ok(Some(value))
                        })();
                        if responder.send(result).await.is_err() {
                            error!("Failed to send GetState response.");
                        }
                    }
                    JsonCommand::RestoreState { state, responder } => {
                        let result = (|| {
                            let state: JsonState = serde_json::from_value(state)?;
                            items = state.items;
                            info!("JsonPipeline state restored with {} items.", items.len());
                            Ok(())
                        })();
                        if responder.send(result).await.is_err() {
                            error!("Failed to send RestoreState response.");
                        }
                    }
                    JsonCommand::Shutdown(responder) => {
                        info!("JsonPipeline writing {} items to file.", items.len());
                        let result = (|| {
                            let mut file = File::create(&path_buf)?;
                            let json_array = Value::Array(items);
                            let json_string = serde_json::to_string_pretty(&json_array)?;
                            file.write_all(json_string.as_bytes())?;
                            Ok(())
                        })();
                        if responder.send(result).await.is_err() {
                            error!("Failed to send JsonPipeline shutdown response.");
                        }
                        break;
                    }
                }
            }
            info!("JsonPipeline async task for file: {:?} finished.", path_buf);
        });

        Ok(JsonPipeline {
            command_sender,
            _phantom: PhantomData,
        })
    }
}

#[async_trait]
impl<I: ScrapedItem> Pipeline<I> for JsonPipeline<I> {
    fn name(&self) -> &str {
        "JsonPipeline"
    }

    async fn process_item(&self, item: I) -> Result<Option<I>, PipelineError> {
        debug!("JsonPipeline processing item.");
        let json_value = item.to_json_value();
        self.command_sender
            .send(JsonCommand::Write(json_value))
            .await
            .map_err(|e| PipelineError::Other(format!("Failed to send Write command: {}", e)))?;
        Ok(Some(item))
    }

    async fn close(&self) -> Result<(), PipelineError> {
        info!("Closing JsonPipeline.");
        let (tx, rx) = kanal::bounded_async(1);
        self.command_sender
            .send(JsonCommand::Shutdown(tx))
            .await
            .map_err(|e| PipelineError::Other(format!("Failed to send Shutdown command: {}", e)))?;
        rx.recv().await.map_err(|e| {
            PipelineError::Other(format!("Failed to receive shutdown response: {}", e))
        })?
    }

    async fn get_state(&self) -> Result<Option<Value>, PipelineError> {
        let (tx, rx) = kanal::bounded_async(1);
        self.command_sender
            .send(JsonCommand::GetState(tx))
            .await
            .map_err(|e| PipelineError::Other(format!("Failed to send GetState command: {}", e)))?;
        rx.recv().await.map_err(|e| {
            PipelineError::Other(format!("Failed to receive GetState response: {}", e))
        })?
    }

    async fn restore_state(&self, state: Value) -> Result<(), PipelineError> {
        let (tx, rx) = kanal::bounded_async(1);
        self.command_sender
            .send(JsonCommand::RestoreState {
                state,
                responder: tx,
            })
            .await
            .map_err(|e| {
                PipelineError::Other(format!("Failed to send RestoreState command: {}", e))
            })?;
        rx.recv().await.map_err(|e| {
            PipelineError::Other(format!("Failed to receive RestoreState response: {}", e))
        })?
    }
}
