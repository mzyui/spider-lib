//! JSON array output pipeline.
//!
//! [`JsonPipeline`] keeps items in memory and writes a pretty-printed JSON
//! array when the pipeline is closed.

use crate::pipeline::Pipeline;
use crate::schema::{SchemaExportConfig, map_item_for_export};
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
    export_config: Option<SchemaExportConfig>,
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
            export_config: None,
            _phantom: PhantomData,
        })
    }

    /// Applies typed export mapping before values are written.
    pub fn with_schema_export_config(mut self, config: SchemaExportConfig) -> Self {
        self.export_config = Some(config);
        self
    }
}

#[async_trait]
impl<I: ScrapedItem> Pipeline<I> for JsonPipeline<I> {
    fn name(&self) -> &str {
        "JsonPipeline"
    }

    async fn process_item(&self, item: I) -> Result<Option<I>, PipelineError> {
        debug!("JsonPipeline processing item.");
        let json_value = map_item_for_export(&item, self.export_config.as_ref());
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
