//! JSON Lines output pipeline.
//!
//! [`JsonlPipeline`] appends one serialized item per line, which makes it a
//! good fit for streaming workflows and shell-based processing.

use crate::pipeline::Pipeline;
use crate::schema::{SchemaExportConfig, map_item_for_export};
use async_trait::async_trait;
use log::{debug, info};
use spider_util::{error::PipelineError, item::ScrapedItem};
use std::fs::OpenOptions;
use std::io::Write;
use std::marker::PhantomData;
use std::path::Path;
use tokio::sync::{mpsc, oneshot};

enum JsonlCommand {
    Write {
        serialized_item: String,
        responder: oneshot::Sender<Result<(), PipelineError>>,
    },
    Shutdown(oneshot::Sender<Result<(), PipelineError>>),
}

/// A pipeline that writes each scraped item to a JSON Lines (.jsonl) file.
/// Each item is written as a JSON object on a new line.
pub struct JsonlPipeline<I: ScrapedItem> {
    command_sender: mpsc::Sender<JsonlCommand>,
    export_config: Option<SchemaExportConfig>,
    _phantom: PhantomData<I>,
}

impl<I: ScrapedItem> JsonlPipeline<I> {
    const COMMAND_CHANNEL_CAPACITY: usize = 1024;
    const FLUSH_EVERY_WRITES: usize = 100;

    /// Creates a new `JsonlPipeline` that writes to the specified file path.
    ///
    /// # Errors
    ///
    /// Returns an error when the output file cannot be opened or the parent
    /// directory cannot be created.
    pub fn new(file_path: impl AsRef<Path>) -> Result<Self, PipelineError> {
        spider_util::util::validate_output_dir(&file_path)
            .map_err(|e: spider_util::error::SpiderError| PipelineError::Other(e.to_string()))?;
        let path_buf = file_path.as_ref().to_path_buf();
        info!("Initializing JsonlPipeline for file: {:?}", path_buf);

        let (command_sender, mut command_receiver) =
            mpsc::channel::<JsonlCommand>(Self::COMMAND_CHANNEL_CAPACITY);

        tokio::task::spawn_blocking(move || {
            let file_result = OpenOptions::new().create(true).append(true).open(&path_buf);
            let mut file = match file_result {
                Ok(file) => file,
                Err(e) => {
                    while let Some(command) = command_receiver.blocking_recv() {
                        match command {
                            JsonlCommand::Write { responder, .. } => {
                                let _ = responder.send(Err(PipelineError::IoError(e.to_string())));
                            }
                            JsonlCommand::Shutdown(responder) => {
                                let _ = responder.send(Err(PipelineError::IoError(e.to_string())));
                                break;
                            }
                        }
                    }
                    return;
                }
            };

            let mut pending_writes = 0usize;
            while let Some(command) = command_receiver.blocking_recv() {
                match command {
                    JsonlCommand::Write {
                        serialized_item,
                        responder,
                    } => {
                        let result = (|| -> Result<(), PipelineError> {
                            file.write_all(serialized_item.as_bytes())?;
                            file.write_all(b"\n")?;
                            pending_writes += 1;
                            if pending_writes >= Self::FLUSH_EVERY_WRITES {
                                file.flush()?;
                                pending_writes = 0;
                            }
                            Ok(())
                        })();
                        let _ = responder.send(result);
                    }
                    JsonlCommand::Shutdown(responder) => {
                        let result = file.flush().map_err(PipelineError::from);
                        let _ = responder.send(result);
                        break;
                    }
                }
            }
        });

        Ok(JsonlPipeline {
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
impl<I: ScrapedItem> Pipeline<I> for JsonlPipeline<I> {
    fn name(&self) -> &str {
        "JsonlPipeline"
    }

    async fn process_item(&self, item: I) -> Result<Option<I>, PipelineError> {
        debug!("JsonlPipeline processing item.");
        let json_value = map_item_for_export(&item, self.export_config.as_ref());
        let serialized_item = serde_json::to_string(&json_value)?;

        let (tx, rx) = oneshot::channel();
        self.command_sender
            .send(JsonlCommand::Write {
                serialized_item,
                responder: tx,
            })
            .await
            .map_err(|e| PipelineError::Other(format!("Failed to send Write command: {}", e)))?;
        rx.await.map_err(|e| {
            PipelineError::Other(format!("Failed to receive Write response: {}", e))
        })??;

        Ok(Some(item))
    }

    async fn close(&self) -> Result<(), PipelineError> {
        let (tx, rx) = oneshot::channel();
        self.command_sender
            .send(JsonlCommand::Shutdown(tx))
            .await
            .map_err(|e| PipelineError::Other(format!("Failed to send Shutdown command: {}", e)))?;
        rx.await.map_err(|e| {
            PipelineError::Other(format!("Failed to receive shutdown response: {}", e))
        })?
    }
}
