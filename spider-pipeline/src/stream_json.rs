//! Streaming JSON output pipeline.
//!
//! [`StreamJsonPipeline`] writes items to a JSON array incrementally instead of
//! holding the full result set in memory.

use crate::pipeline::Pipeline;
use async_trait::async_trait;
use kanal::bounded_async;
use log::{debug, error, info};
use serde_json::Value;
use spider_util::error::PipelineError;
use spider_util::item::ScrapedItem;
use std::marker::PhantomData;
use std::path::Path;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncWriteExt, BufWriter};

const DEFAULT_BATCH_SIZE: usize = 100;

enum StreamJsonCommand {
    Write(Value),
    Shutdown(kanal::AsyncSender<Result<(), PipelineError>>),
}

/// A pipeline that streams items directly to a JSON file without accumulating them in memory.
pub struct StreamJsonPipeline<I: ScrapedItem> {
    command_sender: kanal::AsyncSender<StreamJsonCommand>,
    _phantom: PhantomData<I>,
}

impl<I: ScrapedItem> StreamJsonPipeline<I> {
    const COMMAND_CHANNEL_CAPACITY: usize = 1024;

    /// Creates a new `StreamJsonPipeline` with default batch size.
    ///
    /// # Errors
    ///
    /// Returns an error when the output directory cannot be created.
    pub fn new(file_path: impl AsRef<Path>) -> Result<Self, PipelineError> {
        Self::with_batch_size(file_path, DEFAULT_BATCH_SIZE)
    }

    /// Creates a new `StreamJsonPipeline` with a specified batch size.
    ///
    /// # Errors
    ///
    /// Returns an error when the output directory cannot be created.
    pub fn with_batch_size(
        file_path: impl AsRef<Path>,
        batch_size: usize,
    ) -> Result<Self, PipelineError> {
        spider_util::util::validate_output_dir(&file_path)
            .map_err(|e: spider_util::error::SpiderError| PipelineError::Other(e.to_string()))?;
        let path_buf = file_path.as_ref().to_path_buf();
        info!("Initializing StreamJsonPipeline for file: {:?}", path_buf);

        let (command_sender, command_receiver) =
            bounded_async::<StreamJsonCommand>(Self::COMMAND_CHANNEL_CAPACITY);

        tokio::task::spawn(async move {
            let file = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&path_buf)
                .await
                .map_err(|e| {
                    error!("Failed to create/open file {:?}: {}", path_buf, e);
                })
                .ok();

            if let Some(file) = file {
                let mut writer = BufWriter::new(file);
                let mut items_buffer = Vec::with_capacity(batch_size);
                let mut first_item = true;

                if writer.write_all(b"[\n").await.is_err() {
                    error!("Failed to write opening bracket to file: {:?}", path_buf);
                }

                info!(
                    "StreamJsonPipeline async task started for file: {:?}",
                    path_buf
                );

                while let Ok(command) = command_receiver.recv().await {
                    match command {
                        StreamJsonCommand::Write(value) => {
                            items_buffer.push(value);

                            if items_buffer.len() >= batch_size {
                                flush_items(&mut writer, &mut items_buffer, &mut first_item)
                                    .await
                                    .ok();
                            }
                        }
                        StreamJsonCommand::Shutdown(responder) => {
                            if !items_buffer.is_empty() {
                                flush_items(&mut writer, &mut items_buffer, &mut first_item)
                                    .await
                                    .ok();
                            }

                            let result = async {
                                writer
                                    .write_all(b"\n]")
                                    .await
                                    .map_err(|e| PipelineError::IoError(e.to_string()))?;
                                writer
                                    .flush()
                                    .await
                                    .map_err(|e| PipelineError::IoError(e.to_string()))
                            }
                            .await;

                            if responder.send(result).await.is_err() {
                                error!("Failed to send shutdown response.");
                            }
                            break;
                        }
                    }
                }

                info!(
                    "StreamJsonPipeline async task for file: {:?} finished.",
                    path_buf
                );
            }
        });

        Ok(StreamJsonPipeline {
            command_sender,
            _phantom: PhantomData,
        })
    }
}

async fn flush_items(
    writer: &mut BufWriter<tokio::fs::File>,
    items_buffer: &mut Vec<Value>,
    first_item: &mut bool,
) -> Result<(), PipelineError> {
    for item in items_buffer.drain(..) {
        let item_str = serde_json::to_string(&item)
            .map_err(|e| PipelineError::SerializationError(e.to_string()))?;

        if *first_item {
            *first_item = false;
        } else {
            writer
                .write_all(b",\n")
                .await
                .map_err(|e| PipelineError::IoError(e.to_string()))?;
        }

        writer
            .write_all(b"  ")
            .await
            .map_err(|e| PipelineError::IoError(e.to_string()))?;
        writer
            .write_all(item_str.as_bytes())
            .await
            .map_err(|e| PipelineError::IoError(e.to_string()))?;
    }

    Ok(())
}

#[async_trait]
impl<I: ScrapedItem> Pipeline<I> for StreamJsonPipeline<I> {
    fn name(&self) -> &str {
        "StreamJsonPipeline"
    }

    async fn process_item(&self, item: I) -> Result<Option<I>, PipelineError> {
        debug!("StreamJsonPipeline processing item.");
        let json_value = item.to_json_value();

        self.command_sender
            .send(StreamJsonCommand::Write(json_value))
            .await
            .map_err(|e| PipelineError::Other(format!("Failed to send Write command: {}", e)))?;

        Ok(Some(item))
    }

    async fn close(&self) -> Result<(), PipelineError> {
        info!("Closing StreamJsonPipeline.");
        let (tx, rx) = kanal::bounded_async(1);
        self.command_sender
            .send(StreamJsonCommand::Shutdown(tx))
            .await
            .map_err(|e| PipelineError::Other(format!("Failed to send Shutdown command: {}", e)))?;

        rx.recv().await.map_err(|e| {
            PipelineError::Other(format!("Failed to receive shutdown response: {}", e))
        })?
    }
}
