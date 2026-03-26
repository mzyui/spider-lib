//! CSV output pipeline.
//!
//! [`CsvPipeline`] writes items to a CSV file, inferring headers from the first
//! item and serializing nested values into JSON strings when needed.

use crate::pipeline::Pipeline;
use crate::schema::{SchemaExportConfig, map_item_for_export};
use async_trait::async_trait;
use csv::Writer;
use kanal::bounded_async;
use log::{debug, error, info};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use spider_util::error::PipelineError;
use spider_util::item::ScrapedItem;
use std::fs::{File, OpenOptions};
use std::marker::PhantomData;
use std::path::Path;

#[derive(Serialize, Deserialize)]
struct CsvState {
    headers: Vec<String>,
    #[serde(default)]
    write_header_on_next_write: bool,
}

struct CsvWriterState {
    writer: Writer<File>,
    headers: Vec<String>,
    write_header_on_next_write: bool,
}

enum CsvCommand {
    Write {
        item_value: Value,
        responder: kanal::AsyncSender<Result<(), PipelineError>>,
    },
    GetState(kanal::AsyncSender<Result<Option<Value>, PipelineError>>),
    RestoreState {
        state: Value,
        responder: kanal::AsyncSender<Result<(), PipelineError>>,
    },
    Shutdown(kanal::AsyncSender<()>),
}

/// A pipeline that exports scraped items to a CSV file.
/// Headers are determined from the keys of the first item processed.
pub struct CsvPipeline<I> {
    command_sender: kanal::AsyncSender<CsvCommand>,
    export_config: Option<SchemaExportConfig>,
    _phantom: PhantomData<I>,
}

impl<I: ScrapedItem> CsvPipeline<I> {
    const COMMAND_CHANNEL_CAPACITY: usize = 4096;
    const FLUSH_EVERY_WRITES: usize = 250;

    /// Creates a new `CsvPipeline`.
    ///
    /// # Errors
    ///
    /// Returns an error when the output directory cannot be created.
    pub fn new(file_path: impl AsRef<Path>) -> Result<Self, PipelineError> {
        spider_util::util::validate_output_dir(&file_path)
            .map_err(|e: spider_util::error::SpiderError| PipelineError::Other(e.to_string()))?;
        let path_buf = file_path.as_ref().to_path_buf();
        info!("Initializing CsvPipeline for file: {:?}", path_buf);

        let (command_sender, command_receiver) =
            bounded_async::<CsvCommand>(Self::COMMAND_CHANNEL_CAPACITY);
        let path_clone = path_buf.clone();

        tokio::task::spawn(async move {
            let mut writer_state: Option<CsvWriterState> = None;
            let mut pending_writes = 0usize;

            info!("CSV async task started for file: {:?}", path_clone);

            while let Ok(command) = command_receiver.recv().await {
                match command {
                    CsvCommand::Write {
                        item_value,
                        responder,
                    } => {
                        let result = (|| {
                            if writer_state.is_none() {
                                let should_write_header = should_write_header(&path_clone)?;

                                let file = OpenOptions::new()
                                    .create(true)
                                    .append(true)
                                    .open(&path_clone)?;

                                let writer = Writer::from_writer(file);
                                let headers = if let Some(map) = item_value.as_object() {
                                    let mut h: Vec<String> = map.keys().cloned().collect();
                                    h.sort();
                                    h
                                } else {
                                    return Err(PipelineError::ItemError(
                                        "First item for CSV must be a JSON object".to_string(),
                                    ));
                                };

                                writer_state = Some(CsvWriterState {
                                    writer,
                                    headers,
                                    write_header_on_next_write: should_write_header,
                                });
                            }

                            let state = match writer_state.as_mut() {
                                Some(state) => state,
                                None => {
                                    return Err(PipelineError::Other(
                                        "CSV writer state missing unexpectedly".to_string(),
                                    ));
                                }
                            };
                            if state.write_header_on_next_write {
                                state.writer.write_record(&state.headers)?;
                                state.write_header_on_next_write = false;
                            }
                            let record = if let Some(map) = item_value.as_object() {
                                state
                                    .headers
                                    .iter()
                                    .map(|h| {
                                        map.get(h)
                                            .map(|v| {
                                                if let Some(s) = v.as_str() {
                                                    s.to_string()
                                                } else {
                                                    v.to_string()
                                                }
                                            })
                                            .unwrap_or_default()
                                    })
                                    .collect::<Vec<String>>()
                            } else {
                                return Err(PipelineError::ItemError(
                                    "Item for CSV must be a JSON object.".to_string(),
                                ));
                            };

                            state.writer.write_record(&record)?;
                            pending_writes += 1;
                            if pending_writes >= Self::FLUSH_EVERY_WRITES {
                                state.writer.flush()?;
                                pending_writes = 0;
                            }
                            Ok(())
                        })();

                        if responder.send(result).await.is_err() {
                            error!("Failed to send CSV write response.");
                        }
                    }
                    CsvCommand::GetState(responder) => {
                        let result = (|| {
                            if let Some(state) = &writer_state {
                                let state = CsvState {
                                    headers: state.headers.clone(),
                                    write_header_on_next_write: state.write_header_on_next_write,
                                };
                                let value = serde_json::to_value(state)?;
                                Ok(Some(value))
                            } else {
                                Ok(None)
                            }
                        })();
                        if responder.send(result).await.is_err() {
                            error!("Failed to send GetState response.");
                        }
                    }
                    CsvCommand::RestoreState { state, responder } => {
                        let result = (|| {
                            let state: CsvState = serde_json::from_value(state)?;
                            let file = OpenOptions::new()
                                .create(true)
                                .append(true)
                                .open(&path_clone)?;
                            let writer = Writer::from_writer(file);
                            writer_state = Some(CsvWriterState {
                                writer,
                                headers: state.headers,
                                write_header_on_next_write: state.write_header_on_next_write
                                    || should_write_header(&path_clone)?,
                            });
                            info!("CSV Exporter state restored.");
                            Ok(())
                        })();
                        if responder.send(result).await.is_err() {
                            error!("Failed to send RestoreState response.");
                        }
                    }
                    CsvCommand::Shutdown(responder) => {
                        info!("CSV async task received shutdown command.");
                        if let Some(state) = writer_state.as_mut()
                            && let Err(e) = state.writer.flush()
                        {
                            error!("Failed to flush CSV writer on shutdown: {}", e);
                        }
                        let _ = responder.send(()).await;
                        break;
                    }
                }
            }
            info!("CSV async task for file: {:?} finished.", path_clone);
        });

        Ok(CsvPipeline {
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

fn should_write_header(path: &Path) -> Result<bool, PipelineError> {
    Ok(!path.exists() || path.metadata()?.len() == 0)
}

#[async_trait]
impl<I: ScrapedItem> Pipeline<I> for CsvPipeline<I> {
    fn name(&self) -> &str {
        "CsvPipeline"
    }

    async fn process_item(&self, item: I) -> Result<Option<I>, PipelineError> {
        debug!("CsvPipeline processing item.");
        let item_value = map_item_for_export(&item, self.export_config.as_ref());

        let (tx, rx) = kanal::bounded_async(1);
        self.command_sender
            .send(CsvCommand::Write {
                item_value,
                responder: tx,
            })
            .await
            .map_err(|e| PipelineError::Other(format!("Failed to send Write command: {}", e)))?;

        let result = rx.recv().await.map_err(|e| {
            PipelineError::Other(format!("Failed to receive Write response: {}", e))
        })?;
        result?;

        Ok(Some(item))
    }

    async fn close(&self) -> Result<(), PipelineError> {
        info!("Closing CsvPipeline.");
        let (tx, rx) = kanal::bounded_async(1);
        self.command_sender
            .send(CsvCommand::Shutdown(tx))
            .await
            .map_err(|e| PipelineError::Other(format!("Failed to send Shutdown command: {}", e)))?;
        rx.recv().await.map_err(|e| {
            PipelineError::Other(format!("Failed to receive shutdown response: {}", e))
        })?;
        Ok(())
    }

    async fn get_state(&self) -> Result<Option<Value>, PipelineError> {
        let (tx, rx) = kanal::bounded_async(1);
        self.command_sender
            .send(CsvCommand::GetState(tx))
            .await
            .map_err(|e| PipelineError::Other(format!("Failed to send GetState command: {}", e)))?;
        let result = rx.recv().await.map_err(|e| {
            PipelineError::Other(format!("Failed to receive GetState response: {}", e))
        })?;
        Ok(result?)
    }

    async fn restore_state(&self, state: Value) -> Result<(), PipelineError> {
        let (tx, rx) = kanal::bounded_async(1);
        self.command_sender
            .send(CsvCommand::RestoreState {
                state,
                responder: tx,
            })
            .await
            .map_err(|e| {
                PipelineError::Other(format!("Failed to send RestoreState command: {}", e))
            })?;
        let result = rx.recv().await.map_err(|e| {
            PipelineError::Other(format!("Failed to receive RestoreState response: {}", e))
        })?;
        Ok(result?)
    }
}
