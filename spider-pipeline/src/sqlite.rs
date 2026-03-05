//! Item Pipeline for persisting scraped items to a SQLite database.
//!
//! This module provides the `SqlitePipeline`, an item pipeline that
//! efficiently stores `ScrapedItem`s in a SQLite database. It offloads all
//! blocking database I/O operations to a dedicated Tokio blocking thread,
//! ensuring the asynchronous event loop remains non-blocked.
//!
//! Key features include:
//! - Dynamic table schema creation based on the fields of the first processed item.
//! - Asynchronous insertion of subsequent items into the database.
//! - Support for various data types mapping to SQLite's type system.
//! - Graceful handling of database connections and shutdown.

use crate::pipeline::Pipeline;
use async_trait::async_trait;
use log::{debug, error, info, trace};
use rusqlite::{Connection, params, params_from_iter};
use serde_json::Value;
use spider_util::{error::PipelineError, item::ScrapedItem};
use std::marker::PhantomData;
use std::path::Path;
use tokio::sync::{Mutex, mpsc, oneshot};

enum SqliteCommand {
    CreateSchema {
        item_value: Value,
        responder: oneshot::Sender<Result<(), PipelineError>>,
    },
    InsertItem {
        item_value: Value,
        responder: oneshot::Sender<Result<(), PipelineError>>,
    },
    Shutdown(oneshot::Sender<()>),
}

/// A pipeline that writes scraped items to a SQLite database.
/// All database operations are offloaded to a dedicated blocking thread.
pub struct SqlitePipeline<I: ScrapedItem> {
    command_sender: mpsc::Sender<SqliteCommand>,
    table_created: Mutex<bool>,
    _phantom: PhantomData<I>,
}

impl<I: ScrapedItem> SqlitePipeline<I> {
    /// Creates a new `SqlitePipeline` that writes items to the specified database path and table.
    pub fn new(
        db_path: impl AsRef<Path>,
        table_name: impl Into<String>,
    ) -> Result<Self, PipelineError> {
        spider_util::util::validate_output_dir(&db_path)
            .map_err(|e: spider_util::error::SpiderError| PipelineError::Other(e.to_string()))?;
        let path_buf = db_path.as_ref().to_path_buf();
        let table_name_str = table_name.into();
        info!(
            "Initializing SqlitePipeline for DB: {:?}, Table: {}",
            path_buf, table_name_str
        );

        let (command_sender, mut command_receiver) = mpsc::channel::<SqliteCommand>(100);
        let db_path_clone = path_buf.clone();
        let table_name_clone = table_name_str.clone();

        tokio::task::spawn_blocking(move || {
            let conn_result = Connection::open(&db_path_clone);
            let mut conn = match conn_result {
                Ok(c) => {
                    debug!("Successfully opened SQLite DB: {:?}", db_path_clone);
                    c
                }
                Err(e) => {
                    error!("Error opening SQLite DB {:?}: {}", db_path_clone, e);
                    return;
                }
            };

            info!("SQLite blocking task started for DB: {:?}", db_path_clone);

            while let Some(command) = command_receiver.blocking_recv() {
                match command {
                    SqliteCommand::CreateSchema {
                        item_value,
                        responder,
                    } => {
                        trace!("Processing CreateSchema command");
                        let result = create_table_if_not_exists_sync(
                            &mut conn,
                            &table_name_clone,
                            &item_value,
                        );
                        if let Err(e) = responder.send(result) {
                            error!("Failed to send CreateSchema response: {:?}", e);
                        } else {
                            trace!("CreateSchema command completed successfully");
                        }
                    }
                    SqliteCommand::InsertItem {
                        item_value,
                        responder,
                    } => {
                        trace!("Processing InsertItem command");
                        let result = insert_item_sync(&mut conn, &table_name_clone, &item_value);
                        if let Err(e) = responder.send(result) {
                            error!("Failed to send InsertItem response: {:?}", e);
                        } else {
                            trace!("InsertItem command completed successfully");
                        }
                    }
                    SqliteCommand::Shutdown(responder) => {
                        debug!("SQLite blocking task received shutdown command.");
                        let _ = responder.send(());
                        break;
                    }
                }
            }

            if let Err(e) = conn.close() {
                error!(
                    "Error closing SQLite connection for {:?}: {:?}",
                    db_path_clone, e
                );
            } else {
                debug!(
                    "SQLite connection closed successfully for {:?}",
                    db_path_clone
                );
            }
            info!("SQLite blocking task for DB: {:?} finished.", db_path_clone);
        });

        Ok(SqlitePipeline {
            command_sender,
            table_created: Mutex::new(false),
            _phantom: PhantomData,
        })
    }
}

// Synchronous helper function to create table
fn create_table_if_not_exists_sync(
    conn: &mut Connection,
    table_name: &str,
    item_value: &Value,
) -> Result<(), PipelineError> {
    if let Some(map) = item_value.as_object() {
        let mut columns = Vec::new();
        for (key, value) in map {
            let sqlite_type = match value {
                Value::Null => "TEXT",
                Value::Bool(_) => "INTEGER",
                Value::Number(n) => {
                    if n.is_f64() {
                        "REAL"
                    } else {
                        "INTEGER"
                    }
                }
                Value::String(_) => "TEXT",
                Value::Array(_) | Value::Object(_) => "TEXT",
            };
            columns.push(format!("\"{}\" {}", key, sqlite_type));
        }
        if columns.is_empty() {
            return Err(PipelineError::ItemError(
                "Cannot create table from empty item.".to_string(),
            ));
        }

        let schema_sql = format!(
            "CREATE TABLE IF NOT EXISTS \"{}\" ({})",
            table_name,
            columns.join(", ")
        );
        debug!(
            "Creating table '{}' with schema: {}",
            table_name, schema_sql
        );
        conn.execute(&schema_sql, params![])?;
        debug!("Table '{}' created successfully", table_name);
        Ok(())
    } else {
        Err(PipelineError::ItemError(
            "Item must be a JSON object to infer table schema.".to_string(),
        ))
    }
}

// Synchronous helper function to insert item
fn insert_item_sync(
    conn: &mut Connection,
    table_name: &str,
    item_value: &Value,
) -> Result<(), PipelineError> {
    if let Some(map) = item_value.as_object() {
        let keys: Vec<&String> = map.keys().collect();
        let quoted_keys: Vec<String> = keys.iter().map(|k| format!("\"{}\"", k)).collect();
        let placeholders: Vec<String> = (0..keys.len()).map(|_| "?".to_string()).collect();

        let insert_sql = format!(
            "INSERT INTO \"{}\" ({}) VALUES ({})",
            table_name,
            quoted_keys.join(", "),
            placeholders.join(", ")
        );
        trace!("Preparing SQL statement: {}", insert_sql);

        let mut stmt = conn.prepare(&insert_sql)?;

        let mut rusqlite_params_vec: Vec<rusqlite::types::Value> = Vec::new();
        for key in keys.iter() {
            let v = map.get(*key).unwrap();
            match v {
                Value::Null => rusqlite_params_vec.push(rusqlite::types::Value::Null),
                Value::Bool(b) => rusqlite_params_vec
                    .push(rusqlite::types::Value::Integer(if *b { 1 } else { 0 })),
                Value::Number(n) => {
                    if n.is_f64() {
                        rusqlite_params_vec.push(rusqlite::types::Value::Real(n.as_f64().unwrap()));
                    } else {
                        rusqlite_params_vec
                            .push(rusqlite::types::Value::Integer(n.as_i64().unwrap()));
                    }
                }
                Value::String(s) => {
                    rusqlite_params_vec.push(rusqlite::types::Value::Text(s.clone()))
                }
                Value::Array(_) | Value::Object(_) => rusqlite_params_vec.push(
                    rusqlite::types::Value::Text(serde_json::to_string(v).unwrap()),
                ),
            }
        }

        trace!(
            "Executing insert statement with {} parameters",
            rusqlite_params_vec.len()
        );
        stmt.execute(params_from_iter(rusqlite_params_vec))?;
        trace!("Item inserted successfully into table '{}'", table_name);
        Ok(())
    } else {
        Err(PipelineError::ItemError(
            "Item for insertion must be a JSON object.".to_string(),
        ))
    }
}

#[async_trait]
impl<I: ScrapedItem> Pipeline<I> for SqlitePipeline<I> {
    fn name(&self) -> &str {
        "SqlitePipeline"
    }

    async fn process_item(&self, item: I) -> Result<Option<I>, PipelineError> {
        trace!("SqlitePipeline processing item");
        let item_value = item.to_json_value();

        // Check if table created, and create if not
        let mut table_created_lock = self.table_created.lock().await;
        if !*table_created_lock {
            debug!("Creating table schema for first item");
            let (tx, rx) = oneshot::channel();
            self.command_sender
                .send(SqliteCommand::CreateSchema {
                    item_value: item_value.clone(),
                    responder: tx,
                })
                .await
                .map_err(|e| {
                    PipelineError::Other(format!("Failed to send CreateSchema command: {}", e))
                })?;
            rx.await.map_err(|e| {
                PipelineError::Other(format!("Failed to receive CreateSchema response: {}", e))
            })??;
            *table_created_lock = true;
            debug!("Table schema created successfully");
        }
        drop(table_created_lock);

        // Send insert item command
        trace!("Sending insert item command to SQLite worker");
        let (tx, rx) = oneshot::channel();
        self.command_sender
            .send(SqliteCommand::InsertItem {
                item_value,
                responder: tx,
            })
            .await
            .map_err(|e| {
                PipelineError::Other(format!("Failed to send InsertItem command: {}", e))
            })?;
        rx.await.map_err(|e| {
            PipelineError::Other(format!("Failed to receive InsertItem response: {}", e))
        })??;
        trace!("Item inserted successfully");

        Ok(Some(item))
    }

    async fn close(&self) -> Result<(), PipelineError> {
        debug!("Initiating SqlitePipeline shutdown.");
        let (tx, rx) = oneshot::channel();
        self.command_sender
            .send(SqliteCommand::Shutdown(tx))
            .await
            .map_err(|e| PipelineError::Other(format!("Failed to send Shutdown command: {}", e)))?;
        rx.await.map_err(|e| {
            PipelineError::Other(format!("Failed to receive shutdown response: {}", e))
        })?;
        info!("SqlitePipeline closed successfully.");
        Ok(())
    }
}
