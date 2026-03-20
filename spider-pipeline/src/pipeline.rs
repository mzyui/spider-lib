//! Pipeline trait and lifecycle hooks.
//!
//! A pipeline receives each scraped item after parsing. It may keep the item,
//! transform it, drop it, write it somewhere, or preserve its own state for
//! checkpointing.

use async_trait::async_trait;
use serde_json::Value;
use spider_util::error::PipelineError;
use spider_util::item::ScrapedItem;

/// Contract implemented by item-processing pipelines.
#[async_trait]
pub trait Pipeline<I: ScrapedItem>: Send + Sync + 'static {
    /// Returns the name of the pipeline.
    fn name(&self) -> &str;

    /// Processes a single scraped item.
    ///
    /// This method can perform any processing on the item, such as storing it, validating it,
    /// or passing it to another pipeline. It can also choose to drop the item by returning `Ok(None)`.
    ///
    /// # Errors
    ///
    /// Returns an error when item processing fails.
    async fn process_item(&self, item: I) -> Result<Option<I>, PipelineError>;

    /// Called when the spider is closing.
    ///
    /// This method can be used to perform any cleanup tasks, such as closing file handles or
    /// database connections.
    ///
    /// # Errors
    ///
    /// Returns an error if cleanup fails.
    async fn close(&self) -> Result<(), PipelineError> {
        Ok(())
    }

    /// Returns the current state of the pipeline as a JSON value.
    ///
    /// This method is called during checkpointing to save the pipeline's state.
    /// The returned state should be sufficient to restore the pipeline to its current
    /// state using `restore_state`.
    ///
    /// # Errors
    ///
    /// Returns an error when state capture or serialization fails.
    async fn get_state(&self) -> Result<Option<Value>, PipelineError> {
        Ok(None)
    }

    /// Restores the pipeline's state from a JSON value.
    ///
    /// This method is called when resuming from a checkpoint. The provided state
    /// should be used to restore the pipeline to the state it was in when the
    /// checkpoint was created.
    ///
    /// # Errors
    ///
    /// Returns an error when deserializing or applying state fails.
    async fn restore_state(&self, _state: Value) -> Result<(), PipelineError> {
        Ok(())
    }
}
