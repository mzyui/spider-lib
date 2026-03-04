//! Aggregated context shared across all crawler tasks.
//!
//! This module provides the `CrawlerContext` struct, which bundles together
//! all the shared state that needs to be passed between different crawler tasks.
//! By wrapping this context in an Arc, we can clone it cheaply (just incrementing
//! the reference count) instead of cloning each individual component.
//!
//! ## Benefits
//!
//! - **Reduced cloning overhead**: Single Arc::clone() instead of 5+ individual clones
//! - **Better code organization**: Related state is grouped together
//! - **Easier refactoring**: Adding new shared state only requires one change

use crate::{Scheduler, stats::StatCollector, spider::Spider};
use spider_pipeline::pipeline::Pipeline;
use spider_util::item::ScrapedItem;
use std::sync::Arc;

/// Inner data shared across all crawler tasks.
///
/// This struct contains all the Arc-wrapped components that need to be
/// shared between the crawler's various async tasks.
pub struct CrawlerContextInner<S, I>
where
    S: Spider<Item = I>,
    I: ScrapedItem,
{
    pub scheduler: Arc<Scheduler>,
    pub stats: Arc<StatCollector>,
    pub spider: Arc<S>,
    pub spider_state: Arc<S::State>,
    pub pipelines: Arc<Vec<Box<dyn Pipeline<I>>>>,
}

/// Aggregated context shared across all crawler tasks.
///
/// This struct wraps CrawlerContextInner in a single Arc, allowing
/// efficient cloning with just one atomic reference count operation.
pub struct CrawlerContext<S, I>(pub Arc<CrawlerContextInner<S, I>>)
where
    S: Spider<Item = I>,
    I: ScrapedItem;

impl<S, I> Clone for CrawlerContext<S, I>
where
    S: Spider<Item = I>,
    I: ScrapedItem,
{
    fn clone(&self) -> Self {
        CrawlerContext(Arc::clone(&self.0))
    }
}

impl<S, I> CrawlerContext<S, I>
where
    S: Spider<Item = I>,
    I: ScrapedItem,
{
    /// Creates a new CrawlerContext with the given components.
    pub fn new(
        scheduler: Arc<Scheduler>,
        stats: Arc<StatCollector>,
        spider: Arc<S>,
        spider_state: Arc<S::State>,
        pipelines: Arc<Vec<Box<dyn Pipeline<I>>>>,
    ) -> Self {
        CrawlerContext(Arc::new(CrawlerContextInner {
            scheduler,
            stats,
            spider,
            spider_state,
            pipelines,
        }))
    }

    /// Creates a CrawlerContext from a Crawler instance.
    pub fn from_crawler(
        scheduler: Arc<Scheduler>,
        stats: Arc<StatCollector>,
        spider: Arc<S>,
        spider_state: Arc<S::State>,
        pipelines: Arc<Vec<Box<dyn Pipeline<I>>>>,
    ) -> Self {
        Self::new(scheduler, stats, spider, spider_state, pipelines)
    }
}

// Implement Deref for convenient access to inner fields
impl<S, I> std::ops::Deref for CrawlerContext<S, I>
where
    S: Spider<Item = I>,
    I: ScrapedItem,
{
    type Target = CrawlerContextInner<S, I>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
