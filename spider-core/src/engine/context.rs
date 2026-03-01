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

use std::sync::Arc;
use crate::{Scheduler, stats::StatCollector, spider::Spider};
use spider_pipeline::pipeline::Pipeline;
use spider_util::item::ScrapedItem;

/// Aggregated context shared across all crawler tasks.
///
/// This struct bundles together all the Arc-wrapped components that need to be
/// shared between the crawler's various async tasks. Instead of cloning each
/// Arc individually, we can clone this context with a single Arc::clone().
pub struct CrawlerContext<S, I>
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

impl<S, I> Clone for CrawlerContext<S, I>
where
    S: Spider<Item = I>,
    I: ScrapedItem,
{
    fn clone(&self) -> Self {
        Self {
            scheduler: Arc::clone(&self.scheduler),
            stats: Arc::clone(&self.stats),
            spider: Arc::clone(&self.spider),
            spider_state: Arc::clone(&self.spider_state),
            pipelines: Arc::clone(&self.pipelines),
        }
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
        Self {
            scheduler,
            stats,
            spider,
            spider_state,
            pipelines,
        }
    }

    /// Creates a CrawlerContext from a Crawler instance.
    pub fn from_crawler<C>(
        scheduler: Arc<Scheduler>,
        stats: Arc<StatCollector>,
        spider: Arc<S>,
        spider_state: Arc<S::State>,
        pipelines: Arc<Vec<Box<dyn Pipeline<I>>>>,
    ) -> Self {
        Self::new(scheduler, stats, spider, spider_state, pipelines)
    }
}
