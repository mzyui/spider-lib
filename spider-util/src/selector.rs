//! Cached CSS selector helpers.
//!
//! HTML-heavy crawls often reuse the same selectors across thousands of pages.
//! This module keeps compiled selectors cached so repeated parsing work stays low.

use crate::error::SpiderError;
use ego_tree::NodeId;
use once_cell::sync::Lazy;
use parking_lot::RwLock;
use scraper::{ElementRef, Html, Selector};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

// Global selector cache to avoid repeated compilation
static SELECTOR_CACHE: Lazy<RwLock<HashMap<String, Selector>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));
static COMPILED_SELECTOR_CACHE: Lazy<RwLock<HashMap<String, CompiledSelector>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ExtractionKind {
    Element,
    Text,
    Attr(String),
}

#[derive(Debug, Clone)]
pub(crate) struct CompiledSelector {
    selector: Selector,
    extraction: ExtractionKind,
}

impl CompiledSelector {
    pub(crate) fn selector(&self) -> &Selector {
        &self.selector
    }

    pub(crate) fn extraction(&self) -> &ExtractionKind {
        &self.extraction
    }
}

/// A node selected from an HTML document using the builtin CSS selector API.
#[derive(Debug, Clone)]
pub struct SelectorNode {
    document: Arc<Html>,
    node_id: NodeId,
    extraction: ExtractionKind,
}

/// A Scrapy-like selection result list.
#[derive(Debug, Clone)]
pub struct SelectorList {
    document: Arc<Html>,
    node_ids: Vec<NodeId>,
    extraction: ExtractionKind,
}

impl SelectorNode {
    pub(crate) fn new(document: Arc<Html>, node_id: NodeId, extraction: ExtractionKind) -> Self {
        Self {
            document,
            node_id,
            extraction,
        }
    }

    /// Applies a CSS selector relative to this node.
    ///
    /// # Errors
    ///
    /// Returns [`SpiderError::HtmlParseError`] when the selector is invalid or
    /// when chaining from a text/attribute extraction.
    pub fn css(&self, query: &str) -> Result<SelectorList, SpiderError> {
        if self.extraction != ExtractionKind::Element {
            return Err(SpiderError::HtmlParseError(
                "css() can only be chained from element selections".to_string(),
            ));
        }

        let compiled = get_cached_compiled_selector(query)?;
        let Some(scope) = self.element_ref() else {
            return Ok(SelectorList::empty(
                self.document.clone(),
                compiled.extraction().clone(),
            ));
        };

        let node_ids = scope
            .select(compiled.selector())
            .map(|element| element.id())
            .collect();

        Ok(SelectorList::new(
            self.document.clone(),
            node_ids,
            compiled.extraction().clone(),
        ))
    }

    /// Returns the extracted value for this node, if present.
    pub fn get(&self) -> Option<String> {
        self.element_ref()
            .and_then(|element| extract_element_value(element, &self.extraction))
    }

    /// Returns this node's extracted value as a single-element vector or an empty one.
    pub fn get_all(&self) -> Vec<String> {
        self.get().into_iter().collect()
    }

    /// Returns the named attribute from the selected element.
    pub fn attrib(&self, name: &str) -> Option<String> {
        self.element_ref()
            .and_then(|element| element.attr(name).map(ToOwned::to_owned))
    }

    /// Returns the concatenated text content of the selected element.
    pub fn text_content(&self) -> Option<String> {
        self.element_ref()
            .map(|element| element.text().collect::<String>())
    }

    /// Returns `true` when this element has any descendant matching `query`.
    ///
    /// # Errors
    ///
    /// Returns [`SpiderError::HtmlParseError`] when the selector is invalid or
    /// when called on a text/attribute extraction.
    pub fn has_css(&self, query: &str) -> Result<bool, SpiderError> {
        Ok(!self.css(query)?.is_empty())
    }

    /// Returns `true` when any ancestor of this element matches `query`.
    ///
    /// # Errors
    ///
    /// Returns [`SpiderError::HtmlParseError`] when the selector is invalid or
    /// when called on a text/attribute extraction.
    pub fn has_ancestor(&self, query: &str) -> Result<bool, SpiderError> {
        let selector =
            Selector::parse(query).map_err(|e| SpiderError::HtmlParseError(e.to_string()))?;
        let Some(element) = self.element_ref() else {
            return Ok(false);
        };

        Ok(element
            .ancestors()
            .filter_map(ElementRef::wrap)
            .any(|ancestor| selector.matches(&ancestor)))
    }

    fn element_ref(&self) -> Option<ElementRef<'_>> {
        element_ref_by_id(&self.document, self.node_id)
    }
}

impl SelectorList {
    pub(crate) fn new(
        document: Arc<Html>,
        node_ids: Vec<NodeId>,
        extraction: ExtractionKind,
    ) -> Self {
        Self {
            document,
            node_ids,
            extraction,
        }
    }

    pub(crate) fn from_document_query(
        document: Arc<Html>,
        query: &str,
    ) -> Result<Self, SpiderError> {
        let compiled = get_cached_compiled_selector(query)?;
        let node_ids = document
            .select(compiled.selector())
            .map(|element| element.id())
            .collect();

        Ok(Self::new(document, node_ids, compiled.extraction().clone()))
    }

    pub(crate) fn empty(document: Arc<Html>, extraction: ExtractionKind) -> Self {
        Self::new(document, Vec::new(), extraction)
    }

    /// Applies a CSS selector relative to every node in the list.
    ///
    /// # Errors
    ///
    /// Returns [`SpiderError::HtmlParseError`] when the selector is invalid or
    /// when chaining from a text/attribute extraction.
    pub fn css(&self, query: &str) -> Result<Self, SpiderError> {
        if self.extraction != ExtractionKind::Element {
            return Err(SpiderError::HtmlParseError(
                "css() can only be chained from element selections".to_string(),
            ));
        }

        let compiled = get_cached_compiled_selector(query)?;
        let mut seen = HashSet::new();
        let mut node_ids = Vec::new();

        for node_id in &self.node_ids {
            let Some(scope) = element_ref_by_id(&self.document, *node_id) else {
                continue;
            };

            for element in scope.select(compiled.selector()) {
                let id = element.id();
                if seen.insert(id) {
                    node_ids.push(id);
                }
            }
        }

        Ok(Self::new(
            self.document.clone(),
            node_ids,
            compiled.extraction().clone(),
        ))
    }

    /// Returns the first extracted value in the selection.
    pub fn get(&self) -> Option<String> {
        self.first().and_then(|node| node.get())
    }

    /// Returns all extracted values in the selection.
    pub fn get_all(&self) -> Vec<String> {
        self.node_ids
            .iter()
            .filter_map(|node_id| {
                element_ref_by_id(&self.document, *node_id)
                    .and_then(|element| extract_element_value(element, &self.extraction))
            })
            .collect()
    }

    /// Returns the named attribute from the first selected element.
    pub fn attrib(&self, name: &str) -> Option<String> {
        self.first().and_then(|node| node.attrib(name))
    }

    /// Returns the first selected node.
    pub fn first(&self) -> Option<SelectorNode> {
        self.node_ids.first().copied().map(|node_id| {
            SelectorNode::new(self.document.clone(), node_id, self.extraction.clone())
        })
    }

    /// Returns the number of matched nodes.
    pub fn len(&self) -> usize {
        self.node_ids.len()
    }

    /// Returns `true` when the selection has no matched nodes.
    pub fn is_empty(&self) -> bool {
        self.node_ids.is_empty()
    }
}

impl IntoIterator for SelectorList {
    type Item = SelectorNode;
    type IntoIter = std::vec::IntoIter<SelectorNode>;

    fn into_iter(self) -> Self::IntoIter {
        self.node_ids
            .into_iter()
            .map(|node_id| {
                SelectorNode::new(self.document.clone(), node_id, self.extraction.clone())
            })
            .collect::<Vec<_>>()
            .into_iter()
    }
}

/// Returns a compiled selector from the cache, compiling it on first use.
pub fn get_cached_selector(selector_str: &str) -> Option<Selector> {
    {
        let cache = SELECTOR_CACHE.read();
        if let Some(cached) = cache.get(selector_str) {
            return Some(cached.clone());
        }
    }

    match Selector::parse(selector_str) {
        Ok(selector) => {
            {
                let mut cache = SELECTOR_CACHE.write();
                if let Some(cached) = cache.get(selector_str) {
                    return Some(cached.clone());
                }
                cache.insert(selector_str.to_string(), selector.clone());
            }
            Some(selector)
        }
        Err(_) => None,
    }
}

pub(crate) fn get_cached_compiled_selector(query: &str) -> Result<CompiledSelector, SpiderError> {
    {
        let cache = COMPILED_SELECTOR_CACHE.read();
        if let Some(cached) = cache.get(query) {
            return Ok(cached.clone());
        }
    }

    let compiled = parse_compiled_selector(query)?;

    {
        let mut cache = COMPILED_SELECTOR_CACHE.write();
        if let Some(cached) = cache.get(query) {
            return Ok(cached.clone());
        }
        cache.insert(query.to_string(), compiled.clone());
    }

    Ok(compiled)
}

/// Pre-warms the selector cache with a small set of common selectors.
pub fn prewarm_cache() {
    let common_selectors = vec![
        "a[href]",
        "link[href]",
        "script[src]",
        "img[src]",
        "audio[src]",
        "video[src]",
        "source[src]",
        "form[action]",
        "iframe[src]",
        "frame[src]",
        "embed[src]",
        "object[data]",
    ];

    for selector_str in common_selectors {
        get_cached_selector(selector_str);
        let _ = get_cached_compiled_selector(selector_str);
    }
}

fn parse_compiled_selector(query: &str) -> Result<CompiledSelector, SpiderError> {
    let query = query.trim();
    if query.is_empty() {
        return Err(SpiderError::HtmlParseError(
            "selector query cannot be empty".to_string(),
        ));
    }

    let (selector_str, extraction) = parse_selector_parts(query)?;
    let selector =
        Selector::parse(selector_str).map_err(|e| SpiderError::HtmlParseError(e.to_string()))?;

    Ok(CompiledSelector {
        selector,
        extraction,
    })
}

fn parse_selector_parts(query: &str) -> Result<(&str, ExtractionKind), SpiderError> {
    if let Some(selector) = query.strip_suffix("::text") {
        let selector = selector.trim_end();
        if selector.is_empty() {
            return Err(SpiderError::HtmlParseError(
                "selector cannot be empty before ::text".to_string(),
            ));
        }
        return Ok((selector, ExtractionKind::Text));
    }

    if let Some(start) = query.rfind("::attr(")
        && query.ends_with(')')
    {
        let selector = query[..start].trim_end();
        let attr = query[start + "::attr(".len()..query.len() - 1].trim();
        if selector.is_empty() {
            return Err(SpiderError::HtmlParseError(
                "selector cannot be empty before ::attr(...)".to_string(),
            ));
        }
        if attr.is_empty() {
            return Err(SpiderError::HtmlParseError(
                "attribute name cannot be empty in ::attr(...)".to_string(),
            ));
        }

        return Ok((selector, ExtractionKind::Attr(attr.to_string())));
    }

    Ok((query, ExtractionKind::Element))
}

fn element_ref_by_id(document: &Html, node_id: NodeId) -> Option<ElementRef<'_>> {
    document.tree.get(node_id).and_then(ElementRef::wrap)
}

fn extract_element_value(element: ElementRef<'_>, extraction: &ExtractionKind) -> Option<String> {
    match extraction {
        ExtractionKind::Element => Some(element.html()),
        ExtractionKind::Text => Some(element.text().collect::<String>()),
        ExtractionKind::Attr(attr) => element.attr(attr).map(ToOwned::to_owned),
    }
}
