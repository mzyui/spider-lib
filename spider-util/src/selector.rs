//! Cached CSS selector helpers.
//!
//! HTML-heavy crawls often reuse the same selectors across thousands of pages.
//! This module keeps compiled selectors cached so repeated parsing work stays low.

use crate::error::SpiderError;
use ego_tree::NodeId;
use ego_tree::iter::Children;
use once_cell::sync::Lazy;
use parking_lot::RwLock;
use scraper::{ElementRef, Html, Selector};
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

// Global selector cache to avoid repeated compilation
static SELECTOR_CACHE: Lazy<RwLock<HashMap<String, Selector>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));
static COMPILED_SELECTOR_CACHE: Lazy<RwLock<HashMap<String, CompiledSelector>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));

thread_local! {
    static DOCUMENT_CACHE: RefCell<HashMap<u64, (Arc<str>, Arc<Html>)>> = RefCell::new(HashMap::new());
}

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
    document_html: Arc<str>,
    document_hash: u64,
    path: Arc<[usize]>,
    extraction: ExtractionKind,
}

/// A Scrapy-like selection result list.
#[derive(Debug, Clone)]
pub struct SelectorList {
    document_html: Arc<str>,
    document_hash: u64,
    paths: Vec<Arc<[usize]>>,
    extraction: ExtractionKind,
}

fn assert_selector_types_are_send_sync() {
    fn assert_traits<T: Send + Sync>() {}

    assert_traits::<SelectorNode>();
    assert_traits::<SelectorList>();
}

const _: fn() = assert_selector_types_are_send_sync;

impl SelectorNode {
    pub(crate) fn new(
        document_html: Arc<str>,
        document_hash: u64,
        path: Arc<[usize]>,
        extraction: ExtractionKind,
    ) -> Self {
        Self {
            document_html,
            document_hash,
            path,
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
        with_document(
            self.document_hash,
            &self.document_html,
            |document| -> Result<SelectorList, SpiderError> {
                let Some(scope) = self.element_ref(document) else {
                    return Ok(SelectorList::empty(
                        self.document_html.clone(),
                        self.document_hash,
                        compiled.extraction().clone(),
                    ));
                };

                let paths = scope
                    .select(compiled.selector())
                    .map(|element| node_path(document, element.id()))
                    .collect();

                Ok(SelectorList::new(
                    self.document_html.clone(),
                    self.document_hash,
                    paths,
                    compiled.extraction().clone(),
                ))
            },
        )
    }

    /// Returns the extracted value for this node, if present.
    pub fn get(&self) -> Option<String> {
        with_document(self.document_hash, &self.document_html, |document| {
            self.element_ref(document)
                .and_then(|element| extract_element_value(element, &self.extraction))
        })
    }

    /// Returns this node's extracted value as a single-element vector or an empty one.
    pub fn get_all(&self) -> Vec<String> {
        self.get().into_iter().collect()
    }

    /// Returns the named attribute from the selected element.
    pub fn attrib(&self, name: &str) -> Option<String> {
        with_document(self.document_hash, &self.document_html, |document| {
            self.element_ref(document)
                .and_then(|element| element.attr(name).map(ToOwned::to_owned))
        })
    }

    /// Returns the concatenated text content of the selected element.
    pub fn text_content(&self) -> Option<String> {
        with_document(self.document_hash, &self.document_html, |document| {
            self.element_ref(document)
                .map(|element| element.text().collect::<String>())
        })
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
        with_document(
            self.document_hash,
            &self.document_html,
            |document| -> Result<bool, SpiderError> {
                let Some(element) = self.element_ref(document) else {
                    return Ok(false);
                };

                Ok(element
                    .ancestors()
                    .filter_map(ElementRef::wrap)
                    .any(|ancestor| selector.matches(&ancestor)))
            },
        )
    }

    fn element_ref<'a>(&self, document: &'a Html) -> Option<ElementRef<'a>> {
        element_ref_by_path(document, &self.path)
    }
}

impl SelectorList {
    pub(crate) fn new(
        document_html: Arc<str>,
        document_hash: u64,
        paths: Vec<Arc<[usize]>>,
        extraction: ExtractionKind,
    ) -> Self {
        Self {
            document_html,
            document_hash,
            paths,
            extraction,
        }
    }

    pub(crate) fn from_document_query(
        document_html: Arc<str>,
        document_hash: u64,
        query: &str,
    ) -> Result<Self, SpiderError> {
        let compiled = get_cached_compiled_selector(query)?;
        with_document(
            document_hash,
            &document_html,
            |document| -> Result<Self, SpiderError> {
                let paths = document
                    .select(compiled.selector())
                    .map(|element| node_path(document, element.id()))
                    .collect();

                Ok(Self::new(
                    document_html.clone(),
                    document_hash,
                    paths,
                    compiled.extraction().clone(),
                ))
            },
        )
    }

    pub(crate) fn empty(
        document_html: Arc<str>,
        document_hash: u64,
        extraction: ExtractionKind,
    ) -> Self {
        Self::new(document_html, document_hash, Vec::new(), extraction)
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
        with_document(
            self.document_hash,
            &self.document_html,
            |document| -> Result<Self, SpiderError> {
                let mut paths = Vec::new();

                for path in &self.paths {
                    let Some(scope) = element_ref_by_path(document, path) else {
                        continue;
                    };

                    for element in scope.select(compiled.selector()) {
                        let path = node_path(document, element.id());
                        if seen.insert(path.clone()) {
                            paths.push(path);
                        }
                    }
                }

                Ok(Self::new(
                    self.document_html.clone(),
                    self.document_hash,
                    paths,
                    compiled.extraction().clone(),
                ))
            },
        )
    }

    /// Returns the first extracted value in the selection.
    pub fn get(&self) -> Option<String> {
        self.first().and_then(|node| node.get())
    }

    /// Returns all extracted values in the selection.
    pub fn get_all(&self) -> Vec<String> {
        with_document(self.document_hash, &self.document_html, |document| {
            self.paths
                .iter()
                .filter_map(|path| {
                    element_ref_by_path(document, path)
                        .and_then(|element| extract_element_value(element, &self.extraction))
                })
                .collect()
        })
    }

    /// Returns the named attribute from the first selected element.
    pub fn attrib(&self, name: &str) -> Option<String> {
        self.first().and_then(|node| node.attrib(name))
    }

    /// Returns the first selected node.
    pub fn first(&self) -> Option<SelectorNode> {
        self.paths.first().cloned().map(|path| {
            SelectorNode::new(
                self.document_html.clone(),
                self.document_hash,
                path,
                self.extraction.clone(),
            )
        })
    }

    /// Returns the number of matched nodes.
    pub fn len(&self) -> usize {
        self.paths.len()
    }

    /// Returns `true` when the selection has no matched nodes.
    pub fn is_empty(&self) -> bool {
        self.paths.is_empty()
    }
}

impl IntoIterator for SelectorList {
    type Item = SelectorNode;
    type IntoIter = std::vec::IntoIter<SelectorNode>;

    fn into_iter(self) -> Self::IntoIter {
        self.paths
            .into_iter()
            .map(|path| {
                SelectorNode::new(
                    self.document_html.clone(),
                    self.document_hash,
                    path,
                    self.extraction.clone(),
                )
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

fn with_document<T>(document_hash: u64, document_html: &Arc<str>, f: impl FnOnce(&Html) -> T) -> T {
    DOCUMENT_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        let parsed = match cache.get(&document_hash) {
            Some((cached_html, parsed)) if cached_html.as_ref() == document_html.as_ref() => {
                parsed.clone()
            }
            _ => {
                let parsed = Arc::new(Html::parse_document(document_html.as_ref()));
                cache.insert(document_hash, (document_html.clone(), parsed.clone()));
                parsed
            }
        };
        drop(cache);
        f(parsed.as_ref())
    })
}

fn element_ref_by_id(document: &Html, node_id: NodeId) -> Option<ElementRef<'_>> {
    document.tree.get(node_id).and_then(ElementRef::wrap)
}

fn element_ref_by_path<'a>(document: &'a Html, path: &[usize]) -> Option<ElementRef<'a>> {
    let mut current = document.tree.root().id();

    for child_index in path {
        current = nth_child(document.tree.get(current)?.children(), *child_index)?.id();
    }

    element_ref_by_id(document, current)
}

fn node_path(document: &Html, node_id: NodeId) -> Arc<[usize]> {
    let mut path = Vec::new();
    let mut current = node_id;

    while let Some(node) = document.tree.get(current) {
        let Some(parent) = node.parent() else {
            break;
        };
        let parent_id = parent.id();

        let mut child_index = 0usize;
        for child in parent.children() {
            if child.id() == current {
                break;
            }
            child_index += 1;
        }

        path.push(child_index);
        current = parent_id;
    }

    path.reverse();
    Arc::from(path)
}

fn nth_child<'a>(
    mut children: Children<'a, scraper::node::Node>,
    child_index: usize,
) -> Option<ego_tree::NodeRef<'a, scraper::node::Node>> {
    children.nth(child_index)
}

fn extract_element_value(element: ElementRef<'_>, extraction: &ExtractionKind) -> Option<String> {
    match extraction {
        ExtractionKind::Element => Some(element.html()),
        ExtractionKind::Text => Some(element.text().collect::<String>()),
        ExtractionKind::Attr(attr) => element.attr(attr).map(ToOwned::to_owned),
    }
}
