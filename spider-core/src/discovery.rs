//! Runtime-managed response discovery helpers.

use crate::config::DiscoveryConfig;
use log::debug;
use quick_xml::Reader;
use quick_xml::events::Event;
use spider_util::request::Request;
use spider_util::response::{PageMetadata, Response};
use std::collections::HashSet;
use url::Url;

const PAGE_METADATA_META_KEY: &str = "__page_metadata";
const DISCOVERY_SOURCE_META_KEY: &str = "__discovery_source";
const DISCOVERY_RULE_META_KEY: &str = "__discovery_rule";
const SITEMAP_DEPTH_META_KEY: &str = "__sitemap_depth";
const SITEMAP_SOURCE_VALUE: &str = "sitemap";
const HTML_DISCOVERY_SOURCE_VALUE: &str = "html-discovery";

/// Requests and metadata discovered by the runtime for a single response.
#[derive(Debug, Default)]
pub struct DiscoveryOutcome {
    /// Structured metadata extracted from the page.
    pub metadata: Option<PageMetadata>,
    /// Additional requests produced by discovery.
    pub requests: Vec<Request>,
}

#[derive(Debug, Default)]
struct SitemapDocument {
    urls: Vec<Url>,
    nested_sitemaps: Vec<Url>,
}

/// Extracts metadata and follow-up requests from a response according to config.
pub fn discover_response(response: &Response, config: &DiscoveryConfig) -> DiscoveryOutcome {
    let metadata = if config.should_extract_metadata() && looks_like_html(response) {
        response
            .page_metadata()
            .ok()
            .filter(|value| !value.is_empty())
    } else {
        None
    };

    let mut requests = Vec::new();

    if looks_like_html(response) {
        if !config.rules.is_empty() {
            let mut seen = HashSet::new();
            let mut matched_any_rule = false;

            for rule in config
                .rules
                .iter()
                .filter(|rule| rule.matches_response(&response.url))
            {
                let Some(options) =
                    config.effective_link_extract_options_for(rule.link_extract_options.clone())
                else {
                    continue;
                };
                matched_any_rule = true;

                for link in response.links_iter(options) {
                    let request = Request::new(link.url)
                        .with_meta(
                            DISCOVERY_SOURCE_META_KEY,
                            serde_json::Value::String(HTML_DISCOVERY_SOURCE_VALUE.to_string()),
                        )
                        .with_meta(
                            DISCOVERY_RULE_META_KEY,
                            serde_json::Value::String(rule.name.clone()),
                        );

                    if seen.insert(request.fingerprint()) {
                        requests.push(request);
                    }
                }
            }

            if !matched_any_rule && let Some(options) = config.effective_link_extract_options() {
                requests.extend(response.links_iter(options).map(|link| {
                    Request::new(link.url).with_meta(
                        DISCOVERY_SOURCE_META_KEY,
                        serde_json::Value::String(HTML_DISCOVERY_SOURCE_VALUE.to_string()),
                    )
                }));
            }
        } else if let Some(options) = config.effective_link_extract_options() {
            requests.extend(response.links_iter(options).map(|link| {
                Request::new(link.url).with_meta(
                    DISCOVERY_SOURCE_META_KEY,
                    serde_json::Value::String(HTML_DISCOVERY_SOURCE_VALUE.to_string()),
                )
            }));
        }
    }

    if config.discover_sitemaps
        && is_sitemap_response(response)
        && let Some(document) = parse_sitemap(response)
    {
        let current_depth = response
            .get_meta(SITEMAP_DEPTH_META_KEY)
            .and_then(|value| value.as_u64())
            .unwrap_or(0) as usize;

        requests.extend(document.urls.into_iter().map(|url| {
            Request::new(url).with_meta(
                DISCOVERY_SOURCE_META_KEY,
                serde_json::Value::String(SITEMAP_SOURCE_VALUE.to_string()),
            )
        }));

        if current_depth < config.max_sitemap_depth {
            requests.extend(document.nested_sitemaps.into_iter().map(|url| {
                Request::new(url)
                    .with_meta(
                        DISCOVERY_SOURCE_META_KEY,
                        serde_json::Value::String(SITEMAP_SOURCE_VALUE.to_string()),
                    )
                    .with_meta(
                        SITEMAP_DEPTH_META_KEY,
                        serde_json::Value::from((current_depth + 1) as u64),
                    )
            }));
        } else if !document.nested_sitemaps.is_empty() {
            debug!(
                "Skipping {} nested sitemaps from {} because max depth {} was reached",
                document.nested_sitemaps.len(),
                response.url,
                config.max_sitemap_depth
            );
        }
    }

    DiscoveryOutcome { metadata, requests }
}

/// Attaches extracted page metadata to the response metadata map.
pub fn attach_page_metadata(response: &mut Response, metadata: &PageMetadata) {
    if let Ok(value) = serde_json::to_value(metadata) {
        response.insert_meta(PAGE_METADATA_META_KEY.to_string(), value);
    }
}

/// Metadata key used for page metadata injection.
pub fn page_metadata_meta_key() -> &'static str {
    PAGE_METADATA_META_KEY
}

/// Metadata key used for matched discovery rule names.
pub fn discovery_rule_meta_key() -> &'static str {
    DISCOVERY_RULE_META_KEY
}

fn looks_like_html(response: &Response) -> bool {
    response
        .headers
        .get(http::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            value.contains("text/html")
                || value.contains("application/xhtml+xml")
                || value.contains("text/plain")
        })
        || response
            .text()
            .ok()
            .is_some_and(|body| body.trim_start().starts_with('<'))
}

fn is_sitemap_response(response: &Response) -> bool {
    response
        .headers
        .get(http::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.contains("xml"))
        || response.url.path().ends_with(".xml")
        || response.get_meta(SITEMAP_DEPTH_META_KEY).is_some()
}

fn parse_sitemap(response: &Response) -> Option<SitemapDocument> {
    let xml = response.text().ok()?;
    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(true);

    let mut document = SitemapDocument::default();
    let mut current_section: Option<Vec<u8>> = None;
    let mut in_loc = false;

    loop {
        match reader.read_event() {
            Ok(Event::Start(event)) => {
                let name = event.name().into_inner().to_vec();
                if name.as_slice() == b"url" || name.as_slice() == b"sitemap" {
                    current_section = Some(name);
                } else if name.as_slice() == b"loc" {
                    in_loc = true;
                }
            }
            Ok(Event::End(event)) => {
                let name = event.name().into_inner();
                if name == b"url" || name == b"sitemap" {
                    current_section = None;
                } else if name == b"loc" {
                    in_loc = false;
                }
            }
            Ok(Event::Text(text)) => {
                if !in_loc {
                    continue;
                }

                let raw = text.decode().ok()?;
                let raw = raw.trim();
                if raw.is_empty() {
                    continue;
                }

                let Ok(url) = response.url.join(raw) else {
                    continue;
                };

                match current_section.as_deref() {
                    Some(b"url") => document.urls.push(url),
                    Some(b"sitemap") => document.nested_sitemaps.push(url),
                    _ => {}
                }
            }
            Ok(Event::Eof) => break,
            Err(_) => return None,
            _ => {}
        }
    }

    if document.urls.is_empty() && document.nested_sitemaps.is_empty() {
        None
    } else {
        Some(document)
    }
}
