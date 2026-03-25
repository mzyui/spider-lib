//! Configuration types used by the crawler runtime.
//!
//! Most users touch these settings indirectly through [`crate::CrawlerBuilder`],
//! but they are public because they are also useful for explicit configuration
//! and inspection.
//!
//! ## Example
//!
//! ```rust,ignore
//! use spider_core::config::{CrawlerConfig, CheckpointConfig};
//! use std::time::Duration;
//!
//! let crawler_config = CrawlerConfig::default()
//!     .with_max_concurrent_downloads(10)
//!     .with_parser_workers(4)
//!     .with_max_concurrent_pipelines(8)
//!     .with_channel_capacity(2000);
//!
//! let checkpoint_config = CheckpointConfig::builder()
//!     .path("./crawl.checkpoint")
//!     .interval(Duration::from_secs(60))
//!     .build();
//! ```

use std::path::{Path, PathBuf};
use std::time::Duration;

use spider_util::response::{LinkExtractOptions, LinkType};
use url::Url;

/// Runtime discovery mode applied to each downloaded response.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiscoveryMode {
    /// Disable framework-managed discovery.
    Disabled,
    /// Discover navigational HTML links only.
    HtmlLinks,
    /// Discover navigational HTML links and inject page metadata into response metadata.
    HtmlAndMetadata,
    /// Discover all supported resource types from HTML plus optional metadata.
    FullResources,
    /// Only process sitemap responses for follow-up URLs.
    SitemapOnly,
}

/// Rule-like configuration for runtime-managed discovery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscoveryRule {
    /// Stable rule name injected into request/response metadata when matched.
    pub name: String,
    /// URL patterns that the source response must match.
    pub allow_patterns: Vec<String>,
    /// URL patterns that exclude the source response.
    pub deny_patterns: Vec<String>,
    /// Domains or subdomains allowed for the source response.
    pub allow_domains: Vec<String>,
    /// Domains or subdomains denied for the source response.
    pub deny_domains: Vec<String>,
    /// Path prefixes allowed for the source response.
    pub allow_path_prefixes: Vec<String>,
    /// Path prefixes denied for the source response.
    pub deny_path_prefixes: Vec<String>,
    /// Link extraction behavior used when the rule matches.
    pub link_extract_options: LinkExtractOptions,
}

impl DiscoveryRule {
    /// Creates a new discovery rule with the provided name.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            allow_patterns: Vec::new(),
            deny_patterns: Vec::new(),
            allow_domains: Vec::new(),
            deny_domains: Vec::new(),
            allow_path_prefixes: Vec::new(),
            deny_path_prefixes: Vec::new(),
            link_extract_options: LinkExtractOptions::default(),
        }
    }

    /// Replaces the link extraction options used by this rule.
    pub fn with_link_extract_options(mut self, options: LinkExtractOptions) -> Self {
        self.link_extract_options = options;
        self
    }

    /// Restricts this rule to source response URLs that match at least one pattern.
    pub fn with_allow_patterns(
        mut self,
        patterns: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.allow_patterns = patterns.into_iter().map(Into::into).collect();
        self
    }

    /// Excludes this rule for source response URLs that match any pattern.
    pub fn with_deny_patterns(
        mut self,
        patterns: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.deny_patterns = patterns.into_iter().map(Into::into).collect();
        self
    }

    /// Restricts this rule to source response domains or subdomains.
    pub fn with_allow_domains(
        mut self,
        domains: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.allow_domains = domains.into_iter().map(normalize_domain_filter).collect();
        self
    }

    /// Excludes this rule for source response domains or subdomains.
    pub fn with_deny_domains(
        mut self,
        domains: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.deny_domains = domains.into_iter().map(normalize_domain_filter).collect();
        self
    }

    /// Restricts this rule to source response paths with one of the provided prefixes.
    pub fn with_allow_path_prefixes(
        mut self,
        prefixes: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.allow_path_prefixes = prefixes.into_iter().map(normalize_path_prefix).collect();
        self
    }

    /// Excludes this rule for source response paths with one of the provided prefixes.
    pub fn with_deny_path_prefixes(
        mut self,
        prefixes: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.deny_path_prefixes = prefixes.into_iter().map(normalize_path_prefix).collect();
        self
    }

    /// Sets whether only same-site links should be extracted for matching responses.
    pub fn with_same_site_only(mut self, enabled: bool) -> Self {
        self.link_extract_options.same_site_only = enabled;
        self
    }

    /// Sets whether text content should be scanned for plain-text URLs.
    pub fn with_text_links(mut self, enabled: bool) -> Self {
        self.link_extract_options.include_text_links = enabled;
        self
    }

    /// Restricts discovered follow-up links to matching patterns.
    pub fn with_follow_allow_patterns(
        mut self,
        patterns: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.link_extract_options = self.link_extract_options.with_allow_patterns(patterns);
        self
    }

    /// Excludes discovered follow-up links that match the given patterns.
    pub fn with_follow_deny_patterns(
        mut self,
        patterns: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.link_extract_options = self.link_extract_options.with_deny_patterns(patterns);
        self
    }

    /// Restricts discovered follow-up links to the given domains or subdomains.
    pub fn with_follow_allow_domains(
        mut self,
        domains: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.link_extract_options = self.link_extract_options.with_allow_domains(domains);
        self
    }

    /// Excludes discovered follow-up links for the given domains or subdomains.
    pub fn with_follow_deny_domains(
        mut self,
        domains: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.link_extract_options = self.link_extract_options.with_deny_domains(domains);
        self
    }

    /// Restricts discovered follow-up links to the provided path prefixes.
    pub fn with_follow_allow_path_prefixes(
        mut self,
        prefixes: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.link_extract_options = self.link_extract_options.with_allow_path_prefixes(prefixes);
        self
    }

    /// Excludes discovered follow-up links for the provided path prefixes.
    pub fn with_follow_deny_path_prefixes(
        mut self,
        prefixes: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.link_extract_options = self.link_extract_options.with_deny_path_prefixes(prefixes);
        self
    }

    /// Restricts attribute extraction to specific HTML tags for matching responses.
    pub fn with_allowed_tags(mut self, tags: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.link_extract_options = self.link_extract_options.with_allowed_tags(tags);
        self
    }

    /// Restricts attribute extraction to specific HTML attributes for matching responses.
    pub fn with_allowed_attributes(
        mut self,
        attributes: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.link_extract_options = self
            .link_extract_options
            .with_allowed_attributes(attributes);
        self
    }

    /// Restricts discovered follow-up links to the provided link types.
    pub fn with_allowed_link_types(
        mut self,
        link_types: impl IntoIterator<Item = LinkType>,
    ) -> Self {
        self.link_extract_options = self
            .link_extract_options
            .with_allowed_link_types(link_types);
        self
    }

    /// Excludes the provided link types from discovered follow-up links.
    pub fn with_denied_link_types(
        mut self,
        link_types: impl IntoIterator<Item = LinkType>,
    ) -> Self {
        self.link_extract_options = self.link_extract_options.with_denied_link_types(link_types);
        self
    }

    pub(crate) fn matches_response(&self, url: &Url) -> bool {
        let absolute_url = url.as_str();
        if !self.allow_patterns.is_empty()
            && !self
                .allow_patterns
                .iter()
                .any(|pattern| glob_matches(pattern, absolute_url))
        {
            return false;
        }

        if self
            .deny_patterns
            .iter()
            .any(|pattern| glob_matches(pattern, absolute_url))
        {
            return false;
        }

        let host = url.host_str().unwrap_or_default();
        if !self.allow_domains.is_empty()
            && !self
                .allow_domains
                .iter()
                .any(|domain| domain_matches(host, domain))
        {
            return false;
        }

        if self
            .deny_domains
            .iter()
            .any(|domain| domain_matches(host, domain))
        {
            return false;
        }

        let path = url.path();
        if !self.allow_path_prefixes.is_empty()
            && !self
                .allow_path_prefixes
                .iter()
                .any(|prefix| path.starts_with(prefix))
        {
            return false;
        }

        if self
            .deny_path_prefixes
            .iter()
            .any(|prefix| path.starts_with(prefix))
        {
            return false;
        }

        true
    }
}

/// Discovery-specific runtime configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscoveryConfig {
    /// How the runtime should discover follow-up work from responses.
    pub mode: DiscoveryMode,
    /// Whether sitemap XML should be parsed into follow-up requests.
    pub discover_sitemaps: bool,
    /// Maximum recursion depth for nested sitemap indexes.
    pub max_sitemap_depth: usize,
    /// Whether page metadata should be extracted and attached to response metadata.
    pub extract_page_metadata: bool,
    /// Base link extraction options used for HTML discovery.
    pub link_extract_options: LinkExtractOptions,
    /// Optional rule-like link discovery behavior matched against source responses.
    pub rules: Vec<DiscoveryRule>,
}

impl Default for DiscoveryConfig {
    fn default() -> Self {
        Self {
            mode: DiscoveryMode::Disabled,
            discover_sitemaps: false,
            max_sitemap_depth: 4,
            extract_page_metadata: false,
            link_extract_options: LinkExtractOptions::default(),
            rules: Vec::new(),
        }
    }
}

impl DiscoveryConfig {
    /// Creates a new discovery config with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the discovery mode.
    pub fn with_mode(mut self, mode: DiscoveryMode) -> Self {
        self.mode = mode;
        self
    }

    /// Enables or disables sitemap parsing.
    pub fn with_sitemaps(mut self, enabled: bool) -> Self {
        self.discover_sitemaps = enabled;
        self
    }

    /// Sets the maximum nested sitemap depth.
    pub fn with_max_sitemap_depth(mut self, depth: usize) -> Self {
        self.max_sitemap_depth = depth;
        self
    }

    /// Enables or disables page metadata extraction.
    pub fn with_page_metadata(mut self, enabled: bool) -> Self {
        self.extract_page_metadata = enabled;
        self
    }

    /// Replaces the base link extraction options.
    pub fn with_link_extract_options(mut self, options: LinkExtractOptions) -> Self {
        self.link_extract_options = options;
        self
    }

    /// Replaces the configured discovery rules.
    pub fn with_rules(mut self, rules: impl IntoIterator<Item = DiscoveryRule>) -> Self {
        self.rules = rules.into_iter().collect();
        self
    }

    /// Adds a single discovery rule.
    pub fn with_rule(mut self, rule: DiscoveryRule) -> Self {
        self.rules.push(rule);
        self
    }

    /// Sets whether only same-site links should be discovered.
    pub fn with_same_site_only(mut self, enabled: bool) -> Self {
        self.link_extract_options.same_site_only = enabled;
        self
    }

    /// Sets whether text content should be scanned for plain-text URLs.
    pub fn with_text_links(mut self, enabled: bool) -> Self {
        self.link_extract_options.include_text_links = enabled;
        self
    }

    /// Restricts discovery to URLs that match at least one glob-style pattern.
    pub fn with_allow_patterns(
        mut self,
        patterns: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.link_extract_options = self.link_extract_options.with_allow_patterns(patterns);
        self
    }

    /// Excludes URLs that match any glob-style pattern.
    pub fn with_deny_patterns(
        mut self,
        patterns: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.link_extract_options = self.link_extract_options.with_deny_patterns(patterns);
        self
    }

    /// Restricts discovery to the given domains or subdomains.
    pub fn with_allow_domains(
        mut self,
        domains: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.link_extract_options = self.link_extract_options.with_allow_domains(domains);
        self
    }

    /// Excludes discovery for the given domains or subdomains.
    pub fn with_deny_domains(
        mut self,
        domains: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.link_extract_options = self.link_extract_options.with_deny_domains(domains);
        self
    }

    /// Restricts discovery to URL paths with one of the provided prefixes.
    pub fn with_allow_path_prefixes(
        mut self,
        prefixes: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.link_extract_options = self.link_extract_options.with_allow_path_prefixes(prefixes);
        self
    }

    /// Excludes URL paths with one of the provided prefixes.
    pub fn with_deny_path_prefixes(
        mut self,
        prefixes: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.link_extract_options = self.link_extract_options.with_deny_path_prefixes(prefixes);
        self
    }

    /// Restricts attribute extraction to specific HTML tags.
    pub fn with_allowed_tags(mut self, tags: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.link_extract_options = self.link_extract_options.with_allowed_tags(tags);
        self
    }

    /// Restricts attribute extraction to specific attributes.
    pub fn with_allowed_attributes(
        mut self,
        attributes: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.link_extract_options = self
            .link_extract_options
            .with_allowed_attributes(attributes);
        self
    }

    /// Restricts discovery to the provided link types.
    pub fn with_allowed_link_types(
        mut self,
        link_types: impl IntoIterator<Item = LinkType>,
    ) -> Self {
        self.link_extract_options = self
            .link_extract_options
            .with_allowed_link_types(link_types);
        self
    }

    /// Excludes the provided link types from discovery.
    pub fn with_denied_link_types(
        mut self,
        link_types: impl IntoIterator<Item = LinkType>,
    ) -> Self {
        self.link_extract_options = self.link_extract_options.with_denied_link_types(link_types);
        self
    }

    /// Returns the effective link extraction options for the configured mode.
    pub fn effective_link_extract_options(&self) -> Option<LinkExtractOptions> {
        self.effective_link_extract_options_for(self.link_extract_options.clone())
    }

    /// Returns the effective link extraction options for a specific rule or override.
    pub fn effective_link_extract_options_for(
        &self,
        mut options: LinkExtractOptions,
    ) -> Option<LinkExtractOptions> {
        match self.mode {
            DiscoveryMode::Disabled | DiscoveryMode::SitemapOnly => None,
            DiscoveryMode::HtmlLinks | DiscoveryMode::HtmlAndMetadata => {
                if options.allowed_link_types.is_none() {
                    options.allowed_link_types = Some(vec![LinkType::Page]);
                }
                Some(options)
            }
            DiscoveryMode::FullResources => Some(options),
        }
    }

    /// Returns `true` when metadata extraction should run.
    pub fn should_extract_metadata(&self) -> bool {
        self.extract_page_metadata || matches!(self.mode, DiscoveryMode::HtmlAndMetadata)
    }
}

fn normalize_domain_filter(domain: impl Into<String>) -> String {
    domain
        .into()
        .trim()
        .trim_start_matches('.')
        .to_ascii_lowercase()
}

fn normalize_path_prefix(prefix: impl Into<String>) -> String {
    let prefix = prefix.into();
    let prefix = prefix.trim();
    if prefix.is_empty() || prefix == "/" {
        "/".to_string()
    } else if prefix.starts_with('/') {
        prefix.to_string()
    } else {
        format!("/{prefix}")
    }
}

fn domain_matches(host: &str, filter: &str) -> bool {
    let host = host.to_ascii_lowercase();
    let filter = filter.to_ascii_lowercase();
    host == filter || host.ends_with(&format!(".{filter}"))
}

fn glob_matches(pattern: &str, input: &str) -> bool {
    let pattern = pattern.as_bytes();
    let input = input.as_bytes();
    let (mut p, mut s) = (0usize, 0usize);
    let mut last_star = None;
    let mut match_after_star = 0usize;

    while s < input.len() {
        if p < pattern.len() && (pattern[p] == b'?' || pattern[p] == input[s]) {
            p += 1;
            s += 1;
        } else if p < pattern.len() && pattern[p] == b'*' {
            last_star = Some(p);
            p += 1;
            match_after_star = s;
        } else if let Some(star_idx) = last_star {
            p = star_idx + 1;
            match_after_star += 1;
            s = match_after_star;
        } else {
            return false;
        }
    }

    while p < pattern.len() && pattern[p] == b'*' {
        p += 1;
    }

    p == pattern.len()
}

/// Core runtime configuration for the crawler.
#[derive(Debug, Clone)]
pub struct CrawlerConfig {
    /// The maximum number of concurrent downloads.
    pub max_concurrent_downloads: usize,
    /// The maximum number of outstanding requests tracked by the scheduler.
    pub max_pending_requests: usize,
    /// The number of workers dedicated to parsing responses.
    pub parser_workers: usize,
    /// The maximum number of concurrent item processing pipelines.
    pub max_concurrent_pipelines: usize,
    /// The capacity of communication channels between components.
    pub channel_capacity: usize,
    /// Number of requests/items processed per parser output batch.
    pub output_batch_size: usize,
    /// Downloader backpressure threshold for the response channel.
    pub response_backpressure_threshold: usize,
    /// Parser backpressure threshold for the item channel.
    pub item_backpressure_threshold: usize,
    /// When enabled, retries are scheduled outside the downloader permit path.
    pub retry_release_permit: bool,
    /// Enables balanced browser-like default headers for the built-in reqwest downloader.
    pub browser_like_headers: bool,
    /// Enables in-place live statistics updates on terminal stdout.
    pub live_stats: bool,
    /// Refresh interval for live statistics output.
    pub live_stats_interval: Duration,
    /// Optional item fields to show in live-stats preview instead of full JSON.
    pub live_stats_preview_fields: Option<Vec<String>>,
    /// Maximum time to wait for a graceful shutdown before forcing task abort.
    pub shutdown_grace_period: Duration,
    /// Maximum number of scraped items to process before stopping the crawl.
    pub item_limit: Option<usize>,
    /// Response discovery behavior such as sitemap parsing and HTML link extraction.
    pub discovery: DiscoveryConfig,
}

impl Default for CrawlerConfig {
    fn default() -> Self {
        let cpu_count = num_cpus::get();
        let max_concurrent_downloads = (cpu_count * 4).clamp(8, 128);
        let max_pending_requests = (max_concurrent_downloads * 8).clamp(64, 4096);
        let parser_workers = (cpu_count * 2).clamp(4, 32);
        let max_concurrent_pipelines = (cpu_count * 2).clamp(4, 16);
        let channel_capacity = (max_pending_requests / 2).clamp(512, 4096);
        CrawlerConfig {
            max_concurrent_downloads,
            max_pending_requests,
            parser_workers,
            max_concurrent_pipelines,
            channel_capacity,
            output_batch_size: 64,
            response_backpressure_threshold: (max_concurrent_downloads * 6).min(channel_capacity),
            item_backpressure_threshold: (parser_workers * 6).min(channel_capacity),
            retry_release_permit: true,
            browser_like_headers: true,
            live_stats: false,
            live_stats_interval: Duration::from_millis(50),
            live_stats_preview_fields: None,
            shutdown_grace_period: Duration::from_secs(5),
            item_limit: None,
            discovery: DiscoveryConfig::default(),
        }
    }
}

impl CrawlerConfig {
    /// Creates a new `CrawlerConfig` with default settings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the maximum number of concurrent downloads.
    pub fn with_max_concurrent_downloads(mut self, limit: usize) -> Self {
        self.max_concurrent_downloads = limit;
        self
    }

    /// Sets the maximum number of outstanding requests tracked by the scheduler.
    pub fn with_max_pending_requests(mut self, limit: usize) -> Self {
        self.max_pending_requests = limit;
        self
    }

    /// Sets the number of parser workers.
    pub fn with_parser_workers(mut self, count: usize) -> Self {
        self.parser_workers = count;
        self
    }

    /// Sets the maximum number of concurrent pipelines.
    pub fn with_max_concurrent_pipelines(mut self, limit: usize) -> Self {
        self.max_concurrent_pipelines = limit;
        self
    }

    /// Sets the channel capacity.
    pub fn with_channel_capacity(mut self, capacity: usize) -> Self {
        self.channel_capacity = capacity;
        self
    }

    /// Sets the parser output batch size.
    pub fn with_output_batch_size(mut self, batch_size: usize) -> Self {
        self.output_batch_size = batch_size;
        self
    }

    /// Sets the downloader response-channel backpressure threshold.
    pub fn with_response_backpressure_threshold(mut self, threshold: usize) -> Self {
        self.response_backpressure_threshold = threshold;
        self
    }

    /// Sets the parser item-channel backpressure threshold.
    pub fn with_item_backpressure_threshold(mut self, threshold: usize) -> Self {
        self.item_backpressure_threshold = threshold;
        self
    }

    /// Controls whether retry delays release the downloader permit immediately.
    pub fn with_retry_release_permit(mut self, enabled: bool) -> Self {
        self.retry_release_permit = enabled;
        self
    }

    /// Enables or disables balanced browser-like default headers for the built-in reqwest downloader.
    pub fn with_browser_like_headers(mut self, enabled: bool) -> Self {
        self.browser_like_headers = enabled;
        self
    }

    /// Enables or disables in-place live stats updates on stdout.
    pub fn with_live_stats(mut self, enabled: bool) -> Self {
        self.live_stats = enabled;
        self
    }

    /// Sets the refresh interval used by live stats mode.
    pub fn with_live_stats_interval(mut self, interval: Duration) -> Self {
        self.live_stats_interval = interval;
        self
    }

    /// Sets which item fields should be shown in live stats preview output.
    ///
    /// Field names support dot notation for nested JSON objects, for example:
    /// `title`, `source_url`, or `metadata.Japanese`.
    ///
    /// You can also set aliases with `label=path`, for example:
    /// `url=source_url` or `jp=metadata.Japanese`.
    pub fn with_live_stats_preview_fields(
        mut self,
        fields: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.live_stats_preview_fields = Some(fields.into_iter().map(Into::into).collect());
        self
    }

    /// Sets the maximum grace period for crawler shutdown.
    pub fn with_shutdown_grace_period(mut self, grace_period: Duration) -> Self {
        self.shutdown_grace_period = grace_period;
        self
    }

    /// Sets the maximum number of scraped items to process before stopping the crawl.
    pub fn with_item_limit(mut self, limit: usize) -> Self {
        self.item_limit = Some(limit);
        self
    }

    /// Sets the discovery configuration.
    pub fn with_discovery(mut self, discovery: DiscoveryConfig) -> Self {
        self.discovery = discovery;
        self
    }

    /// Validates the configuration.
    pub fn validate(&self) -> Result<(), String> {
        if self.max_concurrent_downloads == 0 {
            return Err("max_concurrent_downloads must be greater than 0".to_string());
        }
        if self.max_pending_requests == 0 {
            return Err("max_pending_requests must be greater than 0".to_string());
        }
        if self.parser_workers == 0 {
            return Err("parser_workers must be greater than 0".to_string());
        }
        if self.max_concurrent_pipelines == 0 {
            return Err("max_concurrent_pipelines must be greater than 0".to_string());
        }
        if self.output_batch_size == 0 {
            return Err("output_batch_size must be greater than 0".to_string());
        }
        if self.response_backpressure_threshold == 0 {
            return Err("response_backpressure_threshold must be greater than 0".to_string());
        }
        if self.item_backpressure_threshold == 0 {
            return Err("item_backpressure_threshold must be greater than 0".to_string());
        }
        if self.live_stats_interval.is_zero() {
            return Err("live_stats_interval must be greater than 0".to_string());
        }
        if matches!(self.live_stats_preview_fields.as_ref(), Some(fields) if fields.is_empty()) {
            return Err("live_stats_preview_fields must not be empty".to_string());
        }
        if self.shutdown_grace_period.is_zero() {
            return Err("shutdown_grace_period must be greater than 0".to_string());
        }
        if matches!(self.item_limit, Some(0)) {
            return Err("item_limit must be greater than 0".to_string());
        }
        if self.discovery.max_sitemap_depth == 0 {
            return Err("discovery.max_sitemap_depth must be greater than 0".to_string());
        }
        Ok(())
    }
}

/// Configuration for checkpoint save/load operations.
///
/// This struct holds settings for automatic checkpoint persistence,
/// allowing crawls to be resumed after interruption.
#[derive(Debug, Clone, Default)]
pub struct CheckpointConfig {
    /// Optional path for saving and loading checkpoints.
    pub path: Option<PathBuf>,
    /// Optional interval between automatic checkpoint saves.
    pub interval: Option<Duration>,
}

impl CheckpointConfig {
    /// Creates a new `CheckpointConfig` with no path or interval.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a new `CheckpointConfigBuilder` for fluent construction.
    pub fn builder() -> CheckpointConfigBuilder {
        CheckpointConfigBuilder::default()
    }

    /// Sets the checkpoint path.
    pub fn with_path<P: AsRef<Path>>(mut self, path: P) -> Self {
        self.path = Some(path.as_ref().to_path_buf());
        self
    }

    /// Sets the checkpoint interval.
    pub fn with_interval(mut self, interval: Duration) -> Self {
        self.interval = Some(interval);
        self
    }

    /// Returns true if checkpointing is enabled.
    pub fn is_enabled(&self) -> bool {
        self.path.is_some()
    }
}

/// Builder for `CheckpointConfig`.
#[derive(Debug, Default)]
pub struct CheckpointConfigBuilder {
    path: Option<PathBuf>,
    interval: Option<Duration>,
}

impl CheckpointConfigBuilder {
    /// Creates a new builder with default settings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the checkpoint path.
    pub fn path<P: AsRef<Path>>(mut self, path: P) -> Self {
        self.path = Some(path.as_ref().to_path_buf());
        self
    }

    /// Sets the checkpoint interval.
    pub fn interval(mut self, interval: Duration) -> Self {
        self.interval = Some(interval);
        self
    }

    /// Builds the `CheckpointConfig`.
    pub fn build(self) -> CheckpointConfig {
        CheckpointConfig {
            path: self.path,
            interval: self.interval,
        }
    }
}

/// Configuration for the parser workers.
///
/// This struct holds settings specific to the response parsing subsystem.
#[derive(Debug, Clone)]
pub struct ParserConfig {
    /// The number of parser worker tasks to spawn.
    pub worker_count: usize,
    /// The capacity of the internal parse queue per worker.
    pub queue_capacity: usize,
}

impl Default for ParserConfig {
    fn default() -> Self {
        ParserConfig {
            worker_count: num_cpus::get().clamp(4, 16),
            queue_capacity: 100,
        }
    }
}

impl ParserConfig {
    /// Creates a new `ParserConfig` with default settings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the number of parser workers.
    pub fn with_worker_count(mut self, count: usize) -> Self {
        self.worker_count = count;
        self
    }

    /// Sets the internal queue capacity per worker.
    pub fn with_queue_capacity(mut self, capacity: usize) -> Self {
        self.queue_capacity = capacity;
        self
    }
}

/// Configuration for the downloader.
///
/// This struct holds settings specific to the HTTP download subsystem.
#[derive(Debug, Clone)]
pub struct DownloaderConfig {
    /// The maximum number of concurrent downloads.
    pub max_concurrent: usize,
    /// The backpressure threshold for response channel occupancy.
    pub backpressure_threshold: usize,
}

impl Default for DownloaderConfig {
    fn default() -> Self {
        let max_concurrent = num_cpus::get().max(16);
        DownloaderConfig {
            max_concurrent,
            backpressure_threshold: max_concurrent * 2,
        }
    }
}

impl DownloaderConfig {
    /// Creates a new `DownloaderConfig` with default settings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the maximum number of concurrent downloads.
    pub fn with_max_concurrent(mut self, limit: usize) -> Self {
        self.max_concurrent = limit;
        self
    }

    /// Sets the backpressure threshold.
    pub fn with_backpressure_threshold(mut self, threshold: usize) -> Self {
        self.backpressure_threshold = threshold;
        self
    }
}

/// Configuration for the item processor.
///
/// This struct holds settings specific to the item processing pipeline.
#[derive(Debug, Clone)]
pub struct ItemProcessorConfig {
    /// The maximum number of concurrent pipeline processors.
    pub max_concurrent: usize,
}

impl Default for ItemProcessorConfig {
    fn default() -> Self {
        ItemProcessorConfig {
            max_concurrent: num_cpus::get().min(8),
        }
    }
}

impl ItemProcessorConfig {
    /// Creates a new `ItemProcessorConfig` with default settings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the maximum number of concurrent processors.
    pub fn with_max_concurrent(mut self, limit: usize) -> Self {
        self.max_concurrent = limit;
        self
    }
}
