use scraper::Html;
use spider_lib::prelude::*;
use std::collections::BTreeMap;
use url::Url;

const START_URL: &str = "https://kusonime.com/";
const OUTPUT_PATH: &str = "output/kusonime.json";

const CONTENT_KEYWORDS: &[&str] = &["anime", "film", "movie", "batch", "episode", "tv", "ova"];
const EXCLUDED_KEYWORDS: &[&str] = &[
    "login", "register", "signup", "search", "tag", "author", "feed", "comment", "wp-admin",
    "wp-login",
];

#[scraped_item]
pub struct KusonimeLinkItem {
    pub title: String,
    pub url: String,
    pub content_type: String,
    pub meta: BTreeMap<String, String>,
    pub downloads: Vec<DownloadLink>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DownloadLink {
    pub section: String,
    pub provider: String,
    pub url: String,
}

pub struct KusonimeSpider;

#[async_trait]
impl Spider for KusonimeSpider {
    type Item = KusonimeLinkItem;
    type State = ();

    fn start_requests(&self) -> Result<StartRequests<'_>, SpiderError> {
        Ok(StartRequests::Urls(vec![START_URL]))
    }

    async fn parse(
        &self,
        response: Response,
        _state: &Self::State,
    ) -> Result<ParseOutput<Self::Item>, SpiderError> {
        let html = response.to_html()?;
        let mut output = ParseOutput::new();

        if is_detail_page(&html)? {
            if let Some(item) = extract_detail_item(&html, &response)? {
                output.add_item(item);
            }
            return Ok(output);
        }

        let anchor_selector = "a[href]".to_selector()?;
        let mut queued_urls = std::collections::HashSet::new();
        let mut next_page_url = None;

        for anchor in html.select(&anchor_selector) {
            let Some(raw_href) = anchor.attr("href") else {
                continue;
            };

            if should_skip_href(raw_href) {
                continue;
            }

            let Ok(url) = response.url.join(raw_href) else {
                continue;
            };

            if !is_supported_url(&url) || !is_same_site(&response.url, &url) {
                continue;
            }

            let normalized_url = normalize_url(&url);
            let anchor_text = normalize_text(&anchor.text().collect::<String>());
            let haystack = format!(
                "{} {}",
                normalized_url.to_lowercase(),
                anchor_text.to_lowercase()
            );

            if next_page_url.is_none()
                && is_next_page_link(
                    &anchor_text,
                    &url,
                    anchor.attr("rel"),
                    anchor.attr("class"),
                    &response.url,
                )
            {
                next_page_url = Some(url.clone());
            }

            if is_content_candidate(&haystack) {
                if queued_urls.insert(normalized_url.clone()) {
                    output.add_request(Request::new(url));
                }

                continue;
            }
        }

        if let Some(next_page_url) = next_page_url {
            output.add_request(Request::new(next_page_url));
        }

        Ok(output)
    }
}

fn should_skip_href(href: &str) -> bool {
    let trimmed = href.trim();
    trimmed.is_empty()
        || trimmed.starts_with('#')
        || trimmed.starts_with("javascript:")
        || trimmed.starts_with("mailto:")
        || trimmed.starts_with("tel:")
}

fn is_supported_url(url: &Url) -> bool {
    matches!(url.scheme(), "http" | "https")
}

fn normalize_url(url: &Url) -> String {
    let mut normalized = url.clone();
    normalized.set_fragment(None);

    let path = normalized.path().trim_end_matches('/').to_string();
    if path.is_empty() {
        normalized.set_path("/");
    } else {
        normalized.set_path(&path);
    }

    normalized.to_string()
}

fn normalize_text(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn contains_any_keyword(haystack: &str, keywords: &[&str]) -> bool {
    keywords.iter().any(|keyword| haystack.contains(keyword))
}

fn is_detail_page(html: &Html) -> Result<bool, SpiderError> {
    Ok(html
        .select(&".venutama .lexot .info".to_selector()?)
        .next()
        .is_some()
        && html
            .select(&".dlbodz #dl .smokeurlrh".to_selector()?)
            .next()
            .is_some())
}

fn is_content_candidate(haystack: &str) -> bool {
    contains_any_keyword(haystack, CONTENT_KEYWORDS)
        && !contains_any_keyword(haystack, EXCLUDED_KEYWORDS)
        && !haystack.contains("/category/")
        && !haystack.contains("/tag/")
        && !haystack.ends_with('/')
}

fn extract_detail_item(
    html: &Html,
    response: &Response,
) -> Result<Option<KusonimeLinkItem>, SpiderError> {
    let title = html
        .select(&".post-thumb .jdlz".to_selector()?)
        .next()
        .map(|node| normalize_text(&node.text().collect::<String>()))
        .filter(|text| !text.is_empty())
        .unwrap_or_else(|| best_title("", &response.url));

    let meta = extract_metadata(html)?;
    let downloads = extract_download_links(html, response)?;

    if downloads.is_empty() {
        return Ok(None);
    }

    let meta_haystack = format!(
        "{} {}",
        title.to_lowercase(),
        meta.values()
            .map(|value| value.to_lowercase())
            .collect::<Vec<_>>()
            .join(" ")
    );

    Ok(Some(KusonimeLinkItem {
        title,
        url: normalize_url(&response.url),
        content_type: detect_content_type(&meta_haystack).to_string(),
        meta,
        downloads,
    }))
}

fn extract_metadata(html: &Html) -> Result<BTreeMap<String, String>, SpiderError> {
    let info_selector = ".venutama .lexot .info p".to_selector()?;
    let label_selector = "b".to_selector()?;
    let mut meta = BTreeMap::new();

    for row in html.select(&info_selector) {
        let label = row
            .select(&label_selector)
            .next()
            .map(|node| normalize_text(&node.text().collect::<String>()))
            .unwrap_or_default()
            .trim_matches(':')
            .trim()
            .to_string();

        if label.is_empty() {
            continue;
        }

        let value = normalize_text(&row.text().collect::<String>())
            .replace(&label, "")
            .trim()
            .trim_start_matches(':')
            .trim()
            .to_string();

        if !value.is_empty() {
            meta.insert(label, value);
        }
    }

    Ok(meta)
}

fn extract_download_links(
    html: &Html,
    response: &Response,
) -> Result<Vec<DownloadLink>, SpiderError> {
    let row_selector = ".dlbodz #dl .smokeurlrh".to_selector()?;
    let strong_selector = "strong".to_selector()?;
    let link_selector = "a[href]".to_selector()?;
    let mut downloads = Vec::new();

    for row in html.select(&row_selector) {
        let section = row
            .select(&strong_selector)
            .next()
            .map(|node| normalize_text(&node.text().collect::<String>()))
            .unwrap_or_else(|| "Unknown".to_string());

        for link in row.select(&link_selector) {
            let Some(raw_href) = link.attr("href") else {
                continue;
            };

            if should_skip_href(raw_href) {
                continue;
            }

            let Ok(url) = response.url.join(raw_href) else {
                continue;
            };

            if !is_supported_url(&url) {
                continue;
            }

            let provider = normalize_text(&link.text().collect::<String>());
            downloads.push(DownloadLink {
                section: section.clone(),
                provider: if provider.is_empty() {
                    "Unknown".to_string()
                } else {
                    provider
                },
                url: url.to_string(),
            });
        }
    }

    Ok(downloads)
}

fn is_next_page_link(
    anchor_text: &str,
    url: &Url,
    rel: Option<&str>,
    class: Option<&str>,
    base_url: &Url,
) -> bool {
    let rel = rel.unwrap_or_default().to_lowercase();
    let class = class.unwrap_or_default().to_lowercase();
    let text = anchor_text.to_lowercase();

    let is_next_link = rel.contains("next")
        || class.contains("next")
        || text == "next"
        || text == "next page"
        || text == "older posts"
        || text.contains("next »")
        || text.contains("selanjutnya")
        || text == ">";

    is_next_link && is_supported_url(url) && is_same_site(base_url, url)
}

fn detect_content_type(haystack: &str) -> &'static str {
    if haystack.contains("film") || haystack.contains("movie") {
        "film"
    } else {
        "anime"
    }
}

fn best_title(anchor_text: &str, url: &Url) -> String {
    if !anchor_text.is_empty() {
        return anchor_text.to_string();
    }

    url.path_segments()
        .and_then(|segments| segments.filter(|segment| !segment.is_empty()).next_back())
        .map(|segment| segment.replace('-', " "))
        .unwrap_or_else(|| url.as_str().to_string())
}

#[tokio::main]
async fn main() -> Result<(), SpiderError> {
    let crawler = CrawlerBuilder::new(KusonimeSpider)
        .live_stats(true)
        //        .add_middleware(RetryMiddleware::new().max_retries(2))
        .add_pipeline(DeduplicationPipeline::new(&["url"]))
        .add_pipeline(StreamJsonPipeline::new(OUTPUT_PATH)?)
        .build()
        .await?;

    crawler.start_crawl().await?;

    Ok(())
}
