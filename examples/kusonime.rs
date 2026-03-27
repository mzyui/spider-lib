use serde_json::{Value, json};
use spider_lib::prelude::*;

/// Scraped item model for a Kusonime detail page.
#[scraped_item]
pub struct KusonimeItem {
    pub source_url: String,
    pub title: String,
    pub description: String,
    pub metadata: Value,
    pub download_links: Value,
}

/// Example spider for crawling Kusonime listings and detail pages.
pub struct KusonimeSpider;

#[async_trait]
impl Spider for KusonimeSpider {
    type Item = KusonimeItem;
    type State = ();

    fn start_requests(&self) -> Result<StartRequests<'_>, SpiderError> {
        Ok(StartRequests::Urls(vec!["https://kusonime.com/"]))
    }

    async fn parse(
        &self,
        response: Response,
        _state: &Self::State,
    ) -> Result<ParseOutput<Self::Item>, SpiderError> {
        let mut output = ParseOutput::new();

        if !response.css("#dl .smokeurlrh")?.is_empty() {
            let title = clean_title(&first_text(&response, "h1.jdlz")?);
            let description = extract_description(&response)?;
            let metadata = extract_metadata(&response)?;
            let download_links = extract_download_links(&response)?;

            output.add_item(KusonimeItem {
                source_url: response.url.to_string(),
                title,
                description,
                metadata,
                download_links,
            });

            return Ok(output);
        }

        for entry in response.css(".kover .content h2.episodeye a[href]")? {
            if let Some(href) = entry.attrib("href") {
                output.add_request(Request::new(response.url.join(&href)?));
            }
        }

        if let Some(next_href) = response
            .css(".pagination .nextpostslink[href], link[rel='next'][href]::attr(href)")?
            .get()
        {
            output.add_request(Request::new(response.url.join(&next_href)?));
        }

        Ok(output)
    }
}

fn first_text(response: &Response, selector: &str) -> Result<String, SpiderError> {
    Ok(response
        .css(&format!("{selector}::text"))?
        .get()
        .map(|text| normalize_whitespace(&text))
        .unwrap_or_default())
}

fn extract_description(response: &Response) -> Result<String, SpiderError> {
    let mut paragraphs = Vec::new();

    for paragraph in response.css(".venutama p")? {
        if has_excluded_ancestor(&paragraph)? || paragraph.has_css("a[href]")? {
            continue;
        }

        let text = clean_description_text(&normalize_whitespace(
            &paragraph.text_content().unwrap_or_default(),
        ));
        if text.is_empty() || text == "\u{a0}" || text.chars().count() < 80 {
            continue;
        }

        paragraphs.push(text);
    }

    let description = paragraphs.join("\n\n");
    if !description.is_empty() {
        return Ok(description);
    }

    Ok(response
        .css("meta[name='description']::attr(content)")?
        .get()
        .map(|text| normalize_whitespace(&text))
        .map(|text| clean_description_text(&text))
        .unwrap_or_default())
}

fn extract_metadata(response: &Response) -> Result<Value, SpiderError> {
    let mut entries = Vec::new();

    for paragraph in response.css(".info p")? {
        let full_text = normalize_whitespace(&paragraph.text_content().unwrap_or_default());
        let raw_label = paragraph
            .css("b::text")?
            .get()
            .map(|text| normalize_whitespace(&text))
            .unwrap_or_default();
        let raw_label = clean_metadata_part(&raw_label);
        let label = normalize_metadata_key(&raw_label);
        let value = extract_metadata_value(&full_text, &raw_label);

        if !label.is_empty() && !value.is_empty() {
            entries.push((label, Value::String(value)));
        }
    }

    Ok(Value::Object(entries.into_iter().collect()))
}

fn extract_download_links(response: &Response) -> Result<Value, SpiderError> {
    let mut resolutions = Vec::new();

    for block in response.css("#dl .smokeurlrh")? {
        let resolution = block
            .css("strong::text")?
            .get()
            .map(|text| normalize_whitespace(&text))
            .unwrap_or_default();

        let mut mirrors = Vec::new();
        for link in block.css("a[href]")? {
            let provider = normalize_whitespace(&link.text_content().unwrap_or_default());
            let url = link.attrib("href").unwrap_or_default().trim().to_string();

            if !provider.is_empty() && !url.is_empty() {
                mirrors.push(json!({
                    "provider": provider,
                    "url": url,
                }));
            }
        }

        if !resolution.is_empty() && !mirrors.is_empty() {
            resolutions.push(json!({
                "resolution": resolution,
                "links": mirrors,
            }));
        }
    }

    Ok(Value::Array(resolutions))
}

fn normalize_whitespace(input: &str) -> String {
    input.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn clean_title(input: &str) -> String {
    let title = input.trim();
    let words: Vec<&str> = title.split_whitespace().collect();
    let mut cutoff = words.len();
    let mut saw_release_descriptor = false;

    while cutoff > 0 {
        let token = words[cutoff - 1];
        if is_release_descriptor_token(token) {
            saw_release_descriptor = true;
            cutoff -= 1;
            continue;
        }

        if saw_release_descriptor && is_title_separator_token(token) {
            cutoff -= 1;
        }

        break;
    }

    if saw_release_descriptor {
        words[..cutoff].join(" ").trim().to_string()
    } else {
        title.to_string()
    }
}

fn is_release_descriptor_token(token: &str) -> bool {
    let normalized = token
        .trim_matches(|ch: char| !ch.is_ascii_alphanumeric())
        .to_ascii_lowercase();

    matches!(
        normalized.as_str(),
        "batch" | "sub" | "subtitle" | "indo" | "indonesia"
    )
}

fn is_title_separator_token(token: &str) -> bool {
    token
        .chars()
        .all(|ch| ch.is_ascii_punctuation() || ch.is_whitespace())
}

fn clean_metadata_part(input: &str) -> String {
    input.trim().trim_matches(':').trim().to_string()
}

fn extract_metadata_value(full_text: &str, label: &str) -> String {
    if label.is_empty() {
        return String::new();
    }

    let remainder = full_text
        .split_once(':')
        .map(|(_, rest)| rest)
        .unwrap_or_else(|| full_text.strip_prefix(label).unwrap_or_default());

    clean_metadata_part(remainder)
}

fn normalize_metadata_key(input: &str) -> String {
    input
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect::<String>()
        .split('_')
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join("_")
}

fn clean_description_text(input: &str) -> String {
    let trimmed = input.trim();

    if starts_with_download_promo(trimmed) {
        return String::new();
    }

    if let Some(index) = find_download_marker(trimmed) {
        return trimmed[..index].trim_end().to_string();
    }

    trimmed.to_string()
}

fn starts_with_download_promo(input: &str) -> bool {
    let lower = input.to_ascii_lowercase();
    lower.starts_with("download ")
        || lower.starts_with("link download ")
        || lower.starts_with("batch sub indo")
}

fn find_download_marker(input: &str) -> Option<usize> {
    let markers = [" Download ", ". Download ", "! Download ", "? Download "];

    markers
        .iter()
        .filter_map(|marker| {
            input
                .find(marker)
                .map(|index| index + marker.len() - "Download ".len())
        })
        .min()
}

fn has_excluded_ancestor(element: &SelectorNode) -> Result<bool, SpiderError> {
    element.has_ancestor(
        ".info, .dlbodz, .infolink, .socialshare, .tagser, .kategoz, .rtd, #dl, #dl-notif",
    )
}

#[tokio::main]
async fn main() -> Result<(), SpiderError> {
    let crawler = CrawlerBuilder::new(KusonimeSpider)
        .live_stats(true)
        .live_stats_preview_fields(["title"])
        .add_pipeline(StreamJsonPipeline::new("output/kusonime-stream.json")?)
        .build()
        .await?;

    crawler.start_crawl().await?;

    Ok(())
}
