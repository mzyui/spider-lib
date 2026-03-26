use spider_lib::prelude::*;

#[scraped_item]
pub struct ShowcaseItem {
    pub title: String,
    pub url: String,
    pub status: u16,
    pub body_bytes: usize,
    pub cached: bool,
    pub pages_seen: usize,
    pub total_bytes_seen: u64,
    pub first_visit: bool,
    pub note: Option<String>,
}

#[derive(Clone, Default)]
pub struct ShowcaseState {
    pub pages_seen: Counter,
    pub total_bytes_seen: Counter64,
    pub saw_cached_response: Flag,
    pub visited_urls: VisitedUrls,
    pub status_counts: ConcurrentMap<String, usize>,
    pub titles_seen: ConcurrentVec<String>,
    pub access_metrics: StateAccessMetrics,
}

impl ShowcaseState {
    pub fn record_response(&self, response: &Response, title: &str) {
        self.access_metrics.record_access_start();
        self.access_metrics.record_read();
        self.access_metrics.record_write();

        self.pages_seen.inc();
        self.total_bytes_seen.add(response.body.len() as u64);

        if response.cached {
            self.saw_cached_response.set(true);
        }

        let url = response.url.to_string();
        if !self.visited_urls.is_visited(&url) {
            self.visited_urls.mark(url);
        }

        let status_key = response.status.as_u16().to_string();
        let next_count = self.status_counts.get(&status_key).unwrap_or(0) + 1;
        self.status_counts.insert(status_key, next_count);

        if !title.is_empty() {
            self.titles_seen.push(title.to_string());
        }

        self.access_metrics.record_access_end();
    }

    pub fn summary(&self) -> String {
        format!(
            "pages={} bytes={} visited={} titles={} cached={} reads={} writes={} peak={}",
            self.pages_seen.get(),
            self.total_bytes_seen.get(),
            self.visited_urls.len(),
            self.titles_seen.len(),
            self.saw_cached_response.get(),
            self.access_metrics.read_count(),
            self.access_metrics.write_count(),
            self.access_metrics.concurrent_access_peak()
        )
    }
}

pub struct ShowcaseSpider;

#[async_trait]
impl Spider for ShowcaseSpider {
    type Item = ShowcaseItem;
    type State = ShowcaseState;

    fn start_requests(&self) -> Result<StartRequests<'_>, SpiderError> {
        Ok(StartRequests::iter(
            vec![Ok(Request::new("https://example.com/".parse()?))].into_iter(),
        ))
    }

    async fn parse(
        &self,
        response: Response,
        state: &Self::State,
    ) -> Result<ParseOutput<Self::Item>, SpiderError> {
        let html = response.to_html()?;
        let mut output = ParseOutput::new();

        let title = html
            .select(&"h1".to_selector()?)
            .next()
            .map(|node| node.text().collect::<String>())
            .unwrap_or_else(|| "Example Domain".to_string())
            .trim()
            .to_string();

        let url = response.url.to_string();
        let first_visit = !state.visited_urls.is_visited(&url);

        state.record_response(&response, &title);

        output.add_item(ShowcaseItem {
            title,
            url,
            status: response.status.as_u16(),
            body_bytes: response.body.len(),
            cached: response.cached,
            pages_seen: state.pages_seen.get(),
            total_bytes_seen: state.total_bytes_seen.get(),
            first_visit,
            note: Some(state.summary()),
        });

        Ok(output)
    }
}

pub fn prepare_output_dir() -> Result<(), SpiderError> {
    create_dir("output")
}
