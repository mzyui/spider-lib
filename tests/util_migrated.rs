use bytes::Bytes;
use reqwest::StatusCode;
use spider_util::metrics::{MetricsDisplayFormatter, MetricsSnapshot, format_plain_text_metrics};
use spider_util::response::{Link, LinkExtractOptions, LinkSource, LinkType, Response};
use std::collections::HashMap;
use std::time::Duration;
use url::Url;

fn sample_snapshot() -> MetricsSnapshot {
    let mut response_status_counts = HashMap::new();
    response_status_counts.insert(500, 2);
    response_status_counts.insert(200, 10);
    response_status_counts.insert(404, 1);

    MetricsSnapshot {
        requests_enqueued: 10,
        requests_sent: 10,
        requests_succeeded: 9,
        requests_failed: 1,
        requests_retried: 2,
        requests_scheduled_for_retry: 4,
        requests_dropped: 3,
        retry_delay_in_flight_ms: 250,
        responses_received: 10,
        responses_from_cache: 4,
        total_bytes_downloaded: 1_536,
        items_scraped: 8,
        items_processed: 7,
        items_dropped_by_pipeline: 1,
        response_status_counts,
        elapsed_duration: Duration::from_secs(33),
        average_request_time: Some(Duration::from_millis(508)),
        fastest_request_time: Some(Duration::from_millis(274)),
        slowest_request_time: Some(Duration::from_millis(1_850)),
        request_time_count: 10,
        average_parsing_time: Some(Duration::from_millis(4)),
        fastest_parsing_time: Some(Duration::from_millis(0)),
        slowest_parsing_time: Some(Duration::from_millis(27)),
        parsing_time_count: 9,
        recent_requests_per_second: 33.77,
        recent_responses_per_second: 34.80,
        recent_items_per_second: 33.12,
    }
}

#[test]
fn plain_text_metrics_use_aligned_terminal_layout() {
    let report = format_plain_text_metrics(&sample_snapshot());

    assert_eq!(
        report,
        "Crawl Statistics\n\
         ----------------\n\
         duration : 33s\n\
         speed    : req/s 0.30, resp/s 0.30, item/s 0.24\n\
         requests : enqueued 10, sent 10, pending 0, ok 9, fail 1\n\
         retry    : retry 2, scheduled 4, drop 3\n\
         ratios   : success 90.00%, failure 10.00%, cache hit 40.00%\n\
         response : received 10, cache 4, downloaded 1.50 KB, bytes/s 46 B/s\n\
         delay    : retry in flight 250 ms\n\
         items    : scraped 8, processed 7, dropped 1\n\
         req time : avg 508 ms, fastest 274 ms, slowest 1.85 s, total 10\n\
         parsing  : avg 4 ms, fastest 0 ms, slowest 27 ms, total 9\n\
         status   : 200: 10, 404: 1, 500: 2"
    );
}

#[test]
fn metrics_display_formatter_preserves_wrapping_newlines() {
    let report = MetricsDisplayFormatter.format_metrics(&sample_snapshot());

    assert!(report.starts_with('\n'));
    assert!(report.ends_with('\n'));
    assert!(report.contains("status   : 200: 10, 404: 1, 500: 2"));
}

#[test]
fn plain_text_metrics_render_empty_status_as_none() {
    let mut snapshot = sample_snapshot();
    snapshot.response_status_counts.clear();
    snapshot.average_request_time = None;
    snapshot.fastest_request_time = None;
    snapshot.slowest_request_time = None;
    snapshot.average_parsing_time = None;
    snapshot.fastest_parsing_time = None;
    snapshot.slowest_parsing_time = None;

    let report = format_plain_text_metrics(&snapshot);

    assert!(report.contains("ratios   : success 90.00%, failure 10.00%, cache hit 40.00%"));
    assert!(report.contains("req time : avg N/A, fastest N/A, slowest N/A, total 10"));
    assert!(report.contains("parsing  : avg N/A, fastest N/A, slowest N/A, total 9"));
    assert!(report.ends_with("status   : none"));
}

#[test]
fn plain_text_metrics_handle_zero_denominator_ratios() {
    let mut snapshot = sample_snapshot();
    snapshot.requests_sent = 0;
    snapshot.requests_succeeded = 0;
    snapshot.requests_failed = 0;
    snapshot.responses_received = 0;
    snapshot.responses_from_cache = 0;
    snapshot.requests_enqueued = 5;

    let report = format_plain_text_metrics(&snapshot);

    assert!(report.contains("requests : enqueued 5, sent 0, pending 2, ok 0, fail 0"));
    assert!(report.contains("retry    : retry 2, scheduled 4, drop 3"));
    assert!(report.contains("ratios   : success 0.00%, failure 0.00%, cache hit 0.00%"));
}

#[test]
fn plain_text_metrics_treat_dropped_pre_download_requests_as_not_pending() {
    let mut snapshot = sample_snapshot();
    snapshot.requests_enqueued = 170;
    snapshot.requests_sent = 145;
    snapshot.requests_succeeded = 145;
    snapshot.requests_failed = 0;
    snapshot.requests_dropped = 25;

    let report = format_plain_text_metrics(&snapshot);

    assert!(report.contains("requests : enqueued 170, sent 145, pending 0, ok 145, fail 0"));
    assert!(report.contains("retry    : retry 2, scheduled 4, drop 25"));
}

fn response(body: &str) -> Response {
    let url = Url::parse("https://example.com/base/").unwrap();

    Response {
        url: url.clone(),
        status: StatusCode::OK,
        headers: http::header::HeaderMap::new(),
        body: Bytes::from(body.to_owned()),
        request_url: url,
        meta: None,
        cached: false,
    }
}

fn has_link(links: &[Link], url: &str, link_type: LinkType) -> bool {
    let url = Url::parse(url).unwrap();
    links
        .iter()
        .any(|link| link.url == url && link.link_type == link_type)
}

#[test]
fn links_keeps_default_same_site_behavior_and_deduplicates() {
    let response = response(
        r#"
        <html>
            <body>
                <a href="/page">Page</a>
                <a href="https://external.test/page">External</a>
                <a href="/page">Duplicate</a>
                <script src="/app.js"></script>
                Text URL https://example.com/page
            </body>
        </html>
        "#,
    );

    let links = response.links();

    assert_eq!(links.len(), 2);
    assert!(links.contains(&Link {
        url: Url::parse("https://example.com/page").unwrap(),
        link_type: LinkType::Page,
    }));
    assert!(links.contains(&Link {
        url: Url::parse("https://example.com/app.js").unwrap(),
        link_type: LinkType::Script,
    }));
}

#[test]
fn links_iter_can_include_external_links() {
    let response = response(r#"<a href="https://external.test/page">External</a>"#);

    let links: Vec<_> = response
        .links_iter(LinkExtractOptions::default().same_site_only(false))
        .collect();

    assert!(has_link(
        &links,
        "https://external.test/page",
        LinkType::Page
    ));
}

#[test]
fn links_iter_can_disable_text_links() {
    let response = response(r#"Text URL https://example.com/from-text"#);

    let links: Vec<_> = response
        .links_iter(LinkExtractOptions::default().include_text_links(false))
        .collect();

    assert!(links.is_empty());
}

#[test]
fn links_iter_supports_custom_sources() {
    let response = response(r#"<div data-href="/promo"></div>"#);
    let options = LinkExtractOptions::default().with_sources([]).add_source(
        LinkSource::new("div[data-href]", "data-href")
            .with_link_type(LinkType::Other("promo".to_string())),
    );

    let links: Vec<_> = response.links_iter(options).collect();

    assert!(has_link(
        &links,
        "https://example.com/promo",
        LinkType::Other("promo".to_string())
    ));
}

#[test]
fn links_iter_can_filter_by_link_type() {
    let response = response(
        r#"
        <a href="/page">Page</a>
        <img src="/image.png" />
        "#,
    );

    let links: Vec<_> = response
        .links_iter(LinkExtractOptions::default().with_allowed_link_types([LinkType::Image]))
        .collect();

    assert_eq!(links.len(), 1);
    assert!(has_link(
        &links,
        "https://example.com/image.png",
        LinkType::Image
    ));
}
