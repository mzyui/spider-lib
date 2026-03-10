use spider_downloader::ReqwestClientDownloader;
use spider_util::request::Request;
use url::Url;

#[test]
fn proxy_meta_parsing_returns_none_when_missing() {
    let request = Request::new(Url::parse("https://example.com").expect("valid url"));
    assert_eq!(ReqwestClientDownloader::test_proxy_from_request(&request), None);
}

#[test]
fn invalid_proxy_falls_back_without_error() {
    let downloader = ReqwestClientDownloader::new();
    let proxy_client = downloader.test_get_or_create_proxy_client("://invalid-proxy");
    assert!(proxy_client.is_none());
    assert_eq!(downloader.test_proxy_client_count(), 0);
}

#[test]
fn valid_proxy_client_is_cached_and_reused() {
    let downloader = ReqwestClientDownloader::new();
    let proxy = "http://127.0.0.1:8080";

    let first = downloader.test_get_or_create_proxy_client(proxy);
    assert!(first.is_some());
    assert!(downloader.test_has_proxy_client(proxy));

    let second = downloader.test_get_or_create_proxy_client(proxy);
    assert!(second.is_some());
    assert!(downloader.test_has_proxy_client(proxy));
}

#[test]
fn request_without_proxy_uses_base_client_path() {
    let downloader = ReqwestClientDownloader::new();
    let request = Request::new(Url::parse("https://example.com").expect("valid url"));

    let _ = downloader.test_select_client_for_request(&request);
    assert_eq!(downloader.test_proxy_client_count(), 0);
}
