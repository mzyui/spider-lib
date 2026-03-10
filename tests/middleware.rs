#[cfg(feature = "middleware-proxy")]
use spider_middleware::middleware::{Middleware, MiddlewareAction};
#[cfg(feature = "middleware-proxy")]
use spider_middleware::proxy::{ProxyMiddleware, ProxyRotationStrategy, ProxySource};
#[cfg(feature = "middleware-proxy")]
use spider_util::error::{ReqwestError, SpiderError};
#[cfg(feature = "middleware-proxy")]
use spider_util::request::Request;
#[cfg(feature = "middleware-proxy")]
use url::Url;

#[cfg(feature = "middleware-proxy")]
#[tokio::test]
async fn sticky_failover_rotates_proxy_on_error() {
    let mut middleware = ProxyMiddleware::builder()
        .source(ProxySource::List(vec![
            "http://proxy-1.local:8080".to_string(),
            "http://proxy-2.local:8080".to_string(),
        ]))
        .strategy(ProxyRotationStrategy::StickyFailover)
        .build()
        .expect("proxy middleware should build");

    let initial_request = Request::new(Url::parse("https://example.com").expect("valid url"));
    let first = middleware
        .process_request(&(), initial_request)
        .await
        .expect("proxy assignment should succeed");
    let first = match first {
        MiddlewareAction::Continue(request) => request,
        _ => panic!("proxy middleware should continue"),
    };

    assert_eq!(
        first
            .get_meta("proxy")
            .expect("proxy metadata should be set"),
        serde_json::Value::String("http://proxy-1.local:8080".to_string())
    );

    let error = SpiderError::ReqwestError(ReqwestError {
        message: "timeout".to_string(),
        is_connect: false,
        is_timeout: true,
    });
    let _ = <ProxyMiddleware as Middleware<()>>::handle_error(&mut middleware, &first, &error)
        .await;

    let second = middleware
        .process_request(
            &(),
            Request::new(Url::parse("https://example.com").expect("valid url")),
        )
        .await
        .expect("proxy assignment after rotation should succeed");
    let second = match second {
        MiddlewareAction::Continue(request) => request,
        _ => panic!("proxy middleware should continue"),
    };

    assert_eq!(
        second
            .get_meta("proxy")
            .expect("proxy metadata should be set"),
        serde_json::Value::String("http://proxy-2.local:8080".to_string())
    );
}
