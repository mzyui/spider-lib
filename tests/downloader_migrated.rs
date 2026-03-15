use spider_downloader::{Downloader, ReqwestClientDownloader};
use spider_util::request::Request;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::thread;
use std::time::Duration;
use url::Url;

#[tokio::test]
async fn form_requests_preserve_expected_payload_shape() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    let server = thread::spawn(move || -> String {
        let (mut stream, _) = listener.accept().unwrap();
        let mut buffer = [0_u8; 4096];
        let bytes_read = stream.read(&mut buffer).unwrap();
        let request_text = String::from_utf8_lossy(&buffer[..bytes_read]).into_owned();

        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nOK")
            .unwrap();

        request_text
    });

    let form = dashmap::DashMap::from_iter([
        ("alpha".to_string(), "one".to_string()),
        ("beta".to_string(), "two words".to_string()),
    ]);

    let downloader = ReqwestClientDownloader::new_with_timeout(Duration::from_secs(5));
    let request =
        Request::new(Url::parse(&format!("http://{addr}/submit")).unwrap()).with_form(form);

    let response = downloader.download(request).await.unwrap();
    assert_eq!(response.status.as_u16(), 200);

    let raw_request = server.join().unwrap();
    assert!(raw_request.starts_with("POST /submit HTTP/1.1"));
    assert!(raw_request.contains("content-type: application/x-www-form-urlencoded"));
    assert!(raw_request.contains(
        "user-agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36"
    ));

    let body = raw_request.split("\r\n\r\n").nth(1).unwrap_or_default();
    assert!(body.contains("alpha=one"));
    assert!(body.contains("beta=two+words"));
}
