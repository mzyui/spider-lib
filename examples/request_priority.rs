use spider_lib::prelude::*;

#[tokio::main]
async fn main() -> Result<(), SpiderError> {
    let (scheduler, request_rx) = Scheduler::new(None, 16);

    scheduler
        .enqueue_requests_batch(vec![
            Request::try_new("https://example.com/list")?,
            Request::try_new("https://example.com/archive")?.with_priority(-5),
            Request::try_new("https://example.com/detail/important")?.with_priority(10),
            Request::try_new("https://example.com/detail/also-important")?.with_priority(4),
        ])
        .await?;

    println!("Dequeued order:");

    for index in 1..=4 {
        let request = request_rx.recv().await.map_err(|err| {
            SpiderError::GeneralError(format!("scheduler receiver closed unexpectedly: {}", err))
        })?;
        println!(
            "{}. priority={} url={}",
            index,
            request.priority(),
            request.url
        );
        scheduler.complete_request();
    }

    scheduler.shutdown().await?;
    Ok(())
}
