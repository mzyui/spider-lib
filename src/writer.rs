use std::{
    future::Future,
    path::PathBuf,
    pin::Pin,
    task::{Context, Poll},
};

use tokio::fs::OpenOptions;
use tokio::io::{stderr, stdout, AsyncWriteExt};
use tower::{Layer, Service};

#[derive(Clone)]
pub enum WriterSink {
    Stdout,
    Stderr,
    File(PathBuf),
    Multi(Vec<WriterSink>),
}

#[derive(Clone)]
pub struct WriterLayer {
    sink: WriterSink,
}

impl WriterLayer {
    pub fn new(sink: WriterSink) -> Self {
        Self { sink }
    }
}

impl<S> Layer<S> for WriterLayer {
    type Service = WriterService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        WriterService {
            inner,
            sink: self.sink.clone(),
        }
    }
}

#[derive(Clone)]
pub struct WriterService<S> {
    inner: S,
    sink: WriterSink,
}

impl<S, Req, Res, Err> Service<Req> for WriterService<S>
where
    S: Service<Req, Response = Res, Error = Err> + Clone + Send + 'static,
    S::Future: Send + 'static,
    Req: Send + 'static,
    Res: std::fmt::Debug + Send + Sync + 'static,
    Err: std::fmt::Debug + Send + Sync + 'static,
{
    type Response = Res;
    type Error = Err;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Req) -> Self::Future {
        let mut inner = self.inner.clone();
        let sink = self.sink.clone();

        Box::pin(async move {
            let result = inner.call(req).await;

            // fire-and-forget writer
            let _ = write_to_sink(&sink, &result).await;

            result
        })
    }
}

async fn write_to_sink<Res, Err>(
    sink: &WriterSink,
    result: &Result<Res, Err>,
) -> std::io::Result<()>
where
    Res: std::fmt::Debug,
    Err: std::fmt::Debug,
{
    let line = format!("{:?}\n", result);

    match sink {
        WriterSink::Stdout => {
            let mut out = stdout();
            out.write_all(line.as_bytes()).await?;
        }

        WriterSink::Stderr => {
            let mut err = stderr();
            err.write_all(line.as_bytes()).await?;
        }

        WriterSink::File(path) => {
            let mut file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
                .await?;

            file.write_all(line.as_bytes()).await?;
        }

        WriterSink::Multi(sinks) => {
            for s in sinks {
                Box::pin(write_to_sink(s, result)).await?;
            }
        }
    }

    Ok(())
}
