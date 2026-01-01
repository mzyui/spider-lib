use kanal::{AsyncReceiver, AsyncSender};
use std::marker::PhantomData;
use tokio::task::JoinSet;
use tower::{
    buffer::BufferLayer,
    layer::util::{Identity, Stack},
    limit::ConcurrencyLimitLayer,
    Layer, Service, ServiceBuilder, ServiceExt,
};

pub struct Worker<S, Input, Output>
where
    S: Service<Input, Response = Output, Error = Box<dyn std::error::Error + Send + Sync>>
        + Send
        + Clone,
{
    srv: S,
    receiver: AsyncReceiver<Input>,
    _marker: PhantomData<fn(Input) -> Output>,
}

impl<S, Input, Output> Worker<S, Input, Output>
where
    Output: Send + 'static,
    Input: Send + 'static,
    S: Service<Input, Response = Output, Error = Box<dyn std::error::Error + Send + Sync>>
        + Send
        + Clone
        + 'static,
{
    pub fn new(srv: S, buffer_size: usize) -> (AsyncSender<Input>, Self) {
        let (sender, receiver) = kanal::bounded_async(buffer_size);
        (
            sender,
            Self {
                srv,
                _marker: PhantomData,
                receiver,
            },
        )
    }

    pub async fn start_job(self) -> anyhow::Result<()>
    where
        <S as Service<Input>>::Future: Send,
    {
        let mut workers = JoinSet::new();
        while let Ok(input) = self.receiver.recv().await {
            let mut srv = self.srv.clone();
            workers.spawn(async move {
                if srv.ready().await.is_ok() {
                    let _ = srv.call(input).await;
                }
            });
        }
        while let Some(res) = workers.join_next().await {
            if let Err(e) = res {
                std::panic::resume_unwind(e.into_panic());
            }
        }
        Ok(())
    }
}

pub struct WorkPipeBuilder<L> {
    builder: ServiceBuilder<L>,
    is_concurrent: bool,
}

impl WorkPipeBuilder<Identity> {
    pub fn new() -> Self {
        Self {
            builder: ServiceBuilder::new(),
            is_concurrent: false,
        }
    }
}

impl Default for WorkPipeBuilder<Identity> {
    fn default() -> Self {
        Self::new()
    }
}

impl<L> WorkPipeBuilder<L> {
    pub fn layer<NewLayer>(self, layer: NewLayer) -> WorkPipeBuilder<Stack<NewLayer, L>> {
        WorkPipeBuilder {
            builder: self.builder.layer(layer),
            is_concurrent: self.is_concurrent,
        }
    }

    pub fn with_buffer<T>(
        mut self,
        buffer_size: usize,
    ) -> WorkPipeBuilder<Stack<BufferLayer<T>, L>> {
        self.is_concurrent = true;
        self.layer(BufferLayer::new(buffer_size))
    }

    pub fn with_concurrency_limit(
        self,
        concurrency_limit: usize,
    ) -> WorkPipeBuilder<Stack<ConcurrencyLimitLayer, L>> {
        if !self.is_concurrent {
            // The buffer layer is required to make the service concurrent.
            panic!("`with_concurrency_limit` must be called after `with_buffer`");
        }
        self.layer(ConcurrencyLimitLayer::new(concurrency_limit))
    }

    pub fn service<S, Request>(self, service: S) -> L::Service
    where
        S: Service<Request>,
        L: Layer<S>,
    {
        self.builder.service(service)
    }
}

