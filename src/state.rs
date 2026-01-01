use std::{
    sync::Arc,
    task::{Context, Poll},
};
use tower::{Layer, Service};

#[derive(Debug, Clone)]
pub struct RequestState<T, S> {
    pub request: T,
    pub state: Arc<S>,
}

impl<T, S> RequestState<T, S> {
    pub fn new(request: T, state: Arc<S>) -> Self {
        Self { request, state }
    }
}

#[derive(Clone)]
pub struct StateLayer<S> {
    state: Arc<S>,
}

impl<S> StateLayer<S> {
    pub fn new(state: Arc<S>) -> Self {
        Self { state }
    }
}

impl<Inner, S> Layer<Inner> for StateLayer<S> {
    type Service = StateService<Inner, S>;

    fn layer(&self, inner: Inner) -> Self::Service {
        StateService {
            inner,
            state: self.state.clone(),
        }
    }
}

#[derive(Clone)]
pub struct StateService<Inner, S> {
    inner: Inner,
    state: Arc<S>,
}

impl<Inner, T, S> Service<T> for StateService<Inner, S>
where
    Inner: Service<RequestState<T, S>>,
{
    type Response = Inner::Response;
    type Error = Inner::Error;
    type Future = Inner::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: T) -> Self::Future {
        let wrapped = RequestState::new(req, self.state.clone());
        self.inner.call(wrapped)
    }
}
