//! AutoThrottle middleware for adaptive request pacing.
//!
//! This middleware adapts per-request delays based on observed response latency
//! and status codes. It targets a configurable concurrency level by setting
//! delay roughly to `latency / target_concurrency`, then smooths the transition
//! and applies penalties for throttling/error responses.

use async_trait::async_trait;
use log::debug;
use moka::future::Cache;
use rand::distributions::{Distribution, Uniform};
use spider_util::constants::{MIDDLEWARE_CACHE_CAPACITY, MIDDLEWARE_CACHE_TTL_SECS};
use spider_util::error::SpiderError;
use spider_util::request::Request;
use spider_util::response::Response;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;
use tokio::time::{Instant, sleep};

use crate::middleware::{Middleware, MiddlewareAction};

const STARTED_AT_META_KEY: &str = "__autothrottle_started_at_ms";

/// Scope used to isolate throttle state.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Scope {
    /// A single shared throttle state for all requests.
    Global,
    /// Independent throttle state per origin (`scheme://host:port`).
    Domain,
}

#[derive(Debug, Clone)]
struct ThrottleState {
    delay: Duration,
    next_allowed_at: Instant,
}

/// Middleware that adapts pacing dynamically based on response feedback.
pub struct AutoThrottleMiddleware {
    scope: Scope,
    states: Cache<String, std::sync::Arc<Mutex<ThrottleState>>>,
    min_delay: Duration,
    max_delay: Duration,
    target_concurrency: f64,
    smoothing_factor: f64,
    error_penalty: f64,
    forbidden_penalty: f64,
    too_many_penalty: f64,
    jitter: bool,
}

impl Default for AutoThrottleMiddleware {
    fn default() -> Self {
        Self::builder().build()
    }
}

impl AutoThrottleMiddleware {
    /// Creates a new builder for [`AutoThrottleMiddleware`].
    pub fn builder() -> AutoThrottleMiddlewareBuilder {
        AutoThrottleMiddlewareBuilder::default()
    }

    fn scope_key(&self, request: &Request) -> String {
        match self.scope {
            Scope::Global => "global".to_string(),
            Scope::Domain => spider_util::util::normalize_origin(request),
        }
    }

    fn apply_jitter(&self, delay: Duration) -> Duration {
        if !self.jitter || delay.is_zero() {
            return delay;
        }

        let jitter_window = delay.mul_f64(0.25).min(Duration::from_millis(500));
        let low = delay.saturating_sub(jitter_window);
        let high = delay + jitter_window;

        let mut rng = rand::thread_rng();
        let uniform = Uniform::new_inclusive(low, high);
        uniform.sample(&mut rng)
    }
}

#[async_trait]
impl<C: Send + Sync> Middleware<C> for AutoThrottleMiddleware {
    fn name(&self) -> &str {
        "AutoThrottleMiddleware"
    }

    async fn process_request(
        &self,
        _client: &C,
        mut request: Request,
    ) -> Result<MiddlewareAction<Request>, SpiderError> {
        let key = self.scope_key(&request);
        let state = self
            .states
            .get_with(key, async {
                std::sync::Arc::new(Mutex::new(ThrottleState {
                    delay: self.min_delay,
                    next_allowed_at: Instant::now(),
                }))
            })
            .await;

        let sleep_duration = {
            let mut state_guard = state.lock().await;
            let now = Instant::now();
            let delay = state_guard.delay;

            if now < state_guard.next_allowed_at {
                let wait = state_guard.next_allowed_at - now;
                state_guard.next_allowed_at += delay;
                wait
            } else {
                state_guard.next_allowed_at = now + delay;
                Duration::ZERO
            }
        };

        let sleep_duration = self.apply_jitter(sleep_duration);
        if !sleep_duration.is_zero() {
            sleep(sleep_duration).await;
        }

        if let Ok(since_epoch) = SystemTime::now().duration_since(UNIX_EPOCH) {
            request.insert_meta(
                STARTED_AT_META_KEY.to_string(),
                serde_json::Value::from(since_epoch.as_millis().min(u128::from(u64::MAX)) as u64),
            );
        }

        Ok(MiddlewareAction::Continue(request))
    }

    async fn process_response(
        &self,
        response: Response,
    ) -> Result<MiddlewareAction<Response>, SpiderError> {
        if response.cached {
            return Ok(MiddlewareAction::Continue(response));
        }

        let key = self.scope_key(&response.request_from_response());

        let Some(state) = self.states.get(&key).await else {
            return Ok(MiddlewareAction::Continue(response));
        };

        let observed_latency = response
            .meta
            .as_ref()
            .and_then(|meta| meta.get(STARTED_AT_META_KEY).map(|v| v.value().clone()))
            .and_then(|v| v.as_u64())
            .and_then(|started_at_ms| {
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .ok()
                    .map(|now| now.as_millis().saturating_sub(u128::from(started_at_ms)))
            })
            .map(|delta_ms| {
                let bounded = delta_ms.min(u128::from(u64::MAX)) as u64;
                Duration::from_millis(bounded)
            });

        let status = response.status.as_u16();
        let mut guard = state.lock().await;
        let old_delay = guard.delay;

        if let Some(latency) = observed_latency {
            let target_delay = latency
                .div_f64(self.target_concurrency.max(0.1))
                .clamp(self.min_delay, self.max_delay);
            let smoothed = old_delay.mul_f64(1.0 - self.smoothing_factor)
                + target_delay.mul_f64(self.smoothing_factor);
            guard.delay = smoothed.clamp(self.min_delay, self.max_delay);
        }

        match status {
            429 => guard.delay = guard.delay.mul_f64(self.too_many_penalty),
            403 => guard.delay = guard.delay.mul_f64(self.forbidden_penalty),
            500..=599 => guard.delay = guard.delay.mul_f64(self.error_penalty),
            _ => {}
        }
        guard.delay = guard.delay.clamp(self.min_delay, self.max_delay);

        if old_delay != guard.delay {
            debug!(
                "AutoThrottle adjusted delay for '{}': {:?} -> {:?} (status={})",
                key, old_delay, guard.delay, status
            );
        }

        Ok(MiddlewareAction::Continue(response))
    }
}

/// Builder for [`AutoThrottleMiddleware`].
pub struct AutoThrottleMiddlewareBuilder {
    scope: Scope,
    min_delay: Duration,
    max_delay: Duration,
    target_concurrency: f64,
    smoothing_factor: f64,
    error_penalty: f64,
    forbidden_penalty: f64,
    too_many_penalty: f64,
    cache_ttl: Duration,
    cache_capacity: u64,
    jitter: bool,
}

impl Default for AutoThrottleMiddlewareBuilder {
    fn default() -> Self {
        Self {
            scope: Scope::Domain,
            min_delay: Duration::from_millis(50),
            max_delay: Duration::from_secs(60),
            target_concurrency: 1.0,
            smoothing_factor: 0.3,
            error_penalty: 1.5,
            forbidden_penalty: 1.2,
            too_many_penalty: 2.0,
            cache_ttl: Duration::from_secs(MIDDLEWARE_CACHE_TTL_SECS),
            cache_capacity: MIDDLEWARE_CACHE_CAPACITY,
            jitter: true,
        }
    }
}

impl AutoThrottleMiddlewareBuilder {
    /// Sets throttling scope.
    pub fn scope(mut self, scope: Scope) -> Self {
        self.scope = scope;
        self
    }

    /// Sets minimum delay between requests.
    pub fn min_delay(mut self, min_delay: Duration) -> Self {
        self.min_delay = min_delay;
        self
    }

    /// Sets maximum delay between requests.
    pub fn max_delay(mut self, max_delay: Duration) -> Self {
        self.max_delay = max_delay;
        self
    }

    /// Sets target concurrency used in `latency / target_concurrency`.
    pub fn target_concurrency(mut self, target_concurrency: f64) -> Self {
        self.target_concurrency = target_concurrency;
        self
    }

    /// Sets smoothing factor (0.0..=1.0) for delay updates.
    pub fn smoothing_factor(mut self, smoothing_factor: f64) -> Self {
        self.smoothing_factor = smoothing_factor.clamp(0.0, 1.0);
        self
    }

    /// Sets multiplier for 5xx responses.
    pub fn error_penalty(mut self, error_penalty: f64) -> Self {
        self.error_penalty = error_penalty.max(1.0);
        self
    }

    /// Sets multiplier for 403 responses.
    pub fn forbidden_penalty(mut self, forbidden_penalty: f64) -> Self {
        self.forbidden_penalty = forbidden_penalty.max(1.0);
        self
    }

    /// Sets multiplier for 429 responses.
    pub fn too_many_penalty(mut self, too_many_penalty: f64) -> Self {
        self.too_many_penalty = too_many_penalty.max(1.0);
        self
    }

    /// Enables/disables sleep jitter.
    pub fn jitter(mut self, jitter: bool) -> Self {
        self.jitter = jitter;
        self
    }

    /// Sets middleware state cache TTL.
    pub fn cache_ttl(mut self, cache_ttl: Duration) -> Self {
        self.cache_ttl = cache_ttl;
        self
    }

    /// Sets middleware state cache capacity.
    pub fn cache_capacity(mut self, cache_capacity: u64) -> Self {
        self.cache_capacity = cache_capacity;
        self
    }

    /// Builds [`AutoThrottleMiddleware`].
    pub fn build(self) -> AutoThrottleMiddleware {
        let min_delay = self.min_delay.min(self.max_delay);
        let max_delay = self.max_delay.max(self.min_delay);

        AutoThrottleMiddleware {
            scope: self.scope,
            states: Cache::builder()
                .time_to_idle(self.cache_ttl)
                .max_capacity(self.cache_capacity)
                .build(),
            min_delay,
            max_delay,
            target_concurrency: self.target_concurrency.max(0.1),
            smoothing_factor: self.smoothing_factor.clamp(0.0, 1.0),
            error_penalty: self.error_penalty.max(1.0),
            forbidden_penalty: self.forbidden_penalty.max(1.0),
            too_many_penalty: self.too_many_penalty.max(1.0),
            jitter: self.jitter,
        }
    }
}
