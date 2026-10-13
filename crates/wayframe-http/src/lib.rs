//! Opt-in HTTP adapter. Applications own listeners, routes and deployment policy.
//! This crate never requires MCP, WebAssembly, a registry, or a management service.
#[cfg(feature = "lb")]
mod backend;
pub mod cache;
mod context;
pub mod credentials;
pub mod filters;
pub mod stream;
#[cfg(feature = "lb")]
pub use backend::{BackendId, Upstream};
#[cfg(feature = "lb")]
pub use context::PinnedBackend;
pub use context::{DeploymentGeneration, Identity, Operation, RequestContext, VerifiedIssuer};
pub use filters::{ResponseContext, ResponseFilter};
#[cfg(feature = "flow")]
pub mod workflow;

use anyhow::{ensure, Result};
use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use futures_util::StreamExt;
use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore},
    time::{timeout_at, Instant},
};
use tracing::Instrument;
use wayframe_core::{Decision, Reason};
use wayframe_runtime::Pipeline;

/// Stable HTTP mapping lives here, not in the core block contract.
pub const fn status_for(reason: Reason) -> u16 {
    match reason {
        Reason::InvalidInput => 400,
        Reason::MethodNotAllowed => 405,
        Reason::Unauthenticated => 401,
        Reason::Forbidden => 403,
        Reason::NotFound => 404,
        Reason::Conflict => 409,
        Reason::PayloadTooLarge => 413,
        Reason::RateLimited => 429,
        Reason::Unavailable => 503,
        Reason::DeadlineExceeded => 504,
        Reason::Internal => 500,
    }
}
/// Narrow v0.1 external/Wasm policy ABI. No success status can bypass later blocks.
pub fn policy_decision(status: u16, code: &'static str) -> Decision {
    let reason = match status {
        0 => return Decision::Continue,
        400 => Reason::InvalidInput,
        401 => Reason::Unauthenticated,
        403 => Reason::Forbidden,
        404 => Reason::NotFound,
        409 => Reason::Conflict,
        413 => Reason::PayloadTooLarge,
        429 => Reason::RateLimited,
        _ => return Decision::unavailable(),
    };
    Decision::deny(reason, code)
}
pub fn rejection(reason: Reason, code: &'static str) -> Response {
    let status = status_for(reason);
    let mut response = (
        StatusCode::from_u16(status).expect("fixed status"),
        Json(serde_json::json!({"error":code})),
    )
        .into_response();
    if status == 401 {
        response
            .headers_mut()
            .insert("www-authenticate", "Bearer".parse().expect("literal"));
    }
    if status == 429 || status == 503 {
        response
            .headers_mut()
            .insert("retry-after", "1".parse().expect("literal"));
    }
    response
        .headers_mut()
        .insert("cache-control", "no-store".parse().expect("literal"));
    response
}
/// Preserve frames/trailers and retain the complete deployment through EOF or disposal.
pub fn hold_response<T: Send + Unpin + 'static>(response: Response, resource: T) -> Response {
    struct Held<T> {
        body: Body,
        resource: Option<T>,
    }
    impl<T: Send + Unpin + 'static> http_body::Body for Held<T> {
        type Data = bytes::Bytes;
        type Error = axum::Error;
        fn poll_frame(
            self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
            let this = self.get_mut();
            let result = std::pin::Pin::new(&mut this.body).poll_frame(cx);
            if matches!(result, std::task::Poll::Ready(None)) {
                this.resource.take();
            }
            result
        }
        fn is_end_stream(&self) -> bool {
            self.body.is_end_stream()
        }
        fn size_hint(&self) -> http_body::SizeHint {
            self.body.size_hint()
        }
    }
    let (parts, body) = response.into_parts();
    Response::from_parts(
        parts,
        Body::new(Held {
            body,
            resource: Some(resource),
        }),
    )
}

#[derive(Clone, Debug)]
pub struct Limits {
    pub max_in_flight: usize,
    pub max_header_bytes: usize,
    pub max_request_bytes: u64,
    pub max_response_bytes: u64,
    /// Covers policy evaluation, upload, upstream work and response consumption.
    pub request_timeout: Duration,
    /// Per stream and per direction. Zero disables pacing. Not a fleet-wide quota.
    pub per_stream_bytes_per_second: u64,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            max_in_flight: 128,
            max_header_bytes: 16 * 1024,
            max_request_bytes: 1024 * 1024,
            max_response_bytes: 4 * 1024 * 1024,
            request_timeout: Duration::from_secs(15),
            per_stream_bytes_per_second: 0,
        }
    }
}
impl Limits {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            (1..=100_000).contains(&self.max_in_flight),
            "invalid admission capacity"
        );
        ensure!(
            (1024..=65536).contains(&self.max_header_bytes),
            "invalid metadata budget"
        );
        ensure!(
            self.max_request_bytes > 0 && self.max_response_bytes > 0,
            "body limits must be positive"
        );
        ensure!(
            !self.request_timeout.is_zero() && self.request_timeout <= Duration::from_secs(120),
            "invalid deadline"
        );
        Ok(())
    }
}
const LATENCY_BOUNDS: [u64; 10] = [
    1000,
    10000,
    100000,
    1000000,
    10000000,
    100000000,
    1000000000,
    10000000000,
    30000000000,
    120000000000,
];
#[derive(Default)]
pub struct Latency {
    buckets: [AtomicU64; 11],
    sum: AtomicU64,
}
impl Latency {
    fn observe(&self, elapsed: Duration) {
        let nanos = elapsed.as_nanos().min(u64::MAX as u128) as u64;
        let i = LATENCY_BOUNDS.partition_point(|bound| *bound < nanos);
        self.buckets[i].fetch_add(1, Ordering::Relaxed);
        let _ = self
            .sum
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                Some(n.saturating_add(nanos))
            });
    }
    pub fn snapshot(&self) -> serde_json::Value {
        serde_json::json!({"bounds_ns":LATENCY_BOUNDS,"bucket_counts":self.buckets.iter().map(|b|b.load(Ordering::Relaxed)).collect::<Vec<_>>(),"sum_ns":self.sum.load(Ordering::Relaxed)})
    }
    fn measure(&self) -> Measure<'_> {
        Measure {
            histogram: self,
            at: Instant::now(),
        }
    }
}
struct Measure<'a> {
    histogram: &'a Latency,
    at: Instant,
}
impl Drop for Measure<'_> {
    fn drop(&mut self) {
        self.histogram.observe(self.at.elapsed());
    }
}
#[derive(Default)]
pub struct Metrics {
    pub requests: AtomicU64,
    pub active: AtomicU64,
    pub denied: AtomicU64,
    pub overload: AtomicU64,
    pub upstream_errors: AtomicU64,
    pub bytes_in: AtomicU64,
    pub bytes_out: AtomicU64,
    pub total: Latency,
    pub preflight: Latency,
    pub policy: Latency,
    pub body_inspection: Latency,
    pub upstream_headers: Latency,
}
impl Metrics {
    pub fn snapshot(&self) -> serde_json::Value {
        let read = |n: &AtomicU64| n.load(Ordering::Relaxed);
        serde_json::json!({"requests":read(&self.requests), "active":read(&self.active),
            "denied":read(&self.denied), "overload":read(&self.overload),
            "upstream_errors":read(&self.upstream_errors), "bytes_in":read(&self.bytes_in),
            "bytes_out":read(&self.bytes_out),"latency":{"total":self.total.snapshot(),"preflight":self.preflight.snapshot(),"policy":self.policy.snapshot(),"body_inspection":self.body_inspection.snapshot(),"upstream_headers":self.upstream_headers.snapshot()}})
    }
}
struct Active {
    metrics: Arc<Metrics>,
    _permit: OwnedSemaphorePermit,
    started: Instant,
    transport_deadline: Option<tokio::task::AbortHandle>,
}
impl Active {
    fn new(metrics: Arc<Metrics>, permit: OwnedSemaphorePermit) -> Self {
        metrics.active.fetch_add(1, Ordering::Relaxed);
        Self {
            metrics,
            _permit: permit,
            started: Instant::now(),
            transport_deadline: None,
        }
    }
}
impl Drop for Active {
    fn drop(&mut self) {
        if let Some(timer) = &self.transport_deadline {
            timer.abort();
        }
        self.metrics.active.fetch_sub(1, Ordering::Relaxed);
        self.metrics.total.observe(self.started.elapsed());
    }
}

pub struct GatewayBuilder {
    generation: u64,
    filters: Vec<Arc<dyn ResponseFilter>>,
    cache: Option<Arc<dyn cache::ResponseCache>>,
    origin: String,
    preflight: Option<Pipeline<RequestContext>>,
    policy: Option<Pipeline<RequestContext>>,
    limits: Limits,
    inspection_bytes: Option<usize>,
    authorities: Vec<String>,
    upstream_token: Option<String>,
    http2_prior_knowledge: bool,
    credential_provider: Option<Arc<dyn credentials::Provider>>,
    upstream_ca: Option<Vec<u8>>,
    #[cfg(feature = "lb")]
    upstreams: Vec<Upstream>,
    #[cfg(feature = "lb")]
    failure_cooldown: Duration,
}
impl GatewayBuilder {
    pub fn new(origin: impl Into<String>) -> Self {
        Self {
            generation: 1,
            filters: Vec::new(),
            cache: None,
            origin: origin.into(),
            preflight: None,
            policy: None,
            limits: Limits::default(),
            inspection_bytes: None,
            authorities: Vec::new(),
            upstream_token: None,
            http2_prior_knowledge: false,
            credential_provider: None,
            upstream_ca: None,
            #[cfg(feature = "lb")]
            upstreams: Vec::new(),
            #[cfg(feature = "lb")]
            failure_cooldown: Duration::from_secs(1),
        }
    }
    /// Authentication and cheap admission run before collecting a body.
    pub fn preflight(mut self, pipeline: Pipeline<RequestContext>) -> Self {
        self.preflight = Some(pipeline);
        self
    }
    pub fn generation(mut self, generation: u64) -> Self {
        self.generation = generation;
        self
    }
    pub fn response_cache(mut self, cache: Arc<dyn cache::ResponseCache>) -> Self {
        self.cache = Some(cache);
        self
    }
    pub fn response_filters(mut self, filters: Vec<Arc<dyn ResponseFilter>>) -> Self {
        self.filters = filters;
        self
    }
    /// Protocol-specific inspection and authorization run after optional body collection.
    pub fn policy(mut self, pipeline: Pipeline<RequestContext>) -> Self {
        self.policy = Some(pipeline);
        self
    }
    pub fn limits(mut self, limits: Limits) -> Self {
        self.limits = limits;
        self
    }
    /// Explicit bounded buffering, e.g. for a small JSON-RPC envelope. Default is streaming.
    pub fn inspect_body(mut self, maximum: usize) -> Self {
        self.inspection_bytes = Some(maximum);
        self
    }
    /// Optional exact inbound Host allowlist; useful for loopback development endpoints.
    pub fn authorities(mut self, values: Vec<String>) -> Self {
        self.authorities = values;
        self
    }
    /// A distinct gateway-to-upstream credential; client credentials are NEVER forwarded.
    pub fn upstream_http2(mut self, enabled: bool) -> Self {
        self.http2_prior_knowledge = enabled;
        self
    }
    pub fn upstream_credentials(mut self, provider: Arc<dyn credentials::Provider>) -> Self {
        self.credential_provider = Some(provider);
        self
    }
    pub fn upstream_bearer(mut self, token: String) -> Self {
        self.upstream_token = Some(token);
        self
    }
    /// Trust an operator-provided CA in addition to the public roots; certificate checks remain enabled.
    pub fn upstream_ca(mut self, pem: Vec<u8>) -> Self {
        self.upstream_ca = Some(pem);
        self
    }
    /// Register full, fixed endpoints selected by an LB block. A selection is mandatory when nonempty.
    #[cfg(feature = "lb")]
    pub fn upstreams(mut self, upstreams: Vec<Upstream>) -> Self {
        self.upstreams = upstreams;
        self
    }
    #[cfg(feature = "lb")]
    pub fn failure_cooldown(mut self, duration: Duration) -> Self {
        self.failure_cooldown = duration;
        self
    }
    pub fn build(self) -> Result<Arc<HttpGateway>> {
        self.limits.validate()?;
        ensure!(self.generation > 0, "invalid deployment generation");
        ensure!(self.filters.len() <= 64, "too many response filters");
        let mut filter_ids = std::collections::HashSet::new();
        for filter in &self.filters {
            let id = filter.id();
            ensure!(
                !id.is_empty()
                    && id.len() <= 64
                    && id
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
                    && filter_ids.insert(id),
                "invalid response filter id"
            );
        }
        let origin: reqwest::Url = self.origin.parse()?;
        ensure!(
            matches!(origin.scheme(), "http" | "https")
                && origin.host_str().is_some()
                && origin.username().is_empty()
                && origin.password().is_none()
                && origin.path() == "/"
                && origin.query().is_none()
                && origin.fragment().is_none(),
            "upstream must be a fixed HTTP(S) origin with no path, query or credentials"
        );
        if let Some(n) = self.inspection_bytes {
            ensure!(
                n > 0 && n <= 1024 * 1024 && n as u64 <= self.limits.max_request_bytes,
                "invalid inspection limit"
            );
        }
        if let Some(token) = &self.upstream_token {
            ensure!(
                token.len() >= 32 && token.len() <= 8192 && !token.contains(char::is_whitespace),
                "invalid upstream token"
            );
        }
        let mut client = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(3))
            .pool_max_idle_per_host(32);
        if self.http2_prior_knowledge {
            client = client.http2_prior_knowledge();
        }
        if let Some(pem) = self.upstream_ca {
            ensure!(pem.len() <= 64 * 1024, "CA file too large");
            client = client.add_root_certificate(reqwest::Certificate::from_pem(&pem)?);
        }
        let client = client.build()?;
        #[cfg(feature = "lb")]
        let upstreams = {
            ensure!(self.upstreams.len() <= 128, "too many backends");
            ensure!(
                self.failure_cooldown <= Duration::from_secs(600),
                "invalid backend cooldown"
            );
            let mut prepared = std::collections::HashMap::new();
            for upstream in self.upstreams {
                let (id, value) = upstream.prepare()?;
                ensure!(prepared.insert(id, value).is_none(), "duplicate backend id");
            }
            prepared
        };
        Ok(Arc::new(HttpGateway {
            generation: self.generation,
            filters: self.filters,
            cache: self.cache,
            origin: origin.as_str().trim_end_matches('/').into(),
            preflight: self
                .preflight
                .ok_or_else(|| anyhow::anyhow!("supply an explicit preflight pipeline"))?,
            policy: self.policy.unwrap_or(Pipeline::compile(vec![])?),
            admission: Arc::new(Semaphore::new(self.limits.max_in_flight)),
            limits: self.limits,
            inspection_bytes: self.inspection_bytes,
            authorities: self.authorities,
            upstream_token: self.upstream_token,
            credential_provider: self.credential_provider,
            client,
            metrics: Arc::new(Metrics::default()),
            sequence: AtomicU64::new(1),
            #[cfg(feature = "lb")]
            upstreams,
            #[cfg(feature = "lb")]
            failure_cooldown: self.failure_cooldown,
        }))
    }
}

pub struct HttpGateway {
    generation: u64,
    filters: Vec<Arc<dyn ResponseFilter>>,
    cache: Option<Arc<dyn cache::ResponseCache>>,
    origin: String,
    preflight: Pipeline<RequestContext>,
    policy: Pipeline<RequestContext>,
    limits: Limits,
    inspection_bytes: Option<usize>,
    authorities: Vec<String>,
    upstream_token: Option<String>,
    credential_provider: Option<Arc<dyn credentials::Provider>>,
    client: reqwest::Client,
    admission: Arc<Semaphore>,
    metrics: Arc<Metrics>,
    sequence: AtomicU64,
    #[cfg(feature = "lb")]
    upstreams: std::collections::HashMap<String, backend::Prepared>,
    #[cfg(feature = "lb")]
    failure_cooldown: Duration,
}
impl HttpGateway {
    pub fn builder(origin: impl Into<String>) -> GatewayBuilder {
        GatewayBuilder::new(origin)
    }
    pub fn cors_preflight(&self, origin: &str) -> Option<http::HeaderMap> {
        let mut headers = http::HeaderMap::new();
        let supported = self
            .filters
            .iter()
            .any(|f| f.preflight(origin, &mut headers));
        supported.then_some(headers)
    }
    pub fn cache_stats(&self) -> Option<serde_json::Value> {
        self.cache.as_ref().map(|c| c.stats())
    }
    pub fn metrics(&self) -> &Arc<Metrics> {
        &self.metrics
    }
    pub fn stop_admission(&self) {
        self.admission.close();
    }
    fn deny(&self, decision: Decision) -> Option<Response> {
        match decision {
            Decision::Continue => None,
            Decision::Deny { reason, code } => {
                self.metrics.denied.fetch_add(1, Ordering::Relaxed);
                Some(rejection(reason, code))
            }
        }
    }
    pub async fn handle(self: &Arc<Self>, peer: SocketAddr, request: Request<Body>) -> Response {
        use tracing::Instrument;
        let span = tracing::info_span!("gateway.request", deployment.generation=self.generation, deadline.exceeded=tracing::field::Empty, otel.kind = "server", http.request.method = %request.method(), http.response.status_code = tracing::field::Empty, request.id = tracing::field::Empty, mcp.method = tracing::field::Empty, mcp.tool = tracing::field::Empty, stream.outcome = tracing::field::Empty, otel.status_code = tracing::field::Empty, lb.backend.id = tracing::field::Empty, lb.strategy = tracing::field::Empty, lb.overflow = tracing::field::Empty);
        #[cfg(feature = "otel")]
        {
            use opentelemetry::propagation::TextMapPropagator;
            use tracing_opentelemetry::OpenTelemetrySpanExt;
            struct Headers<'a>(&'a http::HeaderMap);
            impl opentelemetry::propagation::Extractor for Headers<'_> {
                fn get(&self, key: &str) -> Option<&str> {
                    self.0.get(key).and_then(|v| v.to_str().ok())
                }
                fn keys(&self) -> Vec<&str> {
                    self.0.keys().map(|k| k.as_str()).collect()
                }
            }
            let parent = opentelemetry_sdk::propagation::TraceContextPropagator::new()
                .extract(&Headers(request.headers()));
            let _ = span.set_parent(parent);
        }
        let response = self
            .handle_inner(peer, request)
            .instrument(span.clone())
            .await;
        #[cfg(feature = "otel")]
        let mut response = response;
        span.record("http.response.status_code", response.status().as_u16());
        if response.status().is_server_error() {
            span.record("otel.status_code", "ERROR");
        }
        #[cfg(feature = "otel")]
        {
            use opentelemetry::trace::TraceContextExt;
            use tracing_opentelemetry::OpenTelemetrySpanExt;
            let context = span.context();
            if context.span().span_context().is_valid() {
                response.headers_mut().insert(
                    "x-wayframe-trace-id",
                    context
                        .span()
                        .span_context()
                        .trace_id()
                        .to_string()
                        .parse()
                        .expect("trace id"),
                );
            }
        }
        response
    }
    async fn handle_inner(self: &Arc<Self>, peer: SocketAddr, request: Request<Body>) -> Response {
        self.metrics.requests.fetch_add(1, Ordering::Relaxed);
        let mut request = request;
        if wayframe_codec::metadata::normalize_h2(&mut request).is_err() {
            return rejection(Reason::InvalidInput, "invalid_authority");
        }
        let admission_span = tracing::info_span!("gateway.admission");
        let admission = admission_span.in_scope(|| self.admission.clone().try_acquire_owned());
        drop(admission_span);
        let Ok(permit) = admission else {
            self.metrics.overload.fetch_add(1, Ordering::Relaxed);
            return rejection(Reason::Unavailable, "overloaded");
        };
        let mut active = Active::new(self.metrics.clone(), permit);
        let limits = &self.limits;
        let deadline = Instant::now() + limits.request_timeout;
        if let Some(cancel) = request
            .extensions()
            .get::<wayframe_core::TransportCancellation>()
            .cloned()
        {
            let span = tracing::Span::current();
            active.transport_deadline = Some(
                tokio::spawn(async move {
                    tokio::time::sleep_until(deadline).await;
                    span.record("deadline.exceeded", true);
                    (cancel.0)();
                })
                .abort_handle(),
            );
        }
        if request.method() == http::Method::CONNECT
            || request.headers().contains_key(http::header::UPGRADE)
        {
            return (
                StatusCode::NOT_IMPLEMENTED,
                "upgrades and CONNECT are not supported",
            )
                .into_response();
        }
        if request.uri().scheme().is_some() || request.uri().authority().is_some() {
            return rejection(Reason::InvalidInput, "origin_form_required");
        }
        let header_bytes: usize = request
            .headers()
            .iter()
            .map(|(n, v)| n.as_str().len() + v.len())
            .sum();
        if header_bytes > limits.max_header_bytes || request.uri().to_string().len() > 8192 {
            return (
                StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE,
                "metadata budget exceeded",
            )
                .into_response();
        }
        if !self.authorities.is_empty() {
            let mut hosts = request.headers().get_all("host").iter();
            let good = hosts
                .next()
                .and_then(|v| v.to_str().ok())
                .map(|h| {
                    self.authorities
                        .iter()
                        .any(|allowed| allowed.eq_ignore_ascii_case(h))
                })
                .unwrap_or(false);
            if !good || hosts.next().is_some() {
                return rejection(Reason::Forbidden, "authority_not_allowed");
            }
        }
        let Some(path) = stream::canonical_path(request.uri().path()) else {
            return rejection(Reason::InvalidInput, "ambiguous_path");
        };
        if let Some(length) = request.headers().get(http::header::CONTENT_LENGTH) {
            match length.to_str().ok().and_then(|s| s.parse::<u64>().ok()) {
                Some(n) if n > limits.max_request_bytes => {
                    return rejection(Reason::PayloadTooLarge, "request_body_too_large")
                }
                None => return rejection(Reason::InvalidInput, "invalid_content_length"),
                _ => {}
            }
        }
        let mut ctx = RequestContext::new(request.method().clone(), path, peer.ip(), deadline);
        ctx.extensions = request.extensions().clone();
        ctx.request_id = self.sequence.fetch_add(1, Ordering::Relaxed);
        ctx.extensions.insert(DeploymentGeneration(self.generation));
        tracing::Span::current().record("request.id", ctx.request_id);
        ctx.headers = request.headers().clone();
        let preflight_timer = self.metrics.preflight.measure();
        let decision = self.preflight.evaluate(&mut ctx).await;
        drop(preflight_timer);
        if let Some(response) = self.deny(decision) {
            return response;
        }
        let (mut parts, original_body) = request.into_parts();
        let body = if let Some(maximum) = self.inspection_bytes {
            if parts.headers.contains_key(http::header::CONTENT_ENCODING) {
                return (
                    StatusCode::UNSUPPORTED_MEDIA_TYPE,
                    "compressed inspection bodies are not supported",
                )
                    .into_response();
            }
            let _inspection_timer = self.metrics.body_inspection.measure();
            let bytes = match timeout_at(deadline, to_bytes(original_body, maximum))
                .instrument(tracing::info_span!("gateway.body.inspect"))
                .await
            {
                Ok(Ok(bytes)) => bytes,
                Ok(Err(_)) => return rejection(Reason::PayloadTooLarge, "inspection_body_limit"),
                Err(_) => return rejection(Reason::DeadlineExceeded, "body_deadline"),
            };
            ctx.body = Some(bytes.clone());
            // Normalize framing after bounded inspection, even for a chunked incoming request.
            parts.headers.remove(http::header::TRANSFER_ENCODING);
            parts.headers.insert(
                http::header::CONTENT_LENGTH,
                http::HeaderValue::from_str(&bytes.len().to_string()).expect("decimal length"),
            );
            Body::from(bytes)
        } else {
            original_body
        };
        let inspected_length = ctx.body.as_ref().map(bytes::Bytes::len);
        let policy_timer = self.metrics.policy.measure();
        let policy_decision = self.policy.evaluate(&mut ctx).await;
        drop(policy_timer);
        if let Some(op) = &ctx.operation {
            tracing::Span::current().record("mcp.method", op.action.as_str());
            if let Some(tool) = &op.target {
                tracing::Span::current().record("mcp.tool", tool.as_str());
            }
        }
        if let Some(response) = self.deny(policy_decision) {
            return response;
        }
        #[cfg(feature = "lb")]
        let selection = ctx.extensions.get::<wayframe_lb::Selection>().cloned();
        #[cfg(feature = "lb")]
        let registered = if self.upstreams.is_empty() {
            if selection.is_some() {
                return rejection(Reason::Unavailable, "backend_not_registered");
            }
            None
        } else {
            let Some(selected) = &selection else {
                return rejection(Reason::Unavailable, "backend_selection_required");
            };
            let Some(registered) = self.upstreams.get(selected.backend_id()) else {
                return rejection(Reason::Unavailable, "backend_not_registered");
            };
            Some(registered)
        };
        #[cfg(feature = "lb")]
        if let Some(selected) = &selection {
            let span = tracing::Span::current();
            span.record("lb.backend.id", selected.backend_id());
            span.record("lb.strategy", selected.strategy().as_str());
            span.record("lb.overflow", selected.overflow());
        }
        let (cache_ticket, cached_response) = if let Some(cache) = &self.cache {
            match cache.begin(&ctx).await {
                Ok(cache::Access::Bypass) => (None, None),
                Ok(cache::Access::Miss(ticket)) => (Some(ticket), None),
                Ok(cache::Access::Hit(bytes)) => {
                    let response = http::Response::builder()
                        .status(200)
                        .header("content-type", "application/json")
                        .header("content-length", bytes.len())
                        .header("x-wayframe-cache", "hit")
                        .body(bytes)
                        .expect("cache response");
                    (None, Some(reqwest::Response::from(response)))
                }
                Err(decision) => {
                    return self
                        .deny(decision)
                        .unwrap_or_else(|| rejection(Reason::Unavailable, "cache_failed"))
                }
            }
        } else {
            (None, None)
        };
        #[cfg(feature = "lb")]
        let from_cache = cached_response.is_some();
        // Policies inspect canonical paths; forwarding uses the same canonical path.
        let mut target: reqwest::Url = self.origin.parse().expect("validated origin");
        target.set_path(&ctx.path);
        #[cfg(feature = "lb")]
        if let Some(backend) = registered {
            target = backend.endpoint.clone();
        }
        target.set_query(parts.uri.query());
        stream::strip_hop_headers(&mut parts.headers);
        stream::strip_client_identity(&mut parts.headers);
        // Never forward arbitrary client baggage or an unvalidated trace context.
        for name in ["traceparent", "tracestate", "baggage"] {
            parts.headers.remove(name);
        }
        let upstream_span = tracing::info_span!(
            "gateway.upstream",
            otel.kind = "client",
            http.response.status_code = tracing::field::Empty,
            otel.status_code = tracing::field::Empty
        );
        #[cfg(feature = "otel")]
        {
            use opentelemetry::propagation::TextMapPropagator;
            use tracing_opentelemetry::OpenTelemetrySpanExt;
            struct Headers<'a>(&'a mut http::HeaderMap);
            impl opentelemetry::propagation::Injector for Headers<'_> {
                fn set(&mut self, key: &str, value: String) {
                    if let (Ok(key), Ok(value)) = (
                        http::header::HeaderName::from_bytes(key.as_bytes()),
                        http::HeaderValue::from_str(&value),
                    ) {
                        self.0.insert(key, value);
                    }
                }
            }
            opentelemetry_sdk::propagation::TraceContextPropagator::new()
                .inject_context(&upstream_span.context(), &mut Headers(&mut parts.headers));
        }
        if let Some(length) = inspected_length {
            parts.headers.insert(
                http::header::CONTENT_LENGTH,
                http::HeaderValue::from_str(&length.to_string()).expect("decimal length"),
            );
        }
        if !self.filters.is_empty() {
            // Record hooks consume bounded decoded JSON/SSE, with no implicit decompression budget.
            parts.headers.insert(
                http::header::ACCEPT_ENCODING,
                http::HeaderValue::from_static("identity"),
            );
        }
        // An inspected envelope has a bounded, known body, but streaming remains uniform.
        let input = stream::FrameBody::new(stream::constrained_frames(
            body,
            limits.max_request_bytes,
            limits.per_stream_bytes_per_second,
            deadline,
            Some(self.metrics.clone()),
        ));
        let client = &self.client;
        let bearer = &self.upstream_token;
        #[cfg(feature = "lb")]
        let (client, bearer) = registered.map_or((client, bearer), |b| (&b.client, &b.bearer));
        let provider = &self.credential_provider;
        #[cfg(feature = "lb")]
        let provider = registered.map_or(provider, |b| &b.provider);
        let service_token = if let Some(provider) =
            provider.as_ref().filter(|_| cached_response.is_none())
        {
            match provider
                .bearer(deadline)
                .instrument(tracing::info_span!(
                    "gateway.credentials",
                    otel.kind = "client"
                ))
                .await
            {
                Ok(token) => Some(token),
                Err(_) => return rejection(Reason::Unavailable, "service_credentials_unavailable"),
            }
        } else {
            None
        };
        let mut upstream = client
            .request(parts.method, target)
            .headers(parts.headers)
            .body(reqwest::Body::wrap(input));
        if let Some(token) = service_token.as_ref().or(bearer.as_ref()) {
            upstream = upstream.bearer_auth(token);
        }
        let upstream_timer = self.metrics.upstream_headers.measure();
        let result = if let Some(cached) = cached_response {
            cached
        } else {
            match timeout_at(deadline, upstream.send())
                .instrument(upstream_span.clone())
                .await
            {
                Ok(Ok(r)) => r,
                Ok(Err(_error)) => {
                    upstream_span.record("otel.status_code", "ERROR");
                    #[cfg(feature = "lb")]
                    if !_error.is_body() {
                        if let Some(selection) = &selection {
                            selection.failed(self.failure_cooldown);
                        }
                    }
                    self.metrics.upstream_errors.fetch_add(1, Ordering::Relaxed);
                    return (StatusCode::BAD_GATEWAY, "upstream failed").into_response();
                }
                Err(_) => {
                    upstream_span.record("otel.status_code", "ERROR");
                    return rejection(Reason::DeadlineExceeded, "upstream_deadline");
                }
            }
        };
        drop(upstream_timer);
        let status = result.status();
        upstream_span.record("http.response.status_code", status.as_u16());
        if status.is_client_error() || status.is_server_error() {
            upstream_span.record("otel.status_code", "ERROR");
        }
        #[cfg(feature = "lb")]
        if let Some(selected) = &selection {
            if matches!(status.as_u16(), 502..=504) {
                selected.failed(self.failure_cooldown);
            } else if !from_cache {
                selected.succeeded();
            }
        }
        drop(upstream_span);
        let mut headers = result.headers().clone();
        for name in ["authorization", "proxy-authorization", "set-cookie"] {
            headers.remove(name);
        }
        stream::strip_hop_headers(&mut headers);
        let response_context = ResponseContext {
            identity: ctx.identity.clone(),
            operation: ctx.operation.clone(),
            status,
            origin: ctx
                .headers
                .get("origin")
                .and_then(|h| h.to_str().ok())
                .map(str::to_owned),
            protocol_error: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            terminal_seen: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            records_seen: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            request: ctx
                .body
                .as_ref()
                .and_then(|b| wayframe_codec::parse_unique(b).ok()),
            generation: self.generation,
            #[cfg(feature = "lb")]
            backend: selection.as_ref().map(|s| s.backend_id().to_owned()),
            #[cfg(not(feature = "lb"))]
            backend: None,
        };
        for filter in &self.filters {
            if let Some(response) = self.deny(filter.headers(&response_context, &mut headers)) {
                return response;
            }
        }
        stream::strip_hop_headers(&mut headers);
        let filters: Vec<_> = self
            .filters
            .iter()
            .filter(|f| f.records(&response_context))
            .cloned()
            .collect();
        let sse = headers
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .is_some_and(|s| s.starts_with("text/event-stream"));
        if result
            .content_length()
            .is_some_and(|n| n > limits.max_response_bytes)
        {
            return (StatusCode::BAD_GATEWAY, "upstream response too large").into_response();
        }
        let expected_bytes = if filters.is_empty() {
            result.content_length()
        } else {
            if result
                .headers()
                .get("content-encoding")
                .is_some_and(|v| v != "identity")
            {
                return (
                    StatusCode::BAD_GATEWAY,
                    "encoded response cannot be filtered",
                )
                    .into_response();
            }
            headers.remove("content-length");
            filters::strip_representation_metadata(&mut headers);
            None
        };
        type Output = std::pin::Pin<
            Box<
                dyn futures_util::Stream<
                        Item = Result<http_body::Frame<bytes::Bytes>, stream::StreamError>,
                    > + Send,
            >,
        >;
        let response_body = reqwest::Body::from(result);
        let output: Output = if filters.is_empty() {
            Box::pin(stream::constrained_frames(
                response_body,
                limits.max_response_bytes,
                limits.per_stream_bytes_per_second,
                deadline,
                None,
            ))
        } else {
            let trailers = Arc::new(std::sync::Mutex::new(None));
            let captured = trailers.clone();
            let output = stream::constrained_frames(
                response_body,
                limits.max_response_bytes,
                limits.per_stream_bytes_per_second,
                deadline,
                None,
            )
            .filter_map(move |r| {
                futures_util::future::ready(match r {
                    Ok(f) => match f.into_data() {
                        Ok(data) => Some(Ok(data)),
                        Err(frame) => {
                            if let Ok(mut headers) = frame.into_trailers() {
                                filters::strip_representation_metadata(&mut headers);
                                if let Ok(mut stored) = captured.lock() {
                                    *stored = Some(headers);
                                }
                            }
                            None
                        }
                    },
                    Err(e) => Some(Err(e)),
                })
            });
            let transformed = filters::transform(
                output,
                sse,
                limits.max_response_bytes as usize,
                filters,
                response_context.clone(),
            )
            .map(|r| r.map(http_body::Frame::data));
            let trailer = futures_util::stream::once(async move {
                trailers
                    .lock()
                    .ok()
                    .and_then(|mut stored| stored.take())
                    .map(|headers| Ok(http_body::Frame::trailers(headers)))
            })
            .filter_map(futures_util::future::ready);
            Box::pin(transformed.chain(trailer))
        };
        let generation = self.clone();
        let resources = ctx.extensions;
        let output_metrics = self.metrics.clone();
        let response_cache = self.cache.clone();
        let cache_headers = headers.clone();
        let cache_max = 64 * 1024;
        let request_span = tracing::Span::current();
        let stream_span = tracing::info_span!(
            "gateway.response.stream",
            stream.outcome = tracing::field::Empty
        );
        struct Completion {
            filters: Vec<Arc<dyn ResponseFilter>>,
            context: ResponseContext,
            outcome: &'static str,
            stream: tracing::Span,
            request: tracing::Span,
            done: bool,
            expected: Option<u64>,
            consumed: u64,
        }
        impl Drop for Completion {
            fn drop(&mut self) {
                if self.context.protocol_error.load(Ordering::Relaxed) {
                    self.outcome = "protocol_error";
                    self.request.record("otel.status_code", "ERROR");
                    self.request.record("stream.outcome", "protocol_error");
                    self.stream.record("stream.outcome", "protocol_error");
                }
                for filter in &self.filters {
                    filter.complete(&self.context, self.outcome);
                }
                // The OTEL layer timestamps the last exit, so close both scopes at body disposal.
                self.request.in_scope(|| {
                    self.stream.in_scope(|| {
                        if !self.done {
                            let outcome = if self.expected == Some(self.consumed) {
                                "complete"
                            } else {
                                "cancelled"
                            };
                            self.request.record("stream.outcome", outcome);
                            self.stream.record("stream.outcome", outcome);
                        }
                    })
                });
            }
        }
        // Captured outside the generator so an unpolled/dropped body also closes its trace.
        let completion = Completion {
            filters: self.filters.clone(),
            context: response_context,
            outcome: "cancelled",
            request: request_span,
            stream: stream_span,
            done: false,
            expected: expected_bytes,
            consumed: 0,
        };
        #[cfg(feature = "lb")]
        let backend_id = selection.as_ref().map(|s| BackendId(s.backend_id().into()));
        #[cfg(feature = "lb")]
        let cooldown = self.failure_cooldown;
        let stream = async_stream::stream! {
            let mut completion = completion;
            let mut cache_bytes = cache_ticket.as_ref().map(|_| Vec::new());
            #[cfg(feature = "lb")]
            let selection = selection; // retain one reservation through body disposal/EOF, including an unpolled body
            let _active = active; // retain the admission permit until EOF or disconnect
            let _generation = generation; // retain old policy/pool resources through streaming
            let _resources = resources; // retain application resource leases through streaming
            tokio::pin!(output);
            while let Some(chunk) = output.next().instrument(completion.stream.clone()).await {
                #[cfg(feature = "lb")]
                if chunk.as_ref().is_err_and(|error| error.is_source_failure()) {
                    if let Some(selected) = &selection {
                        selected.failed(cooldown);
                    }
                }
                if chunk.is_err() {
                    completion.outcome = "error";
                    completion.request.record("otel.status_code", "ERROR");
                    completion.request.record("stream.outcome", "error");
                    completion.stream.record("stream.outcome", "error");
                    completion.done = true;
                }
                if let Ok(frame) = &chunk {
                    if frame
                        .trailers_ref()
                        .is_some_and(|t| t.get("grpc-status").is_some_and(|s| s != "0"))
                    {
                        completion
                            .context
                            .protocol_error
                            .store(true, Ordering::Relaxed);
                    }
                    if let Some(bytes) = frame.data_ref() {
                        completion.consumed += bytes.len() as u64;
                        if let Some(buffer) = &mut cache_bytes {
                            if buffer.len().saturating_add(bytes.len()) <= cache_max {
                                buffer.extend_from_slice(bytes);
                            } else {
                                cache_bytes = None;
                            }
                        }
                        output_metrics
                            .bytes_out
                            .fetch_add(bytes.len() as u64, Ordering::Relaxed);
                    }
                }
                yield chunk;
            }
            if !completion.done {
                if let (Some(cache), Some(ticket), Some(bytes)) =
                    (&response_cache, &cache_ticket, &cache_bytes)
                {
                    cache.store(ticket, status, &cache_headers, bytes);
                }
                completion.outcome = "complete";
                completion.request.record("stream.outcome", "complete");
                completion.stream.record("stream.outcome", "complete");
                completion.done = true;
            }
        };
        let mut response = Response::new(Body::new(stream::FrameBody::new(stream)));
        *response.status_mut() = status;
        *response.headers_mut() = headers;
        #[cfg(feature = "lb")]
        if let Some(backend_id) = backend_id {
            response.extensions_mut().insert(backend_id);
        }
        if let Some(identity) = ctx.identity {
            response.extensions_mut().insert(identity);
        }
        if let Some(operation) = ctx.operation {
            response.extensions_mut().insert(operation);
        }
        response
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rejects_unsafe_upstream_configuration() {
        for target in [
            "file:///tmp/x",
            "http://u:p@localhost/",
            "http://localhost/path",
            "http://localhost/?x=1",
        ] {
            assert!(HttpGateway::builder(target)
                .preflight(Pipeline::compile(vec![]).unwrap())
                .build()
                .is_err());
        }
    }
    #[test]
    fn external_decisions_cannot_claim_final_success() {
        assert_eq!(policy_decision(0, "p"), Decision::Continue);
        assert_eq!(policy_decision(200, "p"), Decision::unavailable());
        assert_eq!(policy_decision(503, "p"), Decision::unavailable());
        assert!(matches!(
            policy_decision(403, "p"),
            Decision::Deny {
                reason: Reason::Forbidden,
                ..
            }
        ));
    }
    #[tokio::test]
    async fn preflight_denial_never_contacts_upstream() {
        use wayframe_core::{Block, Execution, Phase};
        struct Deny;
        impl Block<RequestContext> for Deny {
            fn id(&self) -> &str {
                "deny"
            }
            fn phase(&self) -> Phase {
                Phase::Authenticate
            }
            fn execute<'a>(&'a self, _: &'a mut RequestContext) -> Execution<'a> {
                Execution::Immediate(Decision::deny(Reason::Unauthenticated, "unauthorized"))
            }
        }
        let g = HttpGateway::builder("http://127.0.0.1:1")
            .preflight(
                Pipeline::compile(vec![Arc::new(Deny) as Arc<dyn Block<RequestContext>>]).unwrap(),
            )
            .build()
            .unwrap();
        let response = g
            .handle(
                "127.0.0.1:1234".parse().unwrap(),
                Request::new(Body::empty()),
            )
            .await;
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(g.metrics.active.load(Ordering::Relaxed), 0);
        assert_eq!(g.metrics.upstream_errors.load(Ordering::Relaxed), 0);
    }
}

#[cfg(all(test, feature = "lb"))]
mod lb_tests {
    use super::*;
    use wayframe_core::{Block, Execution, Phase};
    use wayframe_lb::{Backend, LoadBalance, Pool, Strategy};
    fn pool(id: &str) -> Pool {
        Pool::new(vec![Backend::new(id)]).unwrap()
    }
    fn route(pool: &Pool) -> Arc<dyn Block<RequestContext>> {
        Arc::new(LoadBalance::new(
            "lb",
            pool.clone(),
            Strategy::LeastConnections,
        ))
    }
    #[tokio::test]
    async fn registered_destinations_require_a_trusted_selection() {
        let gateway = HttpGateway::builder("http://127.0.0.1:9")
            .preflight(Pipeline::compile(vec![]).unwrap())
            .upstreams(vec![Upstream::new("a", "http://127.0.0.1:9/backend")])
            .build()
            .unwrap();
        let response = gateway
            .handle(
                "127.0.0.1:1".parse().unwrap(),
                Request::builder()
                    .uri("/mcp")
                    .header("x-wayframe-backend", "a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(gateway.metrics.upstream_errors.load(Ordering::Relaxed), 0);
    }
    #[tokio::test]
    async fn unknown_selected_id_cannot_fall_back_to_the_default_origin() {
        let pool = pool("b");
        let gateway = HttpGateway::builder("http://127.0.0.1:9")
            .preflight(Pipeline::compile(vec![]).unwrap())
            .policy(Pipeline::compile(vec![route(&pool)]).unwrap())
            .upstreams(vec![Upstream::new("a", "http://127.0.0.1:9/backend")])
            .build()
            .unwrap();
        let response = gateway
            .handle(
                "127.0.0.1:1".parse().unwrap(),
                Request::builder().uri("/mcp").body(Body::empty()).unwrap(),
            )
            .await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(pool.snapshot().unwrap()[0].active, 0);
        assert_eq!(gateway.metrics.upstream_errors.load(Ordering::Relaxed), 0);
    }
    struct Deny;
    impl Block<RequestContext> for Deny {
        fn id(&self) -> &str {
            "deny-after-lb"
        }
        fn phase(&self) -> Phase {
            Phase::Dispatch
        }
        fn execute<'a>(&'a self, _: &'a mut RequestContext) -> Execution<'a> {
            Execution::Immediate(Decision::deny(Reason::Forbidden, "denied"))
        }
    }
    #[tokio::test]
    async fn a_later_denial_releases_the_selected_reservation() {
        let pool = pool("a");
        let gateway = HttpGateway::builder("http://127.0.0.1:9")
            .preflight(Pipeline::compile(vec![]).unwrap())
            .policy(Pipeline::compile(vec![route(&pool), Arc::new(Deny)]).unwrap())
            .upstreams(vec![Upstream::new("a", "http://127.0.0.1:9/backend")])
            .build()
            .unwrap();
        let response = gateway
            .handle(
                "127.0.0.1:1".parse().unwrap(),
                Request::builder().uri("/mcp").body(Body::empty()).unwrap(),
            )
            .await;
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        let stats = pool.snapshot().unwrap();
        assert_eq!(stats[0].active, 0);
        assert_eq!(stats[0].selected, 1);
        assert_eq!(gateway.metrics.upstream_errors.load(Ordering::Relaxed), 0);
    }
    #[tokio::test]
    async fn dropping_an_unpolled_response_releases_its_lease_and_uses_registered_path() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                axum::Router::new().route("/backend", axum::routing::get(|| async { "ok" })),
            )
            .await
            .unwrap();
        });
        let pool = pool("a");
        let gateway = HttpGateway::builder("http://127.0.0.1:9")
            .preflight(Pipeline::compile(vec![]).unwrap())
            .policy(Pipeline::compile(vec![route(&pool)]).unwrap())
            .upstreams(vec![Upstream::new(
                "a",
                format!("http://{address}/backend"),
            )])
            .build()
            .unwrap();
        let response = gateway
            .handle(
                "127.0.0.1:1".parse().unwrap(),
                Request::builder().uri("/mcp").body(Body::empty()).unwrap(),
            )
            .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(pool.snapshot().unwrap()[0].active, 1);
        assert_eq!(response.extensions().get::<BackendId>().unwrap().0, "a");
        drop(response);
        assert_eq!(pool.snapshot().unwrap()[0].active, 0);
        server.abort();
    }
}
