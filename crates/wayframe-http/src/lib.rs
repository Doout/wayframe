//! Opt-in HTTP adapter. Applications own listeners, routes and deployment policy.
//! This crate never requires MCP, WebAssembly, a registry, or a management service.
mod context;
pub mod stream;
pub use context::{Identity, Operation, RequestContext};

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
use wayframe_core::{Decision, Reason};
use wayframe_runtime::Pipeline;

/// Stable HTTP mapping lives here, not in the core block contract.
pub const fn status_for(reason: Reason) -> u16 {
    match reason {
        Reason::InvalidInput => 400,
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
    fn validate(&self) -> Result<()> {
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
#[derive(Default)]
pub struct Metrics {
    pub requests: AtomicU64,
    pub active: AtomicU64,
    pub denied: AtomicU64,
    pub overload: AtomicU64,
    pub upstream_errors: AtomicU64,
    pub bytes_in: AtomicU64,
    pub bytes_out: AtomicU64,
}
impl Metrics {
    pub fn snapshot(&self) -> serde_json::Value {
        let read = |n: &AtomicU64| n.load(Ordering::Relaxed);
        serde_json::json!({"requests":read(&self.requests), "active":read(&self.active),
            "denied":read(&self.denied), "overload":read(&self.overload),
            "upstream_errors":read(&self.upstream_errors), "bytes_in":read(&self.bytes_in),
            "bytes_out":read(&self.bytes_out)})
    }
}
struct Active {
    metrics: Arc<Metrics>,
    _permit: OwnedSemaphorePermit,
}
impl Active {
    fn new(metrics: Arc<Metrics>, permit: OwnedSemaphorePermit) -> Self {
        metrics.active.fetch_add(1, Ordering::Relaxed);
        Self {
            metrics,
            _permit: permit,
        }
    }
}
impl Drop for Active {
    fn drop(&mut self) {
        self.metrics.active.fetch_sub(1, Ordering::Relaxed);
    }
}

pub struct GatewayBuilder {
    origin: String,
    preflight: Option<Pipeline<RequestContext>>,
    policy: Option<Pipeline<RequestContext>>,
    limits: Limits,
    inspection_bytes: Option<usize>,
    authorities: Vec<String>,
    upstream_token: Option<String>,
}
impl GatewayBuilder {
    pub fn new(origin: impl Into<String>) -> Self {
        Self {
            origin: origin.into(),
            preflight: None,
            policy: None,
            limits: Limits::default(),
            inspection_bytes: None,
            authorities: Vec::new(),
            upstream_token: None,
        }
    }
    /// Authentication and cheap admission run before collecting a body.
    pub fn preflight(mut self, pipeline: Pipeline<RequestContext>) -> Self {
        self.preflight = Some(pipeline);
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
    pub fn upstream_bearer(mut self, token: String) -> Self {
        self.upstream_token = Some(token);
        self
    }
    pub fn build(self) -> Result<Arc<HttpGateway>> {
        self.limits.validate()?;
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
        let client = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(3))
            .pool_max_idle_per_host(32)
            .build()?;
        Ok(Arc::new(HttpGateway {
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
            client,
            metrics: Arc::new(Metrics::default()),
            sequence: AtomicU64::new(1),
        }))
    }
}

pub struct HttpGateway {
    origin: String,
    preflight: Pipeline<RequestContext>,
    policy: Pipeline<RequestContext>,
    limits: Limits,
    inspection_bytes: Option<usize>,
    authorities: Vec<String>,
    upstream_token: Option<String>,
    client: reqwest::Client,
    admission: Arc<Semaphore>,
    metrics: Arc<Metrics>,
    sequence: AtomicU64,
}
impl HttpGateway {
    pub fn builder(origin: impl Into<String>) -> GatewayBuilder {
        GatewayBuilder::new(origin)
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
        self.metrics.requests.fetch_add(1, Ordering::Relaxed);
        let Ok(permit) = self.admission.clone().try_acquire_owned() else {
            self.metrics.overload.fetch_add(1, Ordering::Relaxed);
            return rejection(Reason::Unavailable, "overloaded");
        };
        let active = Active::new(self.metrics.clone(), permit);
        let limits = &self.limits;
        let deadline = Instant::now() + limits.request_timeout;
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
        ctx.request_id = self.sequence.fetch_add(1, Ordering::Relaxed);
        ctx.headers = request.headers().clone();
        if let Some(response) = self.deny(self.preflight.evaluate(&mut ctx).await) {
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
            let bytes = match timeout_at(deadline, to_bytes(original_body, maximum)).await {
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
        if let Some(response) = self.deny(self.policy.evaluate(&mut ctx).await) {
            return response;
        }
        // Policies inspect canonical paths; forwarding uses the same canonical path.
        let mut target: reqwest::Url = self.origin.parse().expect("validated origin");
        target.set_path(&ctx.path);
        target.set_query(parts.uri.query());
        stream::strip_hop_headers(&mut parts.headers);
        stream::strip_client_identity(&mut parts.headers);
        if let Some(length) = inspected_length {
            parts.headers.insert(
                http::header::CONTENT_LENGTH,
                http::HeaderValue::from_str(&length.to_string()).expect("decimal length"),
            );
        }
        // An inspected envelope has a bounded, known body, but streaming remains uniform.
        let input = stream::constrained(
            body.into_data_stream(),
            limits.max_request_bytes,
            limits.per_stream_bytes_per_second,
            deadline,
            self.metrics.clone(),
            false,
        );
        let mut upstream = self
            .client
            .request(parts.method, target)
            .headers(parts.headers)
            .body(reqwest::Body::wrap_stream(input));
        if let Some(token) = &self.upstream_token {
            upstream = upstream.bearer_auth(token);
        }
        let result = match timeout_at(deadline, upstream.send()).await {
            Ok(Ok(r)) => r,
            Ok(Err(_)) => {
                self.metrics.upstream_errors.fetch_add(1, Ordering::Relaxed);
                return (StatusCode::BAD_GATEWAY, "upstream failed").into_response();
            }
            Err(_) => return rejection(Reason::DeadlineExceeded, "upstream_deadline"),
        };
        let status = result.status();
        let mut headers = result.headers().clone();
        stream::strip_hop_headers(&mut headers);
        if result
            .content_length()
            .is_some_and(|n| n > limits.max_response_bytes)
        {
            return (StatusCode::BAD_GATEWAY, "upstream response too large").into_response();
        }
        let output = stream::constrained(
            result.bytes_stream(),
            limits.max_response_bytes,
            limits.per_stream_bytes_per_second,
            deadline,
            self.metrics.clone(),
            true,
        );
        let generation = self.clone();
        let stream = async_stream::stream! {
            let _active = active; // retain the admission permit until EOF or disconnect
            let _generation = generation; // retain old policy/pool resources through streaming
            tokio::pin!(output);
            while let Some(chunk) = output.next().await { yield chunk; }
        };
        let mut response = Response::new(Body::from_stream(stream));
        *response.status_mut() = status;
        *response.headers_mut() = headers;
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
