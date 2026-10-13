//! Explicit execution boundaries. Direct calls remain the default.
use anyhow::{anyhow, ensure, Context as _, Result};
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::{path::PathBuf, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::{mpsc, oneshot},
    time::timeout_at,
};
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use wayframe_core::{Block, Context, Decision, Execution, Phase};
use wayframe_http::{policy_decision, RequestContext};

pub const MAX_FRAME: usize = 16 * 1024;
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PolicyRequest {
    pub version: u16,
    pub request_id: u64,
    pub method: String,
    pub path: String,
    pub subject: String,
    pub tenant: String,
    pub operation: String,
    pub target: String,
    pub remaining_ms: u64,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PolicyResponse {
    pub version: u16,
    pub request_id: u64,
    pub status: u16,
}
impl PolicyRequest {
    fn from_context(c: &RequestContext) -> Self {
        Self {
            version: 1,
            request_id: c.request_id,
            method: c.method.to_string(),
            path: c.path.clone(),
            subject: c
                .identity
                .as_ref()
                .map(|i| i.subject.clone())
                .unwrap_or_default(),
            tenant: c
                .identity
                .as_ref()
                .map(|i| i.tenant.clone())
                .unwrap_or_default(),
            operation: c
                .operation
                .as_ref()
                .map(|o| o.action.clone())
                .unwrap_or_default(),
            target: c
                .operation
                .as_ref()
                .and_then(|o| o.target.clone())
                .unwrap_or_default(),
            remaining_ms: c
                .deadline
                .saturating_duration_since(tokio::time::Instant::now())
                .as_millis()
                .min(u128::from(u64::MAX)) as u64,
        }
    }
}
fn response_decision(r: PolicyResponse, id: u64) -> Result<Decision> {
    ensure!(
        r.version == 1 && r.request_id == id,
        "invalid policy response correlation/version"
    );
    match r.status {
        0 => Ok(Decision::Continue),
        400 | 401 | 403 | 404 | 409 | 413 | 429 => {
            Ok(policy_decision(r.status, "external_policy_denied"))
        }
        _ => Err(anyhow!("unsupported policy status")),
    }
}
/// Four-byte big-endian length + bounded JSON. One exchange per connection in v0.1.
pub async fn exchange<T>(io: T, request: &PolicyRequest) -> Result<PolicyResponse>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    let bytes = serde_json::to_vec(request)?;
    ensure!(bytes.len() <= MAX_FRAME, "request too large");
    let codec = LengthDelimitedCodec::builder()
        .max_frame_length(MAX_FRAME)
        .new_codec();
    let mut wire = Framed::new(io, codec);
    wire.send(Bytes::from(bytes)).await?;
    let reply = wire
        .next()
        .await
        .ok_or_else(|| anyhow!("policy peer closed"))??;
    Ok(serde_json::from_slice(&reply)?)
}

enum Endpoint {
    #[cfg(unix)]
    Unix { path: PathBuf, expected_uid: u32 },
    Https {
        url: reqwest::Url,
        token: String,
        client: reqwest::Client,
    },
}
pub struct ExternalPolicy {
    id: String,
    endpoint: Endpoint,
    timeout: Duration,
}
impl ExternalPolicy {
    #[cfg(unix)]
    pub fn unix(id: String, path: PathBuf, expected_uid: u32, timeout: Duration) -> Result<Self> {
        ensure!(path.is_absolute(), "Unix socket paths must be absolute");
        ensure!(!timeout.is_zero(), "timeout must be positive");
        Ok(Self {
            id,
            endpoint: Endpoint::Unix { path, expected_uid },
            timeout,
        })
    }
    pub fn https(id: String, url: &str, token: String, timeout: Duration) -> Result<Self> {
        let url: reqwest::Url = url.parse()?;
        ensure!(
            url.scheme() == "https"
                && url.username().is_empty()
                && url.password().is_none()
                && url.fragment().is_none(),
            "policy endpoint must use HTTPS without URL credentials/fragment"
        );
        ensure!(
            token.len() >= 32 && !timeout.is_zero(),
            "invalid policy token or timeout"
        );
        let client = reqwest::Client::builder()
            .https_only(true)
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(timeout)
            .build()?;
        Ok(Self {
            id,
            endpoint: Endpoint::Https { url, token, client },
            timeout,
        })
    }
    async fn call(&self, request: &PolicyRequest) -> Result<PolicyResponse> {
        match &self.endpoint {
            #[cfg(unix)]
            Endpoint::Unix { path, expected_uid } => {
                let stream = tokio::net::UnixStream::connect(path).await?;
                ensure!(
                    stream.peer_cred()?.uid() == *expected_uid,
                    "Unix policy peer UID mismatch"
                );
                exchange(stream, request).await
            }
            Endpoint::Https { url, token, client } => {
                let mut response = client
                    .post(url.clone())
                    .bearer_auth(token)
                    .json(request)
                    .send()
                    .await?
                    .error_for_status()?;
                let mut data = Vec::new();
                while let Some(chunk) = response.chunk().await? {
                    ensure!(
                        data.len().saturating_add(chunk.len()) <= MAX_FRAME,
                        "policy response too large"
                    );
                    data.extend_from_slice(&chunk);
                }
                Ok(serde_json::from_slice(&data)?)
            }
        }
    }
}
impl Block<RequestContext> for ExternalPolicy {
    fn id(&self) -> &str {
        &self.id
    }
    fn phase(&self) -> Phase {
        Phase::Authorize
    }
    fn execute<'a>(&'a self, ctx: &'a mut RequestContext) -> Execution<'a> {
        Execution::Deferred(Box::pin(async move {
            let request = PolicyRequest::from_context(ctx);
            let deadline = ctx.deadline.min(tokio::time::Instant::now() + self.timeout);
            match timeout_at(deadline, self.call(&request)).await {
                Ok(Ok(reply)) => response_decision(reply, request.request_id)
                    .unwrap_or_else(|_| Decision::unavailable()),
                _ => Decision::unavailable(),
            }
        }))
    }
}

struct Job<C: Context> {
    context: C,
    reply: oneshot::Sender<(C, Decision)>,
}
/// One worker + bounded mailbox. This is an explicit scheduling boundary, not a default per-block task.
pub struct TaskBridge<C: Context + Clone> {
    id: String,
    phase: Phase,
    tx: mpsc::Sender<Job<C>>,
}
impl<C: Context + Clone> TaskBridge<C> {
    pub fn new(id: String, inner: Arc<dyn Block<C>>, capacity: usize) -> Result<Self> {
        ensure!((1..=4096).contains(&capacity), "invalid queue capacity");
        let phase = inner.phase();
        let (tx, mut rx) = mpsc::channel::<Job<C>>(capacity);
        let runtime = tokio::runtime::Handle::try_current()
            .context("TaskBridge needs an active Tokio runtime")?;
        runtime.spawn(async move {
            while let Some(mut job) = rx.recv().await {
                if job.reply.is_closed() {
                    continue;
                }
                let deadline: tokio::time::Instant = job.context.deadline().into();
                if tokio::time::Instant::now() >= deadline {
                    let _ = job.reply.send((job.context, Decision::unavailable()));
                    continue;
                }
                // Stop queued/running cooperative work when the caller goes away.
                let result = tokio::select! {
                    _ = job.reply.closed() => continue,
                    r = timeout_at(deadline, inner.execute(&mut job.context).resolve()) =>
                        r.unwrap_or_else(|_| Decision::unavailable()),
                };
                let _ = job.reply.send((job.context, result));
            }
        });
        Ok(Self { id, phase, tx })
    }
}
impl<C: Context + Clone> Block<C> for TaskBridge<C> {
    fn id(&self) -> &str {
        &self.id
    }
    fn phase(&self) -> Phase {
        self.phase
    }
    fn execute<'a>(&'a self, ctx: &'a mut C) -> Execution<'a> {
        Execution::Deferred(Box::pin(async move {
            let (reply, rx) = oneshot::channel();
            if self
                .tx
                .try_send(Job {
                    context: ctx.clone(),
                    reply,
                })
                .is_err()
            {
                return Decision::unavailable();
            }
            match timeout_at(ctx.deadline().into(), rx).await {
                Ok(Ok((updated, decision))) => {
                    *ctx = updated;
                    decision
                }
                _ => Decision::unavailable(),
            }
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn response_version_id_and_decision_are_checked() {
        assert!(response_decision(
            PolicyResponse {
                version: 2,
                request_id: 1,
                status: 0
            },
            1
        )
        .is_err());
        assert!(response_decision(
            PolicyResponse {
                version: 1,
                request_id: 2,
                status: 0
            },
            1
        )
        .is_err());
        assert!(response_decision(
            PolicyResponse {
                version: 1,
                request_id: 1,
                status: 200
            },
            1
        )
        .is_err());
        assert_eq!(
            response_decision(
                PolicyResponse {
                    version: 1,
                    request_id: 1,
                    status: 0
                },
                1
            )
            .unwrap(),
            Decision::Continue
        );
    }
    #[tokio::test]
    async fn framed_exchange_handles_partial_io() {
        let (client, server) = tokio::io::duplex(8);
        let task = tokio::spawn(async move {
            let mut wire = Framed::new(
                server,
                LengthDelimitedCodec::builder()
                    .max_frame_length(MAX_FRAME)
                    .new_codec(),
            );
            let bytes = wire.next().await.unwrap().unwrap();
            let request: PolicyRequest = serde_json::from_slice(&bytes).unwrap();
            let reply = PolicyResponse {
                version: 1,
                request_id: request.request_id,
                status: 403,
            };
            wire.send(Bytes::from(serde_json::to_vec(&reply).unwrap()))
                .await
                .unwrap();
        });
        let r = exchange(
            client,
            &PolicyRequest {
                version: 1,
                request_id: 7,
                method: "GET".into(),
                path: "/".into(),
                subject: "s".into(),
                tenant: "t".into(),
                operation: String::new(),
                target: String::new(),
                remaining_ms: 100,
            },
        )
        .await
        .unwrap();
        assert_eq!(r.status, 403);
        task.await.unwrap();
    }
}

#[cfg(test)]
mod task_tests {
    use super::*;
    #[derive(Clone)]
    struct Work {
        due: std::time::Instant,
        value: usize,
    }
    impl Context for Work {
        fn deadline(&self) -> std::time::Instant {
            self.due
        }
    }
    struct Increment;
    impl Block<Work> for Increment {
        fn id(&self) -> &str {
            "increment"
        }
        fn phase(&self) -> Phase {
            Phase::Inspect
        }
        fn execute<'a>(&'a self, work: &'a mut Work) -> Execution<'a> {
            work.value += 1;
            Execution::Immediate(Decision::Continue)
        }
    }
    #[test]
    fn task_bridge_requires_runtime_without_panicking() {
        assert!(TaskBridge::new("task".into(), Arc::new(Increment), 1).is_err());
    }
    #[tokio::test]
    async fn task_bridge_accepts_non_http_contexts() {
        let bridge = TaskBridge::new("task".into(), Arc::new(Increment), 1).unwrap();
        let mut work = Work {
            due: std::time::Instant::now() + Duration::from_secs(1),
            value: 0,
        };
        assert_eq!(
            bridge.execute(&mut work).resolve().await,
            Decision::Continue
        );
        assert_eq!(work.value, 1);
    }
}
