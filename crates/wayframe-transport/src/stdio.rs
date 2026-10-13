//! Supervised JSON-RPC subprocesses. Commands are operator configuration; no shell is invoked.
use anyhow::{ensure, Result};
use futures_util::StreamExt;
use serde::Deserialize;
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, HashMap},
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::AsyncWriteExt,
    process::{ChildStdin, Command},
    sync::{mpsc, Semaphore},
    time::Instant,
};
use tokio_util::codec::{FramedRead, LinesCodec};
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub command: PathBuf,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub environment: BTreeMap<String, String>,
    #[serde(default)]
    pub inherit_env: Vec<String>,
    pub max_in_flight: usize,
    pub max_message_bytes: usize,
}
struct Entry {
    sender: mpsc::Sender<Result<Value>>,
    original: Value,
    progress: Option<Value>,
}
pub type Responses = std::pin::Pin<Box<dyn futures_util::Stream<Item = Result<Value>> + Send>>;
struct Inner {
    input: tokio::sync::Mutex<ChildStdin>,
    pending: Mutex<HashMap<u64, Entry>>,
    sequence: std::sync::atomic::AtomicU64,
    slots: Arc<Semaphore>,
    maximum: usize,
    ready: std::sync::atomic::AtomicBool,
    stop: tokio_util::sync::CancellationToken,
}
/// One generation owns one subprocess. Dropping the owner kills and reaps the child.
pub struct Stdio {
    inner: Arc<Inner>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Stdio {
    fn drop(&mut self) {
        self.task.abort();
    }
}
struct Pending {
    inner: Arc<Inner>,
    id: u64,
}
impl Drop for Pending {
    fn drop(&mut self) {
        if let Ok(mut pending) = self.inner.pending.lock() {
            if pending.remove(&self.id).is_some() {
                self.inner.stop.cancel();
            }
        }
    }
}
impl Stdio {
    pub fn spawn(config: Config) -> Result<Arc<Self>> {
        ensure!(
            config.command.is_absolute()
                && config.command.is_file()
                && config.args.len() <= 128
                && config.args.iter().all(|s| s.len() <= 8192)
                && config.environment.len() <= 128
                && config
                    .environment
                    .iter()
                    .all(|(k, v)| !k.is_empty() && k.len() <= 128 && v.len() <= 8192)
                && (1..=128).contains(&config.max_in_flight)
                && (1024..=1024 * 1024).contains(&config.max_message_bytes),
            "invalid stdio process bounds"
        );
        ensure!(
            config.inherit_env.len() <= 128
                && config.inherit_env.iter().all(|s| !s.is_empty()
                    && s.len() <= 128
                    && s.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')),
            "invalid inherited environment names"
        );
        let inherited = config
            .inherit_env
            .iter()
            .map(|name| Ok((name, std::env::var(name)?)))
            .collect::<Result<Vec<_>>>()?;
        ensure!(
            inherited.iter().all(|(_, v)| v.len() <= 8192),
            "inherited environment value too large"
        );
        let mut child = Command::new(&config.command)
            .args(&config.args)
            .env_clear()
            .envs(config.environment)
            .envs(inherited)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null())
            .kill_on_drop(true)
            .spawn()?;
        let input = child.stdin.take().expect("piped");
        let output = child.stdout.take().expect("piped");
        let inner = Arc::new(Inner {
            input: tokio::sync::Mutex::new(input),
            pending: Mutex::new(HashMap::new()),
            sequence: std::sync::atomic::AtomicU64::new(1),
            slots: Arc::new(Semaphore::new(config.max_in_flight)),
            maximum: config.max_message_bytes,
            ready: std::sync::atomic::AtomicBool::new(true),
            stop: tokio_util::sync::CancellationToken::new(),
        });
        let state = inner.clone();
        let task = tokio::spawn(async move {
            let mut lines = FramedRead::new(output, LinesCodec::new_with_max_length(state.maximum));
            loop {
                tokio::select! {
                    _=child.wait()=>break,
                _=state.stop.cancelled()=>break,
                    line=lines.next()=>{
                        let Some(Ok(line))=line else {break;};let Ok(mut value)=wayframe_codec::parse_unique(line.as_bytes()) else {break;};
                        if value["jsonrpc"]!="2.0" {break;}
                        if value.get("method").is_some() {
                            // Server-initiated requests cannot acquire client privileges through this adapter.
                            if let Some(id)=value.get("id") {let mut bytes=serde_json::to_vec(&json!({"jsonrpc":"2.0","id":id,"error":{"code":-32601,"message":"client callback unavailable"}})).expect("JSON");bytes.push(b'\n');if state.input.lock().await.write_all(&bytes).await.is_err(){break;}}
                            let related=value.pointer("/params/_meta/io.modelcontextprotocol~1subscriptionId").or_else(||value.pointer("/params/progressToken")).and_then(Value::as_u64);
                            if let Some(id)=related {
                                if let Ok(pending)=state.pending.lock() {if let Some(entry)=pending.get(&id) {
                                    if let Some(value)=value.pointer_mut("/params/_meta/io.modelcontextprotocol~1subscriptionId") {*value=entry.original.clone();}
                                    if let (Some(token),Some(value))=(&entry.progress,value.pointer_mut("/params/progressToken")) {*value=token.clone();}
                                    if entry.sender.try_send(Ok(value)).is_err() {state.stop.cancel();}
                                }}
                            }
                            continue;
                        }
                        let Some(id)=value["id"].as_u64() else {break;};
                        if value.get("result").is_some()==value.get("error").is_some() {break;}
                        if let Ok(mut pending)=state.pending.lock(){if let Some(entry)=pending.remove(&id){value["id"]=entry.original; if entry.sender.try_send(Ok(value)).is_err(){state.stop.cancel();}}}
                    }
                }
            }
            state
                .ready
                .store(false, std::sync::atomic::Ordering::Release);
            if let Ok(mut pending) = state.pending.lock() {
                for (_, entry) in pending.drain() {
                    let _ = entry
                        .sender
                        .try_send(Err(anyhow::anyhow!("stdio upstream exited")));
                }
            }
            let _ = child.kill().await;
            let _ = child.wait().await;
        });
        Ok(Arc::new(Self { inner, task }))
    }
    pub fn ready(&self) -> bool {
        self.inner.ready.load(std::sync::atomic::Ordering::Acquire)
            && !self.inner.stop.is_cancelled()
    }
    pub async fn responses(&self, mut message: Value, deadline: Instant) -> Result<Responses> {
        ensure!(self.ready(), "stdio upstream unavailable");
        ensure!(
            message["jsonrpc"] == "2.0" && message["method"].is_string() && message.is_object(),
            "invalid JSON-RPC request"
        );
        let permit = self.inner.slots.clone().try_acquire_owned()?;
        let original = message.get("id").cloned();
        let expecting_result = original.is_some();
        let id = self
            .inner
            .sequence
            .try_update(
                std::sync::atomic::Ordering::Relaxed,
                std::sync::atomic::Ordering::Relaxed,
                |n| n.checked_add(1),
            )
            .map_err(|_| anyhow::anyhow!("request ids exhausted"))?;
        let (sender, mut receiver) = mpsc::channel(8);
        let pending = Pending {
            inner: self.inner.clone(),
            id,
        };
        if let Some(original) = original {
            message["id"] = json!(id);
            let progress = message.pointer("/params/_meta/progressToken").cloned();
            if let Some(value) = message.pointer_mut("/params/_meta/progressToken") {
                *value = json!(id);
            }
            self.inner
                .pending
                .lock()
                .map_err(|_| anyhow::anyhow!("pending poisoned"))?
                .insert(
                    id,
                    Entry {
                        sender,
                        original,
                        progress,
                    },
                );
        }
        let mut bytes = serde_json::to_vec(&message)?;
        ensure!(bytes.len() <= self.inner.maximum, "stdio message too large");
        bytes.push(b'\n');
        tokio::time::timeout_at(deadline, async {
            let mut input = self.inner.input.lock().await;
            input.write_all(&bytes).await?;
            input.flush().await
        })
        .await??;
        let span = tracing::info_span!("transport.stdio", otel.kind = "client");
        use tracing::Instrument;
        Ok(Box::pin(async_stream::try_stream! {
            let _pending = pending;
            let _permit = permit;
            let mut completed = !expecting_result;
            while let Some(value) = tokio::time::timeout_at(deadline, receiver.recv())
                .instrument(span.clone())
                .await?
            {
                let value = value?;
                let done = value.get("method").is_none();
                yield value;
                if done {
                    completed = true;
                    break;
                }
            }
            if !completed {
                Err(anyhow::anyhow!("stdio response ended without a result"))?;
            }
        }))
    }
    pub async fn call(&self, message: Value, deadline: Instant) -> Result<Option<Value>> {
        let mut responses = self.responses(message, deadline).await?;
        while let Some(response) = responses.next().await {
            let response = response?;
            if response.get("method").is_none() {
                return Ok(Some(response));
            }
        }
        Ok(None)
    }
    pub async fn shutdown(&self, budget: Duration) {
        self.task.abort();
        let _ = tokio::time::timeout(budget, async {
            while self
                .inner
                .ready
                .swap(false, std::sync::atomic::Ordering::AcqRel)
            {
                tokio::task::yield_now().await;
            }
        })
        .await;
    }
}
