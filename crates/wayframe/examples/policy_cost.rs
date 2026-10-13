//! Measures real execution boundaries. HTTPS uses an explicitly configured test policy endpoint.
use serde_json::json;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use wayframe::{
    http::{Identity, RequestContext},
    prelude::*,
    transport::{ExternalPolicy, TaskBridge},
};

struct Noop;
impl Block<RequestContext> for Noop {
    fn id(&self) -> &str {
        "native"
    }
    fn phase(&self) -> Phase {
        Phase::Authorize
    }
    fn execute<'a>(&'a self, _: &'a mut RequestContext) -> Execution<'a> {
        Execution::Immediate(Decision::Continue)
    }
}
fn context() -> RequestContext {
    let mut c = RequestContext::new(
        "POST".parse().unwrap(),
        "/mcp".into(),
        "127.0.0.1".parse().unwrap(),
        tokio::time::Instant::now() + Duration::from_secs(3600),
    );
    c.identity = Some(Identity::new("reader", "benchmark"));
    c
}
async fn measure(
    name: &str,
    block: Arc<dyn Block<RequestContext>>,
    runs: usize,
    pool: Option<&wayframe::wasm::Pool>,
) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    let mut ctx = context();
    let mut samples = Vec::with_capacity(runs);
    let mut errors = 0usize;
    let started = Instant::now();
    for n in 0..(runs + 100) {
        if let Some(pool) = pool {
            tokio::time::timeout(Duration::from_secs(5), async {
                while pool.stats().ready == 0 {
                    tokio::task::yield_now().await;
                }
            })
            .await?;
        }
        ctx.request_id = n as u64;
        let start = Instant::now();
        let result = block.execute(&mut ctx).resolve().await;
        let ns = start.elapsed().as_nanos() as u64;
        if n >= 100 {
            samples.push(ns);
            if result != Decision::Continue {
                errors += 1;
            }
        }
    }
    let elapsed = started.elapsed().as_secs_f64();
    let mut sorted = samples.clone();
    sorted.sort_unstable();
    let percentile = |q: f64| sorted[((sorted.len() - 1) as f64 * q).round() as usize];
    Ok(
        json!({"boundary":name,"runs":runs,"mean_ns":samples.iter().map(|n| *n as f64).sum::<f64>()/runs as f64,
        "p50_ns":percentile(0.5),"p95_ns":percentile(0.95),"p99_ns":percentile(0.99),"errors":errors,
        "elapsed_seconds_including_warmup_and_pool_wait":elapsed,"raw_ns":samples,
        "wasm_stats":pool.map(|p| p.stats())}),
    )
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runs = std::env::var("WAYFRAME_POLICY_RUNS")
        .unwrap_or_else(|_| "2000".into())
        .parse::<usize>()?;
    if !(100..=100000).contains(&runs) {
        return Err("policy runs must be 100..100000".into());
    }
    let mut results = vec![measure("native", Arc::new(Noop), runs, None).await?];
    let task = TaskBridge::new("task".into(), Arc::new(Noop), 64)?;
    results.push(measure("task", Arc::new(task), runs, None).await?);
    #[cfg(unix)]
    {
        use futures_util::{SinkExt, StreamExt};
        use tokio_util::codec::{Framed, LengthDelimitedCodec};
        let path =
            std::env::temp_dir().join(format!("wayframe-policy-{}.sock", std::process::id()));
        let listener = tokio::net::UnixListener::bind(&path)?;
        struct Cleanup(std::path::PathBuf);
        impl Drop for Cleanup {
            fn drop(&mut self) {
                let _ = std::fs::remove_file(&self.0);
            }
        }
        let _cleanup = Cleanup(path.clone());
        let client = tokio::net::UnixStream::connect(&path).await?;
        let uid = client.peer_cred()?.uid();
        let (server, _) = listener.accept().await?;
        drop(client);
        drop(server);
        let server = tokio::spawn(async move {
            loop {
                let (stream, _) = listener.accept().await?;
                let mut wire = Framed::new(
                    stream,
                    LengthDelimitedCodec::builder()
                        .max_frame_length(wayframe::transport::MAX_FRAME)
                        .new_codec(),
                );
                let bytes = wire.next().await.ok_or("missing policy request")??;
                let request: wayframe::transport::PolicyRequest = serde_json::from_slice(&bytes)?;
                let response = wayframe::transport::PolicyResponse {
                    version: 1,
                    request_id: request.request_id,
                    status: 0,
                };
                wire.send(bytes::Bytes::from(serde_json::to_vec(&response)?))
                    .await?;
            }
            #[allow(unreachable_code)]
            Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
        });
        let policy = ExternalPolicy::unix("unix".into(), path, uid, Duration::from_secs(2))?;
        results.push(measure("unix", Arc::new(policy), runs, None).await?);
        server.abort();
        let _ = server.await;
    }
    if let Ok(url) = std::env::var("WAYFRAME_POLICY_HTTPS") {
        let token = std::env::var("WAYFRAME_UPSTREAM_TOKEN")?;
        let ca = std::env::var("WAYFRAME_POLICY_CA")
            .ok()
            .map(std::fs::read)
            .transpose()?;
        let policy = ExternalPolicy::https_with_ca(
            "https".into(),
            &url,
            token,
            Duration::from_secs(2),
            ca.as_deref(),
        )?;
        results.push(measure("https", Arc::new(policy), runs, None).await?);
    }
    let runtime = wayframe::wasm::Runtime::new()?;
    let pool = runtime
        .load(std::path::Path::new("plugins/path-control/plugin.toml"))
        .await?;
    let wasm = wayframe::wasm::WasmBlock::new("wasm".into(), pool.clone());
    results.push(measure("wasm", Arc::new(wasm), runs, Some(&pool)).await?);
    println!(
        "{}",
        serde_json::to_string_pretty(
            &json!({"kind":"policy_boundaries","clock":"Instant per invocation; native values include clock overhead","wasm":"Single-use instances. Invocation latency excludes waiting for a clean instance; elapsed time includes replenishment waits.","results":results})
        )?
    );
    if results
        .iter()
        .any(|r| r["errors"].as_u64().unwrap_or(1) != 0)
    {
        return Err("policy boundary returned errors".into());
    }
    Ok(())
}
