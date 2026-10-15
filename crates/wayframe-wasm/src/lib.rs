//! Clean, single-use component pools. Compilation and instantiation do not happen on a warm-hit path.
use anyhow::{anyhow, ensure, Context, Result};
use crossbeam_queue::ArrayQueue;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, HashSet},
    path::Path,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex, Weak,
    },
    thread,
    time::Duration,
};
use tokio::{
    io::AsyncReadExt,
    sync::{OwnedSemaphorePermit, Semaphore},
    time::{timeout, timeout_at, Instant},
};
use wasmtime::component::{Component, InstancePre, Linker, TypedFunc};
use wasmtime::{
    Config, Engine, InstanceAllocationStrategy, OptLevel, PoolingAllocationConfig, Store,
    StoreLimits, StoreLimitsBuilder,
};
#[cfg(test)]
use wayframe_core::Reason;
use wayframe_core::{Block, Decision, Execution, Phase};
use wayframe_http::{policy_decision, RequestContext};

const MAX_COMPONENT: usize = 4 * 1024 * 1024;
const EPOCH_MS: u64 = 5;
type Args = (String, String, String, String, String, String);
type Authorize = TypedFunc<Args, (u16,)>;
fn w<T>(r: wasmtime::Result<T>) -> Result<T> {
    r.map_err(|e| anyhow!("{e}"))
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Manifest {
    pub name: String,
    pub version: String,
    pub abi: String,
    pub component: String,
    pub sha256: String,
    pub capabilities: Vec<String>,
    pub warm_instances: usize,
    pub memory_mib: usize,
    pub fuel: u64,
    pub timeout_ms: u64,
    pub init_timeout_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signature: Option<ArtifactSignature>,
}
#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactSignature {
    pub key_id: String,
    pub ed25519: String,
}
pub struct Trust {
    pub keys: std::collections::BTreeMap<String, ed25519_dalek::VerifyingKey>,
    pub capabilities: std::collections::BTreeSet<String>,
}
impl Trust {
    pub fn from_hex(
        keys: &std::collections::BTreeMap<String, String>,
        capabilities: std::collections::BTreeSet<String>,
    ) -> Result<Self> {
        ensure!(
            !keys.is_empty() && keys.len() <= 64 && capabilities.len() <= 6,
            "invalid publisher trust bounds"
        );
        let mut trusted = std::collections::BTreeMap::new();
        for (id, hex) in keys {
            ensure!(
                !id.is_empty() && id.len() <= 64 && hex.len() == 64,
                "invalid publisher key"
            );
            let key: [u8; 32] = hex::decode(hex)?
                .try_into()
                .map_err(|_| anyhow!("invalid public key size"))?;
            trusted.insert(id.clone(), ed25519_dalek::VerifyingKey::from_bytes(&key)?);
        }
        Ok(Self {
            keys: trusted,
            capabilities,
        })
    }
}
impl Manifest {
    pub fn signing_bytes(&self) -> Result<Vec<u8>> {
        let mut payload = self.clone();
        payload.signature = None;
        Ok(serde_json::to_vec(&payload)?)
    }
    pub fn verify(&self, trust: &Trust) -> Result<()> {
        let signature = self
            .signature
            .as_ref()
            .ok_or_else(|| anyhow!("publisher signature required"))?;
        ensure!(
            signature.key_id.len() <= 64 && signature.ed25519.len() == 128,
            "invalid artifact signature"
        );
        let key = trust
            .keys
            .get(&signature.key_id)
            .ok_or_else(|| anyhow!("untrusted publisher"))?;
        let bytes = hex::decode(&signature.ed25519)?;
        let signature = ed25519_dalek::Signature::from_slice(&bytes)?;
        key.verify_strict(&self.signing_bytes()?, &signature)?;
        ensure!(
            self.capabilities
                .iter()
                .all(|cap| trust.capabilities.contains(cap)),
            "publisher requested an unapproved capability"
        );
        Ok(())
    }
}
impl Manifest {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            !self.name.is_empty()
                && self.name.len() <= 64
                && self
                    .name
                    .bytes()
                    .all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_'),
            "invalid plugin name"
        );
        ensure!(
            !self.version.is_empty() && self.version.len() <= 64,
            "invalid version"
        );
        ensure!(
            self.abi == "wayframe:gateway/policy@0.1.0",
            "unsupported plugin ABI"
        );
        ensure!(
            !self.component.is_empty()
                && self.component.len() <= 1024
                && Path::new(&self.component)
                    .components()
                    .all(|c| matches!(c, std::path::Component::Normal(_))),
            "component must be a relative path with no parent traversal"
        );
        ensure!(
            self.sha256.len() == 64 && self.sha256.bytes().all(|b| b.is_ascii_hexdigit()),
            "invalid SHA-256"
        );
        ensure!(
            (1..=64).contains(&self.warm_instances),
            "warm_instances must be 1..64"
        );
        ensure!(
            (1..=64).contains(&self.memory_mib),
            "memory_mib must be 1..64"
        );
        ensure!((1..=10_000_000).contains(&self.fuel), "invalid fuel budget");
        ensure!(
            (1..=1000).contains(&self.timeout_ms) && (1..=5000).contains(&self.init_timeout_ms),
            "invalid time budget"
        );
        let valid = [
            "request.method",
            "request.path",
            "identity.subject",
            "identity.tenant",
            "operation.action",
            "operation.target",
        ];
        let mut seen = HashSet::new();
        for c in &self.capabilities {
            ensure!(
                valid.contains(&c.as_str()) && seen.insert(c),
                "unknown/duplicate capability"
            );
        }
        Ok(())
    }
    fn permits(&self, capability: &str) -> bool {
        self.capabilities.iter().any(|c| c == capability)
    }
}

struct EpochDriver {
    stop: Arc<AtomicBool>,
    thread: Option<thread::JoinHandle<()>>,
}
impl EpochDriver {
    fn new(engine: Engine) -> Result<Self> {
        let stop = Arc::new(AtomicBool::new(false));
        let stopped = stop.clone();
        let thread = thread::Builder::new()
            .name("wasm-epoch".into())
            .spawn(move || {
                while !stopped.load(Ordering::Relaxed) {
                    thread::sleep(Duration::from_millis(EPOCH_MS));
                    engine.increment_epoch();
                }
            })?;
        Ok(Self {
            stop,
            thread: Some(thread),
        })
    }
}
impl Drop for EpochDriver {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

/// One engine per process; immutable compiled components may be shared between policy generations.
pub struct Runtime {
    engine: Engine,
    cache: Mutex<HashMap<String, Weak<Component>>>,
    compile_gate: Arc<Semaphore>,
    slot_budget: Arc<Semaphore>,
    memory_budget_mib: Arc<Semaphore>,
    _epoch: EpochDriver,
}
impl Runtime {
    /// Download a signed manifest/component from one fixed HTTPS registry origin.
    /// Signatures approve publisher identity; the operator's capability grant remains mandatory.
    pub async fn load_remote(self: &Arc<Self>, url: &str, trust: &Trust) -> Result<Arc<Pool>> {
        let url: reqwest::Url = url.parse()?;
        ensure!(
            url.scheme() == "https"
                && url.username().is_empty()
                && url.password().is_none()
                && url.query().is_none()
                && url.fragment().is_none(),
            "registry must use HTTPS without URL credentials"
        );
        let client = reqwest::Client::builder()
            .https_only(true)
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(15))
            .build()?;
        async fn fetch(client: &reqwest::Client, url: reqwest::Url, max: usize) -> Result<Vec<u8>> {
            let mut response = client.get(url).send().await?.error_for_status()?;
            let mut bytes = Vec::new();
            while let Some(chunk) = response.chunk().await? {
                ensure!(
                    bytes.len().saturating_add(chunk.len()) <= max,
                    "registry download limit"
                );
                bytes.extend_from_slice(&chunk);
            }
            Ok(bytes)
        }
        let text = fetch(&client, url.clone(), 16384).await?;
        let manifest: Manifest = toml::from_str(std::str::from_utf8(&text)?)?;
        manifest.validate()?;
        manifest.verify(trust)?;
        let component_url = url.join(&manifest.component)?;
        ensure!(
            component_url.origin() == url.origin()
                && component_url.query().is_none()
                && component_url.fragment().is_none(),
            "component must remain within registry origin"
        );
        let bytes = fetch(&client, component_url, MAX_COMPONENT).await?;
        let digest = hex::encode(Sha256::digest(&bytes));
        ensure!(
            digest.eq_ignore_ascii_case(&manifest.sha256),
            "registry artifact digest mismatch"
        );
        let component = self.compile(&digest, bytes).await?;
        Pool::prepare(self.clone(), component, manifest).await
    }
    pub fn new() -> Result<Arc<Self>> {
        let mut allocation = PoolingAllocationConfig::default();
        allocation
            .total_component_instances(256)
            .total_core_instances(1024)
            .total_memories(256)
            .total_tables(512)
            .total_stacks(256)
            .max_core_instances_per_component(4)
            .max_memories_per_component(1)
            .max_tables_per_component(2)
            .max_memory_size(64 * 1024 * 1024)
            .table_elements(4096);
        let mut config = Config::new();
        config
            .consume_fuel(true)
            .epoch_interruption(true)
            .wasm_component_model(true)
            .cranelift_opt_level(OptLevel::Speed)
            .allocation_strategy(InstanceAllocationStrategy::Pooling(allocation))
            .memory_init_cow(true);
        let engine = w(Engine::new(&config))?;
        let epoch = EpochDriver::new(engine.clone())?;
        Ok(Arc::new(Self {
            engine,
            cache: Mutex::new(HashMap::new()),
            compile_gate: Arc::new(Semaphore::new(1)),
            slot_budget: Arc::new(Semaphore::new(256)),
            memory_budget_mib: Arc::new(Semaphore::new(512)),
            _epoch: epoch,
        }))
    }
    async fn compile(self: &Arc<Self>, digest: &str, bytes: Vec<u8>) -> Result<Arc<Component>> {
        // Single-flight compilation across installs. Compiling untrusted code still needs OS containment.
        let permit = self.compile_gate.clone().acquire_owned().await?;
        {
            let mut cache = self.cache.lock().map_err(|_| anyhow!("cache poisoned"))?;
            cache.retain(|_, v| v.strong_count() > 0);
            if let Some(c) = cache.get(digest).and_then(Weak::upgrade) {
                return Ok(c);
            }
        }
        let engine = self.engine.clone();
        let component = Arc::new(
            tokio::task::spawn_blocking(move || {
                // A cancelled reload cannot release the compiler slot while this non-cancellable task still runs.
                let _permit = permit;
                w(Component::new(&engine, &bytes))
            })
            .await??,
        );
        self.cache
            .lock()
            .map_err(|_| anyhow!("cache poisoned"))?
            .insert(digest.into(), Arc::downgrade(&component));
        Ok(component)
    }
    pub async fn load(self: &Arc<Self>, manifest_path: &Path) -> Result<Arc<Pool>> {
        self.load_with_trust(manifest_path, None).await
    }
    pub async fn load_with_trust(
        self: &Arc<Self>,
        manifest_path: &Path,
        trust: Option<&Trust>,
    ) -> Result<Arc<Pool>> {
        let text = bounded_read(manifest_path, 16 * 1024).await?;
        let manifest: Manifest = toml::from_str(std::str::from_utf8(&text)?)?;
        manifest.validate()?;
        if let Some(trust) = trust {
            manifest.verify(trust)?;
        }
        let parent = manifest_path.parent().unwrap_or(Path::new("."));
        let path = parent.join(&manifest.component);
        let bytes = bounded_read(&path, MAX_COMPONENT).await?;
        let digest = hex::encode(Sha256::digest(&bytes));
        ensure!(
            digest.eq_ignore_ascii_case(&manifest.sha256),
            "component digest mismatch"
        );
        let component = self.compile(&digest, bytes).await?;
        Pool::prepare(self.clone(), component, manifest).await
    }
}
async fn bounded_read(path: &Path, limit: usize) -> Result<Vec<u8>> {
    let file = tokio::fs::File::open(path)
        .await
        .with_context(|| format!("read {}", path.display()))?;
    ensure!(file.metadata().await?.is_file(), "expected regular file");
    let mut data = Vec::new();
    file.take(limit as u64 + 1).read_to_end(&mut data).await?;
    ensure!(data.len() <= limit, "file exceeds admission size limit");
    Ok(data)
}

struct Warm {
    store: Store<StoreLimits>,
    function: Authorize,
}
#[derive(Default)]
struct Counters {
    hits: AtomicU64,
    misses: AtomicU64,
    errors: AtomicU64,
    refills: AtomicU64,
    refill_errors: AtomicU64,
}
#[derive(Serialize)]
pub struct PoolStats {
    pub plugin: String,
    pub version: String,
    pub ready: usize,
    pub target: usize,
    pub warm_hits: u64,
    pub warm_misses: u64,
    pub invocation_errors: u64,
    pub refills: u64,
    pub refill_errors: u64,
    pub degraded: bool,
    pub code_tier: &'static str,
    pub instance_policy: &'static str,
}

pub struct Pool {
    runtime: Arc<Runtime>,
    _component: Arc<Component>,
    pre: InstancePre<StoreLimits>,
    manifest: Manifest,
    ready: ArrayQueue<Warm>,
    refill_gate: Arc<Semaphore>,
    counters: Counters,
    degraded: AtomicBool,
    // Reservations cover READY + LEASED + REPLENISHING instances, across all live generations.
    _slots: OwnedSemaphorePermit,
    _memory: OwnedSemaphorePermit,
}
impl Pool {
    async fn prepare(
        runtime: Arc<Runtime>,
        component: Arc<Component>,
        manifest: Manifest,
    ) -> Result<Arc<Self>> {
        // NO WASI or other host imports. Unknown/privileged imports fail at this boundary.
        let linker = Linker::<StoreLimits>::new(&runtime.engine);
        let pre = w(linker.instantiate_pre(&component))?;
        let slots = manifest.warm_instances;
        let slot_reservation = runtime
            .slot_budget
            .clone()
            .try_acquire_many_owned(slots as u32)
            .context("insufficient reserved instance capacity for this generation")?;
        let memory_reservation = runtime
            .memory_budget_mib
            .clone()
            .try_acquire_many_owned((slots * manifest.memory_mib) as u32)
            .context("insufficient guest-memory budget for this generation")?;
        let pool = Arc::new(Self {
            runtime,
            _component: component,
            pre,
            manifest,
            ready: ArrayQueue::new(slots),
            refill_gate: Arc::new(Semaphore::new(slots.min(4))),
            counters: Counters::default(),
            degraded: AtomicBool::new(false),
            _slots: slot_reservation,
            _memory: memory_reservation,
        });
        for _ in 0..slots {
            let warm = pool.fresh().await?;
            pool.ready
                .push(warm)
                .map_err(|_| anyhow!("warm queue invariant violated"))?;
        }
        Ok(pool)
    }
    async fn fresh(&self) -> Result<Warm> {
        let limits = StoreLimitsBuilder::new()
            .memory_size(self.manifest.memory_mib * 1024 * 1024)
            .table_elements(4096)
            .instances(4)
            .memories(1)
            .tables(2)
            .trap_on_grow_failure(true)
            .build();
        let mut store = Store::new(&self.runtime.engine, limits);
        store.limiter(|limits| limits);
        store.set_hostcall_fuel(16 * 1024);
        w(store.set_fuel(self.manifest.fuel))?;
        w(store.fuel_async_yield_interval(Some(10_000)))?;
        store.set_epoch_deadline(self.manifest.init_timeout_ms.div_ceil(EPOCH_MS) + 1);
        store.epoch_deadline_trap();
        timeout(
            Duration::from_millis(self.manifest.init_timeout_ms),
            async {
                let instance = w(self.pre.instantiate_async(&mut store).await)?;
                let function = w(instance.get_typed_func::<Args, (u16,)>(&mut store, "authorize"))?;
                Ok::<_, anyhow::Error>(Warm { store, function })
            },
        )
        .await
        .context("plugin initialization timed out")?
    }
    pub fn stats(&self) -> PoolStats {
        let c = &self.counters;
        PoolStats {
            plugin: self.manifest.name.clone(),
            version: self.manifest.version.clone(),
            ready: self.ready.len(),
            target: self.manifest.warm_instances,
            warm_hits: c.hits.load(Ordering::Relaxed),
            warm_misses: c.misses.load(Ordering::Relaxed),
            invocation_errors: c.errors.load(Ordering::Relaxed),
            refills: c.refills.load(Ordering::Relaxed),
            refill_errors: c.refill_errors.load(Ordering::Relaxed),
            degraded: self.degraded.load(Ordering::Relaxed),
            code_tier: "warm",
            instance_policy: "single_use",
        }
    }
    async fn authorize(self: &Arc<Self>, ctx: &RequestContext) -> Decision {
        let Some(warm) = self.ready.pop() else {
            self.counters.misses.fetch_add(1, Ordering::Relaxed);
            return Decision::unavailable();
        };
        self.counters.hits.fetch_add(1, Ordering::Relaxed);
        let mut lease = Lease {
            warm: Some(warm),
            pool: Arc::downgrade(self),
        };
        let result = self
            .invoke(lease.warm.as_mut().expect("owned lease"), ctx)
            .await;
        match result {
            Ok(0) => Decision::Continue,
            Ok(status @ (400 | 401 | 403 | 404 | 409 | 413 | 429)) => {
                policy_decision(status, "plugin_denied")
            }
            _ => {
                self.counters.errors.fetch_add(1, Ordering::Relaxed);
                Decision::unavailable()
            }
        }
        // Lease Drop also runs on future cancellation/trap/timeout. No mutable guest is returned to the pool.
    }
    async fn invoke(&self, warm: &mut Warm, ctx: &RequestContext) -> Result<u16> {
        let m = &self.manifest;
        let select = |cap: &str, value: &str| {
            if m.permits(cap) {
                value.to_owned()
            } else {
                String::new()
            }
        };
        let identity = ctx.identity.as_ref();
        let args = (
            select("request.method", ctx.method.as_str()),
            select("request.path", &ctx.path),
            select(
                "identity.subject",
                identity.map(|i| i.subject.as_str()).unwrap_or(""),
            ),
            select(
                "identity.tenant",
                identity.map(|i| i.tenant.as_str()).unwrap_or(""),
            ),
            select(
                "operation.action",
                ctx.operation
                    .as_ref()
                    .map(|o| o.action.as_str())
                    .unwrap_or(""),
            ),
            select(
                "operation.target",
                ctx.operation
                    .as_ref()
                    .and_then(|o| o.target.as_deref())
                    .unwrap_or(""),
            ),
        );
        ensure!(
            args.0.len() + args.1.len() + args.2.len() + args.3.len() + args.4.len() + args.5.len()
                <= 8192,
            "metadata too large"
        );
        w(warm.store.set_fuel(m.fuel))?;
        warm.store
            .set_epoch_deadline(m.timeout_ms.div_ceil(EPOCH_MS) + 1);
        let deadline = ctx
            .deadline
            .min(Instant::now() + Duration::from_millis(m.timeout_ms));
        timeout_at(deadline, async {
            let (status,) = w(warm.function.call_async(&mut warm.store, args).await)?;
            // Wasmtime 49 performs canonical post-return inside call_async.
            Ok::<_, anyhow::Error>(status)
        })
        .await
        .context("plugin deadline exceeded")?
    }
    fn refill(pool: Weak<Self>) {
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            return;
        };
        handle.spawn(async move {
            let Some(pool) = pool.upgrade() else {
                return;
            };
            let Ok(_permit) = pool.refill_gate.clone().acquire_owned().await else {
                return;
            };
            // At most one refill task per consumed slot; admission never creates cold instances.
            for attempt in 0..3 {
                match pool.fresh().await {
                    Ok(warm) => {
                        if pool.ready.push(warm).is_ok() {
                            pool.counters.refills.fetch_add(1, Ordering::Relaxed);
                        } else {
                            pool.counters.refill_errors.fetch_add(1, Ordering::Relaxed);
                        }
                        return;
                    }
                    Err(_) => {
                        pool.counters.refill_errors.fetch_add(1, Ordering::Relaxed);
                        tokio::time::sleep(Duration::from_millis(10 * (1 << attempt))).await;
                    }
                }
            }
            // Degraded capacity is visible; a config reload creates a new generation.
            pool.degraded.store(true, Ordering::Relaxed);
            tracing::error!(plugin = %pool.manifest.name, "clean-instance replenishment failed");
        });
    }
}
struct Lease {
    warm: Option<Warm>,
    pool: Weak<Pool>,
}
impl Drop for Lease {
    fn drop(&mut self) {
        drop(self.warm.take());
        Pool::refill(self.pool.clone());
    }
}
pub struct WasmBlock {
    id: String,
    pool: Arc<Pool>,
}
impl WasmBlock {
    pub fn new(id: String, pool: Arc<Pool>) -> Self {
        Self { id, pool }
    }
}
impl Block<RequestContext> for WasmBlock {
    fn id(&self) -> &str {
        &self.id
    }
    fn phase(&self) -> Phase {
        Phase::Authorize
    }
    fn execute<'a>(&'a self, ctx: &'a mut RequestContext) -> Execution<'a> {
        Execution::Deferred(Box::pin(async move { self.pool.authorize(ctx).await }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::IpAddr;
    fn manifest() -> Manifest {
        Manifest {
            name: "test".into(),
            version: "1".into(),
            abi: "wayframe:gateway/policy@0.1.0".into(),
            component: "test.wasm".into(),
            sha256: "0".repeat(64),
            capabilities: vec!["request.path".into()],
            warm_instances: 2,
            memory_mib: 2,
            fuel: 100_000,
            timeout_ms: 50,
            init_timeout_ms: 500,
            signature: None,
        }
    }
    fn context() -> RequestContext {
        RequestContext {
            request_id: 1,
            method: "GET".parse().unwrap(),
            path: "/".into(),
            peer_ip: "127.0.0.1".parse::<IpAddr>().unwrap(),
            headers: Default::default(),
            identity: None,
            body: None,
            operation: None,
            extensions: Default::default(),
            deadline: Instant::now() + Duration::from_secs(3),
        }
    }
    #[test]
    fn permissions_are_closed_by_default() {
        let mut m = manifest();
        assert!(m.validate().is_ok());
        m.capabilities.push("network.all".into());
        assert!(m.validate().is_err());
    }
    #[tokio::test]
    async fn component_calls_are_clean_and_replenished() {
        let runtime = Runtime::new().unwrap();
        // This fixture returns 403 on the SECOND call to the SAME instance. Fresh calls must always continue.
        let component = Arc::new(
            w(Component::new(
                &runtime.engine,
                include_str!("../tests/stateful.component.wat"),
            ))
            .unwrap(),
        );
        let pool = Pool::prepare(runtime, component, manifest()).await.unwrap();
        for _ in 0..8 {
            assert_eq!(pool.authorize(&context()).await, Decision::Continue);
            timeout(Duration::from_secs(3), async {
                while pool.stats().ready < 2 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
        }
        assert_eq!(pool.stats().warm_hits, 8);
    }
    #[tokio::test]
    async fn empty_pool_rejects_without_cold_instantiation() {
        let runtime = Runtime::new().unwrap();
        let component = Arc::new(
            w(Component::new(
                &runtime.engine,
                include_str!("../tests/stateful.component.wat"),
            ))
            .unwrap(),
        );
        let pool = Pool::prepare(runtime, component, manifest()).await.unwrap();
        let a = pool.ready.pop();
        let b = pool.ready.pop();
        assert_eq!(pool.authorize(&context()).await, Decision::unavailable());
        assert_eq!(pool.stats().warm_misses, 1);
        drop(a);
        drop(b);
    }
}

#[cfg(test)]
mod failure_tests {
    use super::*;
    fn manifest() -> Manifest {
        Manifest {
            name: "fault-test".into(),
            version: "1".into(),
            abi: "wayframe:gateway/policy@0.1.0".into(),
            component: "test.wasm".into(),
            sha256: "0".repeat(64),
            capabilities: vec!["request.path".into()],
            warm_instances: 1,
            memory_mib: 2,
            fuel: 100_000,
            timeout_ms: 200,
            init_timeout_ms: 500,
            signature: None,
        }
    }
    fn context(path: &str) -> RequestContext {
        RequestContext {
            request_id: 1,
            method: "GET".parse().unwrap(),
            path: path.into(),
            peer_ip: "127.0.0.1".parse().unwrap(),
            headers: Default::default(),
            identity: None,
            body: None,
            operation: None,
            extensions: Default::default(),
            deadline: Instant::now() + Duration::from_secs(2),
        }
    }
    fn component(runtime: &Runtime, wat: &str) -> Arc<Component> {
        Arc::new(w(Component::new(&runtime.engine, wat)).unwrap())
    }
    fn fixed(code: &str) -> String {
        // Only the exported policy body changes; keep the identical canonical ABI.
        let wat = include_str!("../tests/stateful.component.wat");
        let start = wat.find("      global.get $called").unwrap();
        let end = wat[start..].find("end))").unwrap() + start + "end".len();
        format!("{}{}{}", &wat[..start], code, &wat[end..])
    }
    async fn wait_ready(pool: &Pool) {
        timeout(Duration::from_secs(3), async {
            while pool.stats().ready < pool.stats().target {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
    #[test]
    fn publisher_signature_binds_digest_limits_version_and_grants() {
        use ed25519_dalek::{Signer, SigningKey};
        use rand::Rng;
        let mut secret = [0u8; 32];
        rand::rng().fill_bytes(&mut secret);
        let signer = SigningKey::from_bytes(&secret);
        let mut m = manifest();
        m.signature = Some(ArtifactSignature {
            key_id: "publisher".into(),
            ed25519: hex::encode(signer.sign(&m.signing_bytes().unwrap()).to_bytes()),
        });
        let trust = Trust {
            keys: [("publisher".into(), signer.verifying_key())].into(),
            capabilities: m.capabilities.iter().cloned().collect(),
        };
        assert!(m.verify(&trust).is_ok());
        let mut tampered = m.clone();
        tampered.fuel += 1;
        assert!(tampered.verify(&trust).is_err());
        let mut tampered = m.clone();
        tampered.sha256 = "ff".repeat(32);
        assert!(tampered.verify(&trust).is_err());
        let mut missing = m.clone();
        missing.signature = None;
        assert!(missing.verify(&trust).is_err());
        let restricted = Trust {
            keys: trust.keys,
            capabilities: Default::default(),
        };
        assert!(m.verify(&restricted).is_err());
    }
    #[tokio::test]
    async fn invalid_decision_fails_closed_and_replenishes() {
        let rt = Runtime::new().unwrap();
        let c = component(&rt, &fixed("i32.const 200"));
        let pool = Pool::prepare(rt, c, manifest()).await.unwrap();
        assert_eq!(pool.authorize(&context("/")).await, Decision::unavailable());
        wait_ready(&pool).await;
        assert_eq!(pool.stats().invocation_errors, 1);
    }
    #[tokio::test]
    async fn infinite_guest_is_interrupted_and_slot_is_replaced() {
        let rt = Runtime::new().unwrap();
        let c = component(&rt, &fixed("(loop $forever (br $forever)) i32.const 0"));
        let pool = Pool::prepare(rt, c, manifest()).await.unwrap();
        let decision = timeout(Duration::from_secs(2), pool.authorize(&context("/")))
            .await
            .unwrap();
        assert_eq!(decision, Decision::unavailable());
        wait_ready(&pool).await;
    }
    #[tokio::test]
    async fn initial_memory_over_grant_is_rejected() {
        let rt = Runtime::new().unwrap();
        let wat = include_str!("../tests/stateful.component.wat").replace(
            "(memory (export \"memory\") 1)",
            "(memory (export \"memory\") 64)",
        );
        let c = component(&rt, &wat);
        assert!(Pool::prepare(rt, c, manifest()).await.is_err());
    }
    #[tokio::test]
    async fn unknown_host_imports_are_rejected() {
        let rt = Runtime::new().unwrap();
        let wat = r#"(component (import "host-secret" (func (result string))))"#;
        let c = component(&rt, wat);
        assert!(Pool::prepare(rt, c, manifest()).await.is_err());
    }
    #[tokio::test]
    async fn permissions_project_only_operator_granted_metadata() {
        let rt = Runtime::new().unwrap();
        let c = component(
            &rt,
            include_str!("../../../plugins/path-control/path-control.component.wat"),
        );
        let pool = Pool::prepare(rt.clone(), c.clone(), manifest())
            .await
            .unwrap();
        assert!(matches!(
            pool.authorize(&context("/admin")).await,
            Decision::Deny {
                reason: Reason::Forbidden,
                ..
            }
        ));
        let mut m = manifest();
        m.capabilities.clear();
        let restricted = Pool::prepare(rt, c, m).await.unwrap();
        assert_eq!(
            restricted.authorize(&context("/admin")).await,
            Decision::Continue
        );
    }
    #[tokio::test]
    async fn excess_generation_reservation_does_not_consume_active_budget() {
        let rt = Runtime::new().unwrap();
        let c = component(&rt, include_str!("../tests/stateful.component.wat"));
        let mut m = manifest();
        m.warm_instances = 64;
        m.memory_mib = 64;
        assert!(Pool::prepare(rt.clone(), c.clone(), m).await.is_err());
        assert_eq!(rt.slot_budget.available_permits(), 256);
        assert_eq!(rt.memory_budget_mib.available_permits(), 512);
        let good = Pool::prepare(rt, c, manifest()).await.unwrap();
        assert_eq!(good.authorize(&context("/")).await, Decision::Continue);
    }
}
