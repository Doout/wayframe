//! A single-process, durable control plane. Persist before publishing an immutable snapshot.
use anyhow::{ensure, Result};
use arc_swap::ArcSwap;
use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    fs::{File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Instant,
};
use subtle::ConstantTimeEq;
use wayframe::{
    http::{Identity, RequestContext},
    mcp::{parse_unique, ToolPermissions},
    prelude::*,
};

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct User {
    profile: UserInput,
    token_hash: String,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UserInput {
    pub id: String,
    pub subject: String,
    pub tenant: String,
    pub permissions: BTreeSet<String>,
    #[serde(default = "enabled")]
    pub enabled: bool,
}
fn enabled() -> bool {
    true
}
#[derive(Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RateRule {
    pub id: String,
    pub user: Option<String>,
    pub tool: Option<String>,
    pub requests_per_second: f64,
    pub burst: u32,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Rules {
    pub tools: HashMap<String, String>,
    #[serde(default)]
    pub denied_tools: BTreeSet<String>,
    pub rate_limits: Vec<RateRule>,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Snapshot {
    schema: u32,
    revision: u64,
    users: BTreeMap<String, User>,
    rules: Rules,
    #[serde(skip)]
    permissions: Option<Arc<ToolPermissions>>,
}
#[derive(Clone)]
struct Authenticated {
    user_id: String,
    credential_hash: String,
}
struct Bucket {
    tokens: f64,
    at: Instant,
    rate: f64,
    burst: u32,
}
pub struct Store {
    current: ArcSwap<Snapshot>,
    writer: Arc<tokio::sync::Mutex<()>>,
    buckets: Mutex<HashMap<(String, String), Bucket>>,
    path: Option<PathBuf>,
    admin_hash: Option<String>,
    forbidden_hashes: BTreeSet<String>,
    _lock: Option<File>,
}
fn hash(token: &str) -> String {
    hex::encode(Sha256::digest(token.as_bytes()))
}
fn identifier(value: &str) -> bool {
    !value.is_empty()
        && !matches!(value, "." | "..")
        && value.len() <= 128
        && value
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"._:-".contains(&c))
}
fn label(value: &str) -> bool {
    !value.is_empty() && value.len() <= 256 && !value.chars().any(char::is_control)
}
impl Snapshot {
    fn prepare(&mut self) -> Result<()> {
        self.validate()?;
        self.permissions = Some(Arc::new(
            ToolPermissions::new("current".into(), self.rules.tools.clone())
                .map_err(anyhow::Error::msg)?,
        ));
        Ok(())
    }
    fn validate(&self) -> Result<()> {
        ensure!(
            self.schema == 1 && self.revision > 0,
            "unsupported state schema/revision"
        );
        ensure!(self.users.len() <= 1024, "user capacity reached");
        let mut owners = BTreeSet::new();
        let mut credentials = BTreeSet::new();
        for (id, user) in &self.users {
            let p = &user.profile;
            ensure!(
                id == &p.id && identifier(id) && label(&p.subject) && label(&p.tenant),
                "invalid user identity"
            );
            ensure!(owners.insert((&p.tenant, &p.subject)), "duplicate identity");
            ensure!(
                p.permissions.len() <= 128 && p.permissions.iter().all(|p| label(p)),
                "invalid permissions"
            );
            ensure!(
                user.token_hash.len() == 64
                    && user.token_hash.bytes().all(|b| b.is_ascii_hexdigit())
                    && credentials.insert(&user.token_hash),
                "invalid credential digest"
            );
        }
        ToolPermissions::new("validate".into(), self.rules.tools.clone())
            .map_err(anyhow::Error::msg)?;
        ensure!(
            self.rules.denied_tools.len() <= 1024
                && self
                    .rules
                    .denied_tools
                    .iter()
                    .all(|t| self.rules.tools.contains_key(t)),
            "invalid denied tools"
        );
        ensure!(self.rules.rate_limits.len() <= 128, "too many rate rules");
        let mut ids = BTreeSet::new();
        for r in &self.rules.rate_limits {
            ensure!(
                identifier(&r.id) && ids.insert(&r.id),
                "invalid/duplicate rule id"
            );
            ensure!(
                r.user.as_ref().is_none_or(|u| self.users.contains_key(u)),
                "unknown rule user"
            );
            ensure!(
                r.tool
                    .as_ref()
                    .is_none_or(|t| self.rules.tools.contains_key(t)),
                "unknown rule tool"
            );
            ensure!(
                r.requests_per_second.is_finite()
                    && r.requests_per_second > 0.0
                    && r.requests_per_second <= 1_000_000.0
                    && r.burst > 0
                    && r.burst <= 1_000_000,
                "invalid rate budget"
            );
        }
        Ok(())
    }
    fn view(&self) -> serde_json::Value {
        serde_json::json!({"schema": self.schema, "revision": self.revision, "users": self.users.values().map(|u| &u.profile).collect::<Vec<_>>(), "rules": self.rules})
    }
}
fn private_open(path: &Path) -> Result<File> {
    if let Ok(meta) = path.symlink_metadata() {
        ensure!(
            meta.is_file() && !meta.file_type().is_symlink(),
            "state path must be a regular file"
        );
    }
    let mut options = OpenOptions::new();
    options.write(true).read(true).create(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let file = options.open(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        ensure!(
            file.metadata()?.permissions().mode() & 0o077 == 0,
            "state files must be private (0600)"
        );
    }
    Ok(file)
}
fn persist(path: &Path, snapshot: &Snapshot) -> Result<()> {
    let temp = path.with_extension(format!("tmp-{}", std::process::id()));
    let bytes = serde_json::to_vec(snapshot)?;
    ensure!(bytes.len() <= 4 * 1024 * 1024, "state capacity exceeded");
    let result = (|| -> Result<()> {
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&temp)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        std::fs::rename(&temp, path)?;
        // Publication after durable file replacement. Directory sync is best effort on Windows.
        #[cfg(unix)]
        File::open(path.parent().expect("absolute state path"))?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temp);
    }
    result
}
impl Store {
    pub fn open(
        config: &crate::config::Config,
        clients: Vec<(String, Identity)>,
        upstream: &[String],
    ) -> Result<Arc<Self>> {
        let path = std::env::var_os("WAYFRAME_STATE_FILE").map(PathBuf::from);
        let admin = std::env::var_os("WAYFRAME_ADMIN_TOKEN")
            .map(|_| crate::config::secret("WAYFRAME_ADMIN_TOKEN"))
            .transpose()?;
        ensure!(
            path.is_some() == admin.is_some(),
            "set WAYFRAME_STATE_FILE and WAYFRAME_ADMIN_TOKEN together"
        );
        let admin_hash = admin.as_deref().map(hash);
        let mut forbidden_hashes = BTreeSet::new();
        for token in upstream {
            forbidden_hashes.insert(hash(token));
        }
        if let Some(digest) = &admin_hash {
            ensure!(
                !forbidden_hashes.contains(digest),
                "admin and upstream credentials must differ"
            );
            forbidden_hashes.insert(digest.clone());
        }
        let mut snapshot = Snapshot {
            schema: 1,
            revision: 1,
            users: BTreeMap::new(),
            permissions: None,
            rules: Rules {
                tools: config.tools.clone(),
                denied_tools: BTreeSet::new(),
                rate_limits: vec![RateRule {
                    id: "default".into(),
                    user: None,
                    tool: None,
                    requests_per_second: config.requests_per_second,
                    burst: config.burst,
                }],
            },
        };
        for (index, (token, identity)) in clients.into_iter().enumerate() {
            let id = format!("bootstrap-{}", index + 1);
            let digest = hash(&token);
            ensure!(
                !forbidden_hashes.contains(&digest),
                "admin/upstream credential cannot be a client credential"
            );
            snapshot.users.insert(
                id.clone(),
                User {
                    profile: UserInput {
                        id,
                        subject: identity.subject,
                        tenant: identity.tenant,
                        permissions: identity.permissions,
                        enabled: true,
                    },
                    token_hash: digest,
                },
            );
        }
        let mut lock = None;
        if let Some(path) = &path {
            ensure!(path.is_absolute(), "state path must be absolute");
            std::fs::create_dir_all(path.parent().expect("absolute"))?;
            let file = private_open(&path.with_extension("lock"))?;
            fs2::FileExt::try_lock_exclusive(&file)
                .map_err(|_| anyhow::anyhow!("another gateway owns this state file"))?;
            lock = Some(file);
            if path.exists() {
                let file = private_open(path)?;
                ensure!(
                    file.metadata()?.len() <= 4 * 1024 * 1024,
                    "state file too large"
                );
                let bytes = std::fs::read(path)?;
                snapshot = serde_json::from_slice(&bytes)?;
            } else {
                snapshot.validate()?;
                persist(path, &snapshot)?;
            }
        }
        snapshot.prepare()?;
        ensure!(
            snapshot
                .users
                .values()
                .all(|u| !forbidden_hashes.contains(&u.token_hash)),
            "admin/upstream credential collides with a user"
        );
        Ok(Arc::new(Self {
            current: ArcSwap::from_pointee(snapshot),
            writer: Arc::new(tokio::sync::Mutex::new(())),
            buckets: Mutex::new(HashMap::new()),
            path,
            admin_hash,
            forbidden_hashes,
            _lock: lock,
        }))
    }
    pub fn managed(&self) -> bool {
        self.path.is_some()
    }
    fn user<'a>(snapshot: &'a Snapshot, ctx: &RequestContext) -> Option<&'a User> {
        let authenticated = ctx.extensions.get::<Authenticated>()?;
        let user = snapshot.users.get(&authenticated.user_id)?;
        (user.profile.enabled
            && bool::from(
                user.token_hash
                    .as_bytes()
                    .ct_eq(authenticated.credential_hash.as_bytes()),
            ))
        .then_some(user)
    }
    pub fn authenticate(&self, ctx: &mut RequestContext) -> Decision {
        ctx.identity = None;
        ctx.extensions.remove::<Authenticated>();
        let mut headers = ctx.headers.get_all("authorization").iter();
        let token = headers
            .next()
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "));
        let Some(token) = token.filter(|t| t.len() >= 32 && t.len() <= 8192) else {
            return Decision::deny(Reason::Unauthenticated, "unauthorized");
        };
        if headers.next().is_some() {
            return Decision::deny(Reason::Unauthenticated, "unauthorized");
        }
        let digest = hash(token);
        let state = self.current.load();
        if let Some(user) = state.users.values().find(|u| {
            bool::from(u.token_hash.as_bytes().ct_eq(digest.as_bytes())) && u.profile.enabled
        }) {
            ctx.extensions.insert(Authenticated {
                user_id: user.profile.id.clone(),
                credential_hash: digest,
            });
            ctx.identity = Some(
                Identity::new(&user.profile.subject, &user.profile.tenant)
                    .with_permissions(user.profile.permissions.iter().cloned()),
            );
            Decision::Continue
        } else {
            Decision::deny(Reason::Unauthenticated, "unauthorized")
        }
    }
    fn limit(&self, ctx: &RequestContext, scoped: bool) -> Decision {
        let state = self.current.load();
        let Some(user) = Self::user(&state, ctx) else {
            return Decision::deny(Reason::Unauthenticated, "unauthorized");
        };
        let target = ctx
            .operation
            .as_ref()
            .filter(|o| o.action == "tools/call")
            .and_then(|o| o.target.as_deref());
        let rules: Vec<_> = state
            .rules
            .rate_limits
            .iter()
            .filter(|r| {
                r.tool.is_some() == scoped
                    && r.user.as_ref().is_none_or(|u| u == &user.profile.id)
                    && (!scoped || r.tool.as_deref() == target)
            })
            .collect();
        let Ok(mut buckets) = self.buckets.lock() else {
            return Decision::unavailable();
        };
        let now = Instant::now();
        for r in &rules {
            let b = buckets
                .entry((r.id.clone(), user.profile.id.clone()))
                .or_insert(Bucket {
                    tokens: r.burst as f64,
                    at: now,
                    rate: r.requests_per_second,
                    burst: r.burst,
                });
            if b.rate != r.requests_per_second || b.burst != r.burst {
                b.tokens = b.tokens.min(r.burst as f64);
                b.rate = r.requests_per_second;
                b.burst = r.burst;
            }
            b.tokens =
                (b.tokens + now.duration_since(b.at).as_secs_f64() * b.rate).min(b.burst as f64);
            b.at = now;
        }
        if rules
            .iter()
            .any(|r| buckets[&(r.id.clone(), user.profile.id.clone())].tokens < 1.0)
        {
            return Decision::deny(Reason::RateLimited, "rate_limited");
        }
        for r in rules {
            buckets
                .get_mut(&(r.id.clone(), user.profile.id.clone()))
                .expect("inserted")
                .tokens -= 1.0;
        }
        Decision::Continue
    }
    fn authorize(&self, ctx: &mut RequestContext) -> Decision {
        let state = self.current.load();
        let Some(user) = Self::user(&state, ctx) else {
            return Decision::deny(Reason::Unauthenticated, "unauthorized");
        };
        // Re-read current grants so revocation during body collection takes effect.
        ctx.identity = Some(
            Identity::new(&user.profile.subject, &user.profile.tenant)
                .with_permissions(user.profile.permissions.iter().cloned()),
        );
        if ctx
            .operation
            .as_ref()
            .filter(|o| o.action == "tools/call")
            .and_then(|o| o.target.as_ref())
            .is_some_and(|t| state.rules.denied_tools.contains(t))
        {
            return Decision::deny(Reason::Forbidden, "tool_disabled");
        }
        let decision = match state
            .permissions
            .as_ref()
            .expect("prepared rules")
            .execute(ctx)
        {
            Execution::Immediate(d) => d,
            Execution::Deferred(_) => unreachable!("native policy"),
        };
        decision
    }
    pub async fn admin(self: &Arc<Self>, request: Request<Body>) -> Response {
        let Some(admin_hash) = &self.admin_hash else {
            return StatusCode::NOT_FOUND.into_response();
        };
        // Browser origins are unnecessary for this API/CLI and are rejected before mutation.
        if request.headers().contains_key("origin") {
            return StatusCode::FORBIDDEN.into_response();
        }
        let mut auth = request.headers().get_all("authorization").iter();
        let valid = auth
            .next()
            .and_then(|h| h.to_str().ok())
            .and_then(|h| h.strip_prefix("Bearer "))
            .filter(|t| t.len() <= 8192)
            .is_some_and(|t| bool::from(hash(t).as_bytes().ct_eq(admin_hash.as_bytes())));
        if !valid || auth.next().is_some() {
            return StatusCode::UNAUTHORIZED.into_response();
        }
        let method = request.method().as_str().to_owned();
        let path = request.uri().path().to_owned();
        if method == "GET" && path == "/admin/state" {
            return self.view();
        }
        let revision = request
            .headers()
            .get("if-match")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.strip_prefix('"'))
            .and_then(|s| s.strip_suffix('"'))
            .and_then(|s| s.parse::<u64>().ok());
        let Some(revision) = revision else {
            return (
                StatusCode::PRECONDITION_REQUIRED,
                "If-Match revision required",
            )
                .into_response();
        };
        let bytes = match tokio::time::timeout(
            std::time::Duration::from_secs(5),
            to_bytes(request.into_body(), 64 * 1024),
        )
        .await
        {
            Ok(Ok(bytes)) => bytes,
            _ => return StatusCode::PAYLOAD_TOO_LARGE.into_response(),
        };
        let writer = self.writer.clone().lock_owned().await;
        let mut next = (**self.current.load()).clone();
        if next.revision != revision {
            return StatusCode::PRECONDITION_FAILED.into_response();
        }
        let payload = if bytes.is_empty() {
            serde_json::Value::Null
        } else {
            match parse_unique(&bytes) {
                Ok(p) => p,
                Err(_) => return StatusCode::BAD_REQUEST.into_response(),
            }
        };
        let mut issued = None;
        let result = (|| -> Result<()> {
            match (method.as_str(), path.as_str()) {
                ("POST", "/admin/users") => {
                    let input: UserInput = serde_json::from_value(payload)?;
                    ensure!(!next.users.contains_key(&input.id), "user already exists");
                    let token = new_token();
                    next.users.insert(
                        input.id.clone(),
                        User {
                            profile: input,
                            token_hash: hash(&token),
                        },
                    );
                    issued = Some(token);
                }
                ("PUT", "/admin/rules") => {
                    next.rules = serde_json::from_value(payload)?;
                }
                _ => {
                    let tail = path
                        .strip_prefix("/admin/users/")
                        .ok_or_else(|| anyhow::anyhow!("unknown route"))?;
                    let (id, rotating) = tail
                        .strip_suffix("/token")
                        .map_or((tail, false), |id| (id, true));
                    let user = next
                        .users
                        .get_mut(id)
                        .ok_or_else(|| anyhow::anyhow!("unknown user"))?;
                    match (method.as_str(), rotating) {
                        ("PUT", false) => {
                            let p: UserInput = serde_json::from_value(payload)?;
                            ensure!(
                                p.id == id
                                    && p.tenant == user.profile.tenant
                                    && p.subject == user.profile.subject,
                                "identity is immutable; create a new user"
                            );
                            user.profile = p;
                        }
                        ("POST", true) => {
                            ensure!(payload.is_null(), "rotation has no payload");
                            let token = new_token();
                            user.token_hash = hash(&token);
                            issued = Some(token);
                        }
                        ("DELETE", false) => {
                            ensure!(payload.is_null(), "deletion has no payload");
                            next.users.remove(id);
                            next.rules
                                .rate_limits
                                .retain(|r| r.user.as_deref() != Some(id));
                        }
                        _ => anyhow::bail!("unknown route"),
                    }
                }
            }
            next.revision = next
                .revision
                .checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("revision exhausted"))?;
            next.prepare()?;
            ensure!(
                next.users
                    .values()
                    .all(|u| !self.forbidden_hashes.contains(&u.token_hash)),
                "credential collision"
            );
            Ok(())
        })();
        if result.is_err() {
            return (StatusCode::BAD_REQUEST, "invalid management change").into_response();
        }
        let path = self.path.clone().expect("managed state");
        let prepared = next.clone();
        let persist_span = tracing::info_span!("admin.persist", revision = prepared.revision);
        let persist = tokio::task::spawn_blocking(move || {
            persist_span.in_scope(|| persist(&path, &prepared))
        });
        // Complete the transaction even if the client disconnects while persistence runs.
        let store = self.clone();
        let transaction = tokio::spawn(async move {
            let _writer = writer;
            match persist.await {
                Ok(Ok(())) => {
                    if let Ok(mut buckets) = store.buckets.lock() {
                        buckets.retain(|(rule, user), _| {
                            next.users.contains_key(user)
                                && next.rules.rate_limits.iter().any(|r| &r.id == rule)
                        });
                    }
                    store.current.store(Arc::new(next));
                    Ok(())
                }
                _ => Err(()),
            }
        });
        if !matches!(transaction.await, Ok(Ok(()))) {
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
        tracing::info!(action = %method, revision = self.current.load().revision, "management change committed");
        let mut value = self.current.load().view();
        if let Some(token) = issued {
            value["issued_token"] = token.into();
        }
        private_json(value)
    }
    fn view(&self) -> Response {
        private_json(self.current.load().view())
    }
}
fn new_token() -> String {
    let mut bytes = [0u8; 32];
    rand::rng().fill_bytes(&mut bytes);
    hex::encode(bytes)
}
fn private_json(value: serde_json::Value) -> Response {
    let revision = value["revision"].as_u64().unwrap_or(0);
    let mut response = Json(value).into_response();
    response
        .headers_mut()
        .insert("cache-control", "no-store".parse().expect("literal"));
    response.headers_mut().insert(
        "etag",
        format!("\"{}\"", revision).parse().expect("decimal"),
    );
    response
}
#[derive(Clone, Copy)]
pub enum Control {
    Authenticate,
    Rate,
    Authorize,
    ToolRate,
}
pub struct ManagedBlock {
    pub store: Arc<Store>,
    pub control: Control,
}
impl Block<RequestContext> for ManagedBlock {
    fn id(&self) -> &str {
        match self.control {
            Control::Authenticate => "auth",
            Control::Rate => "rate",
            Control::Authorize => "tool-permissions",
            Control::ToolRate => "tool-rate",
        }
    }
    fn phase(&self) -> Phase {
        match self.control {
            Control::Authenticate => Phase::Authenticate,
            Control::Authorize => Phase::Authorize,
            _ => Phase::Limit,
        }
    }
    fn execute<'a>(&'a self, ctx: &'a mut RequestContext) -> Execution<'a> {
        Execution::Immediate(match self.control {
            Control::Authenticate => self.store.authenticate(ctx),
            Control::Rate => self.store.limit(ctx, false),
            Control::Authorize => self.store.authorize(ctx),
            Control::ToolRate => self.store.limit(ctx, true),
        })
    }
}
