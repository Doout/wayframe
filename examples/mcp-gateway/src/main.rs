//! Configurable reference gateway, embedded in an application rather than installed by the framework.
mod config;
mod deployment;
use deployment::{prepare_deployment, Deployment};
mod management;
mod network;
mod plugin;
mod process;
mod session;
mod stdio;
mod telemetry;
use anyhow::{ensure, Result};
use axum::{
    body::Body,
    extract::{ConnectInfo, State},
    http::{Request, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use std::{net::SocketAddr, sync::Arc};
use wayframe::{
    http::{rejection, Identity, Operation, RequestContext},
    prelude::*,
};

struct UpstreamPath(String);
impl Block<RequestContext> for UpstreamPath {
    fn id(&self) -> &str {
        "upstream-path"
    }
    fn phase(&self) -> Phase {
        Phase::Dispatch
    }
    fn execute<'a>(&'a self, ctx: &'a mut RequestContext) -> Execution<'a> {
        ctx.path.clone_from(&self.0);
        Execution::Immediate(Decision::Continue)
    }
}
#[derive(Clone)]
struct TenantLease(#[allow(dead_code)] Arc<tokio::sync::OwnedSemaphorePermit>);
pub(crate) struct TenantQuota(wayframe::resilience::Quota);
impl Block<RequestContext> for TenantQuota {
    fn id(&self) -> &str {
        "tenant-concurrency"
    }
    fn phase(&self) -> Phase {
        Phase::Limit
    }
    fn execute<'a>(&'a self, ctx: &'a mut RequestContext) -> Execution<'a> {
        let Some(identity) = &ctx.identity else {
            return Execution::Immediate(Decision::deny(Reason::Unauthenticated, "unauthorized"));
        };
        Execution::Immediate(match self.0.try_admit(&identity.tenant) {
            Ok(permit) => {
                ctx.extensions.insert(TenantLease(Arc::new(permit)));
                Decision::Continue
            }
            Err(_) => Decision::deny(Reason::RateLimited, "tenant_capacity"),
        })
    }
}
#[derive(Clone)]
struct App {
    live: Arc<wayframe::Live<Deployment>>,
    publish_lock: Arc<tokio::sync::Mutex<()>>,
    store: Arc<management::Store>,
    admin_hosts: Option<Arc<Vec<String>>>,
    sessions: Arc<session::Sessions>,
    config_path: Option<std::path::PathBuf>,
    listener_settings: (SocketAddr, usize, u64),
    bootstrap_fingerprint: String,
    tls: Option<wayframe::listeners::Tls>,
    #[cfg(feature = "wasm")]
    factory: Arc<plugin::Factory>,
}

impl App {
    async fn forward(&self, peer: SocketAddr, request: Request<Body>) -> Response {
        let old_session = request
            .headers()
            .get("mcp-session-id")
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned);
        let deleting = request.method() == "DELETE";
        let deployment = self.live.snapshot();
        let mut response = deployment.value.gateway.handle(peer, request).await;
        if response.status() == StatusCode::UNAUTHORIZED {
            if let Some(resource) = &deployment.value.protected_resource {
                if let Ok(challenge) = resource.challenge(None) {
                    if let Ok(value) = challenge.parse() {
                        response.headers_mut().insert("www-authenticate", value);
                    }
                }
            }
        }
        if response.status() == StatusCode::METHOD_NOT_ALLOWED
            && !response.headers().contains_key("allow")
        {
            response
                .headers_mut()
                .insert("allow", "POST".parse().expect("literal"));
        }
        if let Some(new_session) = response.headers().get("mcp-session-id") {
            let Some(identity) = response.extensions().get::<Identity>() else {
                return rejection(Reason::Unavailable, "session_registration_failed");
            };
            let initialize = response
                .extensions()
                .get::<Operation>()
                .is_some_and(|op| op.action == "initialize");
            let valid = new_session.to_str().ok().is_some_and(|id| {
                if let Some(old) = &old_session {
                    id == old
                } else {
                    response.status().is_success()
                        && initialize
                        && self.sessions.bind_generation(
                            id,
                            identity,
                            response
                                .extensions()
                                .get::<wayframe::http::BackendId>()
                                .map(|b| b.0.clone()),
                            deployment.id,
                        )
                }
            });
            if !valid {
                return rejection(Reason::Unavailable, "session_registration_failed");
            }
        }
        if let Some(id) = old_session {
            if response.status() == StatusCode::NOT_FOUND
                || (deleting && response.status().is_success())
            {
                self.sessions.remove(&id);
            }
        }
        response.headers_mut().insert(
            "x-wayframe-generation",
            deployment.id.to_string().parse().expect("generation"),
        );
        wayframe::http::hold_response(response, deployment)
    }
}
async fn cors_preflight(State(app): State<App>, headers: axum::http::HeaderMap) -> Response {
    let deployment = app.live.snapshot();
    let Some(origin) = headers.get("origin").and_then(|h| h.to_str().ok()) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    if !headers
        .get("host")
        .and_then(|h| h.to_str().ok())
        .is_some_and(|h| {
            deployment
                .value
                .hosts
                .iter()
                .any(|v| v.eq_ignore_ascii_case(h))
        })
        || headers.get_all("host").iter().count() != 1
        || headers.get_all("origin").iter().count() != 1
        || !deployment.value.origins.iter().any(|o| o == origin)
    {
        return StatusCode::FORBIDDEN.into_response();
    }
    let Some(allowed) = deployment.value.gateway.cors_preflight(origin) else {
        return StatusCode::METHOD_NOT_ALLOWED.into_response();
    };
    let mut response = StatusCode::NO_CONTENT.into_response();
    *response.headers_mut() = allowed;
    response
}
async fn handle(
    State(app): State<App>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    request: Request<Body>,
) -> Response {
    app.forward(peer, request).await
}
async fn metrics(State(app): State<App>, request: Request<Body>) -> Response {
    let deployment = app.live.snapshot();
    let mut ctx = RequestContext::new(
        request.method().clone(),
        "/metrics".into(),
        "127.0.0.1".parse().expect("literal"),
        tokio::time::Instant::now() + std::time::Duration::from_secs(1),
    );
    ctx.headers = request.headers().clone();
    let decision = if let Some(auth) = &deployment.value.oidc {
        auth.authenticate(&mut ctx).await
    } else {
        app.store.authenticate(&mut ctx)
    };
    if let Decision::Deny { reason, code } = decision {
        return rejection(reason, code);
    }
    if !ctx
        .identity
        .is_some_and(|i| i.permissions.contains("gateway:metrics"))
    {
        return rejection(Reason::Forbidden, "metrics_not_permitted");
    }
    use axum::response::IntoResponse;
    let mut snapshot = deployment.value.gateway.metrics().snapshot();
    if let Some(pool) = &deployment.value.pool {
        let Ok(backends) = pool.snapshot() else {
            return StatusCode::SERVICE_UNAVAILABLE.into_response();
        };
        snapshot["load_balancer"] = serde_json::json!({"backends":backends});
    }
    snapshot["cache"] = serde_json::json!(deployment.value.gateway.cache_stats());
    snapshot["plugin_instances"] = serde_json::json!(deployment.value.plugins.ids());
    snapshot["groups"] = serde_json::json!(deployment
        .value
        .groups
        .iter()
        .map(|(name, pool)| (name, pool.snapshot().ok()))
        .collect::<std::collections::HashMap<_, _>>());
    snapshot["plugins"] = serde_json::json!(deployment.value.plugins.status());
    snapshot["generation"] = serde_json::json!(deployment.id);
    if let Some(adaptive) = &deployment.value.adaptive {
        let statistics = adaptive.statistics();
        snapshot["adaptive"] = serde_json::json!({"linear_selections":statistics.linear,"graph_selections":statistics.graph});
        if let Some(profile) = adaptive.profile_statistics() {
            snapshot["adaptive"]["profile"] = serde_json::json!({"classes":profile.classes,"capacity":profile.capacity,
                "warmups":profile.warmups,"explorations":profile.explorations,"measured_selections":profile.measured,
                "budget_skips":profile.budget_skips,"evictions":profile.evictions,"stale_observations":profile.stale_observations,
                "successes":profile.successes,"failures":profile.failures});
        }
    }
    Json(snapshot).into_response()
}
#[tokio::main]
async fn main() -> Result<()> {
    let config = config::Config::load()?;
    let arguments: Vec<_> = std::env::args().skip(1).collect();
    if !arguments.is_empty() {
        ensure!(
            arguments.len() == 1 && matches!(arguments[0].as_str(), "--validate" | "--inspect"),
            "use --validate or --inspect"
        );
        println!(
            "{}",
            serde_json::json!({"valid":true,"listen":config.listen.to_string(),"routes":config.routes,"plugins":config.plugins.iter().map(|p|serde_json::json!({"id":p.id,"kind":p.kind,"version":p.version})).collect::<Vec<_>>(),"validation":"configuration bounds and routes; service and plugin preparation occurs at startup"})
        );
        return Ok(());
    }
    let _telemetry = telemetry::init()?;
    let transport = std::env::var("WAYFRAME_TRANSPORT").unwrap_or_else(|_| "http".into());
    ensure!(
        matches!(transport.as_str(), "http" | "stdio" | "tcp"),
        "WAYFRAME_TRANSPORT must be http, stdio or tcp"
    );
    ensure!(
        transport != "stdio" || std::env::var_os("WAYFRAME_UNIX_SOCKET").is_none(),
        "select either stdio or a Unix socket"
    );
    let config::Credentials {
        clients,
        upstream_token,
        backend_tokens,
    } = config.credentials()?;
    let mut forbidden_tokens: Vec<_> = backend_tokens.values().cloned().collect();
    forbidden_tokens.extend(upstream_token.iter().cloned());
    let managed_config = management::Config {
        tools: config.tools.clone(),
        requests_per_second: config.requests_per_second,
        burst: config.burst,
        state_file: std::env::var_os("WAYFRAME_STATE_FILE").map(std::path::PathBuf::from),
        admin_token: std::env::var_os("WAYFRAME_ADMIN_TOKEN")
            .map(|_| config::secret("WAYFRAME_ADMIN_TOKEN"))
            .transpose()?,
    };
    let store = management::Store::open(&managed_config, clients, &forbidden_tokens)?;
    store.configure_admins(
        config
            .admins
            .iter()
            .map(|a| Ok((a.id.clone(), config::secret(&a.token_env)?, a.role)))
            .collect::<Result<Vec<_>>>()?,
    )?;
    if let Some(path) = &managed_config.state_file {
        let audit_path = std::env::var_os("WAYFRAME_AUDIT_FILE")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|| path.with_extension("audit.jsonl"));
        store.attach_audit(Arc::new(wayframe::observability::Audit::open(
            &audit_path,
            64 * 1024 * 1024,
        )?))?;
    }
    let sessions = Arc::new(session::Sessions::new());
    #[cfg(feature = "wasm")]
    let factory = Arc::new(plugin::Factory::new());
    let prepared = prepare_deployment(
        &config,
        store.clone(),
        sessions.clone(),
        1,
        #[cfg(feature = "wasm")]
        factory.clone(),
    )
    .await?;

    let app = App {
        live: Arc::new(wayframe::Live::new(prepared)),
        publish_lock: Arc::new(tokio::sync::Mutex::new(())),
        store,
        admin_hosts: None,
        sessions,
        config_path: std::env::var_os("WAYFRAME_CONFIG").map(std::path::PathBuf::from),
        listener_settings: (config.listen, config.max_in_flight, config.timeout_seconds),
        bootstrap_fingerprint: config.bootstrap_fingerprint()?,
        tls: tls_from_env()?,
        #[cfg(feature = "wasm")]
        factory,
    };
    let admin_task = if let Ok(address) = std::env::var("WAYFRAME_ADMIN_LISTEN") {
        ensure!(app.store.managed(), "admin listener requires managed state");
        let address: SocketAddr = address.parse()?;
        ensure!(
            address.ip().is_loopback(),
            "admin listener must bind to loopback"
        );
        let listener = tokio::net::TcpListener::bind(address).await?;
        let mut admin_app = app.clone();
        admin_app.admin_hosts = Some(Arc::new(vec![address.to_string()]));
        Some(tokio::spawn(async move {
            wayframe::listeners::serve_tcp(
                listener,
                admin_router().with_state(admin_app),
                wayframe::listeners::Limits::default(),
                None,
                std::future::pending(),
            )
            .await
        }))
    } else {
        None
    };
    struct AdminTask(Option<tokio::task::JoinHandle<anyhow::Result<()>>>);
    impl Drop for AdminTask {
        fn drop(&mut self) {
            if let Some(task) = &self.0 {
                task.abort();
            }
        }
    }
    let _admin_task = AdminTask(admin_task);
    let shutdown_live = app.live.clone();
    let listener_tls = app.tls.clone();
    if transport == "tcp" {
        let result = network::tcp(app, config.listen).await;
        drop(_admin_task);
        shutdown_live.snapshot().value.plugins.shutdown().await?;
        return result;
    }
    if transport == "stdio" {
        let token = config::secret("WAYFRAME_LOCAL_TOKEN")?;
        let result = stdio::run(
            app,
            token,
            config.max_request_bytes,
            config.max_response_bytes as usize,
            &config.allowed_hosts[0],
        )
        .await;
        drop(_admin_task);
        shutdown_live.snapshot().value.plugins.shutdown().await?;
        return result;
    }
    if let Ok(path) = std::env::var("WAYFRAME_UNIX_SOCKET") {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let path = std::path::PathBuf::from(path);
            ensure!(
                path.is_absolute() && !path.exists(),
                "Unix socket must be an unused absolute path"
            );
            let listener = tokio::net::UnixListener::bind(&path)?;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
            struct SocketCleanup(std::path::PathBuf);
            impl Drop for SocketCleanup {
                fn drop(&mut self) {
                    let _ = std::fs::remove_file(&self.0);
                }
            }
            let _cleanup = SocketCleanup(path);
            let router = router()
                .route(
                    "/mcp",
                    post(|State(app): State<App>, req: Request<Body>| async move {
                        app.forward("127.0.0.1:0".parse().expect("literal"), req)
                            .await
                    })
                    .get(|State(app): State<App>, req: Request<Body>| async move {
                        app.forward("127.0.0.1:0".parse().expect("literal"), req)
                            .await
                    })
                    .delete(
                        |State(app): State<App>, req: Request<Body>| async move {
                            app.forward("127.0.0.1:0".parse().expect("literal"), req)
                                .await
                        },
                    ),
                )
                .with_state(app);
            let stop_live = shutdown_live.clone();
            let result = wayframe::listeners::serve_unix(
                listener,
                router,
                listener_limits(&config),
                async move {
                    stop_signal().await;
                    stop_live.snapshot().value.gateway.stop_admission();
                },
            )
            .await;
            drop(_admin_task);
            shutdown_live.snapshot().value.plugins.shutdown().await?;
            return result;
        }
        #[cfg(not(unix))]
        {
            let _ = path;
            anyhow::bail!("Unix sockets are not supported on this platform");
        }
    }
    ensure!(
        std::env::var("WAYFRAME_TRANSPORT").map_or(true, |t| t == "http"),
        "WAYFRAME_TRANSPORT must be http, stdio or tcp"
    );
    let router = router()
        .route(
            "/mcp",
            post(handle)
                .get(handle)
                .delete(handle)
                .options(cors_preflight),
        )
        .with_state(app);
    let listener = tokio::net::TcpListener::bind(config.listen).await?;
    tracing::info!(listen = %config.listen, "MCP gateway ready");
    let stop_live = shutdown_live.clone();
    let result = wayframe::listeners::serve_tcp(
        listener,
        router,
        listener_limits(&config),
        listener_tls,
        async move {
            stop_signal().await;
            stop_live.snapshot().value.gateway.stop_admission();
        },
    )
    .await;
    drop(_admin_task);
    shutdown_live.snapshot().value.plugins.shutdown().await?;
    result
}
fn router() -> Router<App> {
    admin_router()
        .route(
            "/healthz",
            get(|| async { Json(serde_json::json!({"alive":true})) }),
        )
        .route("/ws", get(network::upgrade))
        .route("/metrics", get(metrics))
        .route("/readyz", get(readiness))
        .route(
            "/.well-known/oauth-protected-resource",
            get(resource_metadata),
        )
        .route(
            "/.well-known/oauth-protected-resource/mcp",
            get(resource_metadata),
        )
}
async fn resource_metadata(State(app): State<App>) -> Response {
    match app.live.snapshot().value.protected_resource.clone() {
        Some(metadata) => Json(metadata).into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}
async fn readiness(State(app): State<App>) -> Response {
    let deployment = app.live.snapshot();
    let upstream = deployment.value.pool.as_ref().is_none_or(|pool| {
        pool.snapshot()
            .is_ok_and(|backends| backends.iter().any(|b| b.enabled && b.healthy))
    });
    let identity = if let Some(auth) = &deployment.value.oidc {
        auth.readiness().await
    } else {
        true
    };
    let plugins = deployment.value.plugins.readiness().is_ok();
    let process = deployment
        .value
        ._bridge
        .as_ref()
        .is_none_or(|b| b.process.ready());
    let groups = deployment.value.groups.values().all(|pool| {
        pool.snapshot()
            .is_ok_and(|b| b.iter().any(|b| b.enabled && b.healthy))
    });
    if upstream && groups && identity && plugins && process {
        Json(serde_json::json!({"ready":true})).into_response()
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"ready":false})),
        )
            .into_response()
    }
}
fn admin_router() -> Router<App> {
    Router::new().route("/admin/{*path}", axum::routing::any(admin))
}
async fn admin(State(app): State<App>, request: Request<Body>) -> Response {
    use tracing::Instrument;
    let deployment = app.live.snapshot();
    let allowed = app.admin_hosts.as_ref().unwrap_or(&deployment.value.hosts);
    let mut hosts = request.headers().get_all("host").iter();
    if !hosts.next().and_then(|h| h.to_str().ok()).is_some_and(|h| {
        allowed
            .iter()
            .any(|allowed| allowed.eq_ignore_ascii_case(h))
    }) || hosts.next().is_some()
    {
        return StatusCode::FORBIDDEN.into_response();
    }
    if matches!(request.uri().path(), "/admin/deployment" | "/admin/reload") {
        return reload(app, request).await;
    }
    app.store
        .admin(request)
        .instrument(tracing::info_span!("admin.request", otel.kind = "server"))
        .await
}
async fn stop_signal() {
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("SIGTERM handler");
        tokio::select! { _=tokio::signal::ctrl_c()=>{}, _=term.recv()=>{} }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
async fn reload(app: App, request: Request<Body>) -> Response {
    let Some(actor) = app.store.admin_identity(request.headers()) else {
        return StatusCode::UNAUTHORIZED.into_response();
    };
    if !management::Store::permits_admin(&actor, request.method().as_str(), request.uri().path()) {
        return StatusCode::FORBIDDEN.into_response();
    }
    let current = app.live.snapshot();
    if request.method() == "GET" && request.uri().path() == "/admin/deployment" {
        let mut response = Json(
            serde_json::json!({"generation":current.id,"reloadable":app.config_path.is_some()}),
        )
        .into_response();
        response.headers_mut().insert(
            "etag",
            format!("\"{}\"", current.id).parse().expect("generation"),
        );
        return response;
    }
    if request.method() != "POST" || request.uri().path() != "/admin/reload" {
        return StatusCode::METHOD_NOT_ALLOWED.into_response();
    }
    let expected = request
        .headers()
        .get("if-match")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix('"'))
        .and_then(|s| s.strip_suffix('"'))
        .and_then(|s| s.parse::<u64>().ok());
    let Some(expected) = expected else {
        return StatusCode::PRECONDITION_REQUIRED.into_response();
    };
    if current.id != expected {
        return StatusCode::PRECONDITION_FAILED.into_response();
    }
    if app.config_path.is_none() {
        return (
            StatusCode::BAD_REQUEST,
            "configure a trusted WAYFRAME_CONFIG source before reloading",
        )
            .into_response();
    }
    if !matches!(tokio::time::timeout(std::time::Duration::from_secs(5),axum::body::to_bytes(request.into_body(),1)).await,Ok(Ok(bytes)) if bytes.is_empty())
    {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let config = match config::Config::load() {
        Ok(config) => config,
        Err(_) => {
            return (StatusCode::BAD_REQUEST, "configuration validation failed").into_response()
        }
    };
    if (config.listen, config.max_in_flight, config.timeout_seconds) != app.listener_settings {
        return (StatusCode::BAD_REQUEST, "listener settings require restart").into_response();
    }
    if config.bootstrap_fingerprint().ok().as_ref() != Some(&app.bootstrap_fingerprint) {
        return (
            StatusCode::BAD_REQUEST,
            "bootstrap users/rules/admins require the control API or restart",
        )
            .into_response();
    }
    // Prepare outside the publication lock. A failed candidate leaves the old generation active.
    let candidate = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        prepare_deployment(
            &config,
            app.store.clone(),
            app.sessions.clone(),
            expected.saturating_add(1),
            #[cfg(feature = "wasm")]
            app.factory.clone(),
        ),
    )
    .await;
    let prepared = match candidate {
        Ok(Ok(p)) => p,
        _ => return (StatusCode::BAD_REQUEST, "deployment preparation failed").into_response(),
    };
    let _publish = app.publish_lock.lock().await;
    if app.live.snapshot().id != expected {
        return StatusCode::PRECONDITION_FAILED.into_response();
    }
    if app
        .store
        .audit(&actor.id, "reload", "deployment", expected + 1, "intent")
        .is_err()
    {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    let prepared_tls = match tls_from_env() {
        Ok(tls) if tls.is_some() == app.tls.is_some() => tls,
        _ => return (StatusCode::BAD_REQUEST, "TLS preparation failed").into_response(),
    };
    match app.live.publish(expected, prepared) {
        Ok(generation) => {
            if let (Some(current), Some(candidate)) = (&app.tls, &prepared_tls) {
                current.replace(candidate);
            }
            app.sessions.set_generation(generation);
            if app
                .store
                .audit(&actor.id, "reload", "deployment", generation, "committed")
                .is_err()
            {
                return StatusCode::SERVICE_UNAVAILABLE.into_response();
            }
            Json(serde_json::json!({"generation":generation})).into_response()
        }
        Err(_) => StatusCode::PRECONDITION_FAILED.into_response(),
    }
}

fn listener_limits(config: &config::Config) -> wayframe::listeners::Limits {
    wayframe::listeners::Limits {
        max_connections: config.max_in_flight.saturating_mul(4).clamp(16, 100000),
        io_idle_timeout: std::time::Duration::from_secs(config.timeout_seconds),
        ..Default::default()
    }
}

fn tls_from_env() -> Result<Option<wayframe::listeners::Tls>> {
    let cert = std::env::var_os("WAYFRAME_TLS_CERT");
    let key = std::env::var_os("WAYFRAME_TLS_KEY");
    ensure!(
        cert.is_some() == key.is_some(),
        "set TLS certificate and key together"
    );
    let Some(cert) = cert else {
        ensure!(
            std::env::var_os("WAYFRAME_TLS_CLIENT_CA").is_none(),
            "mTLS requires ingress TLS"
        );
        return Ok(None);
    };
    fn read(path: std::ffi::OsString) -> Result<Vec<u8>> {
        let path = std::path::PathBuf::from(path);
        ensure!(
            path.is_file() && path.metadata()?.len() <= 65536,
            "TLS file must be regular and bounded"
        );
        Ok(std::fs::read(path)?)
    }
    let certificate = read(cert)?;
    let key = read(key.expect("validated pair"))?;
    let ca = std::env::var_os("WAYFRAME_TLS_CLIENT_CA")
        .map(read)
        .transpose()?;
    Ok(Some(wayframe::listeners::Tls::from_pem(
        &certificate,
        &key,
        ca.as_deref(),
    )?))
}
