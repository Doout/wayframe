//! Configurable reference gateway, embedded in an application rather than installed by the framework.
mod config;
mod management;
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
    controls::OriginGuard,
    http::{rejection, HttpGateway, Identity, Operation, RequestContext},
    mcp::McpGuard,
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
struct App {
    gateway: Arc<HttpGateway>,
    store: Arc<management::Store>,
    hosts: Arc<Vec<String>>,
    sessions: Arc<session::Sessions>,
}
impl App {
    async fn forward(&self, peer: SocketAddr, request: Request<Body>) -> Response {
        let old_session = request
            .headers()
            .get("mcp-session-id")
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned);
        let deleting = request.method() == "DELETE";
        let mut response = self.gateway.handle(peer, request).await;
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
                    response.status().is_success() && initialize && self.sessions.bind(id, identity)
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
        response
    }
}
async fn handle(
    State(app): State<App>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    request: Request<Body>,
) -> Response {
    app.forward(peer, request).await
}
async fn metrics(State(app): State<App>, request: Request<Body>) -> Response {
    let mut ctx = RequestContext::new(
        request.method().clone(),
        "/metrics".into(),
        "127.0.0.1".parse().expect("literal"),
        tokio::time::Instant::now() + std::time::Duration::from_secs(1),
    );
    ctx.headers = request.headers().clone();
    let decision = app.store.authenticate(&mut ctx);
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
    Json(app.gateway.metrics().snapshot()).into_response()
}
#[tokio::main]
async fn main() -> Result<()> {
    let _telemetry = telemetry::init()?;
    let config = config::Config::load()?;
    let transport = std::env::var("WAYFRAME_TRANSPORT").unwrap_or_else(|_| "http".into());
    ensure!(
        matches!(transport.as_str(), "http" | "stdio"),
        "WAYFRAME_TRANSPORT must be http or stdio"
    );
    ensure!(
        transport != "stdio" || std::env::var_os("WAYFRAME_UNIX_SOCKET").is_none(),
        "select either stdio or a Unix socket"
    );
    let config::Credentials {
        clients,
        upstream_token,
    } = config.credentials()?;
    let (origin, path) = config.endpoint()?;
    let store = management::Store::open(&config, clients, upstream_token.as_deref())?;
    let managed = |control| {
        Arc::new(management::ManagedBlock {
            store: store.clone(),
            control,
        }) as Arc<dyn Block<RequestContext>>
    };
    let sessions = Arc::new(session::Sessions::new());
    let preflight = Pipeline::compile(vec![
        Arc::new(
            OriginGuard::new("origin".into(), config.allowed_origins.clone())
                .map_err(anyhow::Error::msg)?,
        ) as Arc<dyn Block<RequestContext>>,
        managed(management::Control::Authenticate),
        sessions.clone(),
        managed(management::Control::Rate),
    ])?;
    let mut policy: Vec<Arc<dyn Block<RequestContext>>> = vec![
        Arc::new(
            McpGuard::new("mcp-envelope".into(), config.max_request_bytes)
                .map_err(anyhow::Error::msg)?
                .allow_legacy(true)
                .allow_sessions(true),
        ),
        managed(management::Control::Authorize),
    ];
    #[cfg(feature = "wasm")]
    if let Ok(path) = std::env::var("WAYFRAME_PLUGIN_MANIFEST") {
        let runtime = wayframe::wasm::Runtime::new()?;
        let pool = runtime.load(std::path::Path::new(&path)).await?;
        policy.push(Arc::new(wayframe::wasm::WasmBlock::new(
            "plugin".into(),
            pool,
        )));
    }
    #[cfg(not(feature = "wasm"))]
    ensure!(
        std::env::var_os("WAYFRAME_PLUGIN_MANIFEST").is_none(),
        "build with --features wasm to load a plugin"
    );
    policy.push(managed(management::Control::ToolRate));
    policy.push(Arc::new(UpstreamPath(path)));
    let mut builder = HttpGateway::builder(origin)
        .preflight(preflight)
        .policy(Pipeline::compile(policy)?)
        .inspect_body(config.max_request_bytes)
        .limits(config.limits())
        .authorities(config.allowed_hosts.clone());
    if let Some(token) = upstream_token {
        builder = builder.upstream_bearer(token);
    }
    if let Some(ca) = config.ca()? {
        builder = builder.upstream_ca(ca);
    }
    let gateway = builder.build()?;
    let app = App {
        gateway: gateway.clone(),
        store,
        hosts: Arc::new(config.allowed_hosts.clone()),
        sessions,
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
        admin_app.hosts = Arc::new(vec![address.to_string()]);
        Some(tokio::spawn(async move {
            axum::serve(listener, admin_router().with_state(admin_app)).await
        }))
    } else {
        None
    };
    struct AdminTask(Option<tokio::task::JoinHandle<std::io::Result<()>>>);
    impl Drop for AdminTask {
        fn drop(&mut self) {
            if let Some(task) = &self.0 {
                task.abort();
            }
        }
    }
    let _admin_task = AdminTask(admin_task);
    if transport == "stdio" {
        let token = config::secret("WAYFRAME_LOCAL_TOKEN")?;
        stdio::run(
            app,
            token,
            config.max_request_bytes,
            config.max_response_bytes as usize,
            &config.allowed_hosts[0],
        )
        .await?;
        return Ok(());
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
            axum::serve(listener, router)
                .with_graceful_shutdown(async move {
                    stop_signal().await;
                    gateway.stop_admission();
                })
                .await?;
            return Ok(());
        }
        #[cfg(not(unix))]
        {
            let _ = path;
            anyhow::bail!("Unix sockets are not supported on this platform");
        }
    }
    ensure!(
        std::env::var("WAYFRAME_TRANSPORT").map_or(true, |t| t == "http"),
        "WAYFRAME_TRANSPORT must be http or stdio"
    );
    let router = router()
        .route("/mcp", post(handle).get(handle).delete(handle))
        .with_state(app);
    let listener = tokio::net::TcpListener::bind(config.listen).await?;
    tracing::info!(listen = %config.listen, "MCP gateway ready");
    axum::serve(
        listener,
        router.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(async move {
        stop_signal().await;
        gateway.stop_admission();
    })
    .await?;
    Ok(())
}
fn router() -> Router<App> {
    admin_router()
        .route(
            "/healthz",
            get(|| async { Json(serde_json::json!({"alive":true})) }),
        )
        .route("/metrics", get(metrics))
}
fn admin_router() -> Router<App> {
    Router::new().route("/admin/{*path}", axum::routing::any(admin))
}
async fn admin(State(app): State<App>, request: Request<Body>) -> Response {
    use tracing::Instrument;
    let mut hosts = request.headers().get_all("host").iter();
    if !hosts.next().and_then(|h| h.to_str().ok()).is_some_and(|h| {
        app.hosts
            .iter()
            .any(|allowed| allowed.eq_ignore_ascii_case(h))
    }) || hosts.next().is_some()
    {
        return StatusCode::FORBIDDEN.into_response();
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
