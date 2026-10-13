//! Reference application. No gateway executable is installed by the framework.
use anyhow::{ensure, Context as _, Result};
use axum::{
    body::Body,
    extract::{ConnectInfo, State},
    http::Request,
    response::Response,
    routing::{get, post},
    Json, Router,
};
use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::Duration};
use wayframe::{
    controls::{BearerTokens, OriginGuard, RateLimit},
    http::{HttpGateway, Identity, Limits, RequestContext},
    mcp::{McpGuard, ToolPermissions},
    prelude::*,
};

fn secret(name: &str) -> Result<String> {
    let value = std::env::var(name)
        .with_context(|| format!("set {name}; see examples/mcp-gateway/README.md"))?;
    ensure!(
        value.len() >= 32 && value.len() <= 8192 && !value.contains(char::is_whitespace),
        "invalid credential in {name}"
    );
    Ok(value)
}
async fn handle(
    State(gateway): State<Arc<HttpGateway>>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    request: Request<Body>,
) -> Response {
    gateway.handle(peer, request).await
}
#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let listen: SocketAddr = std::env::var("WAYFRAME_LISTEN")
        .unwrap_or_else(|_| "127.0.0.1:8080".into())
        .parse()?;
    ensure!(
        listen.ip().is_loopback(),
        "the example only exposes loopback HTTP; terminate TLS before public use"
    );
    let upstream =
        std::env::var("WAYFRAME_UPSTREAM").unwrap_or_else(|_| "http://127.0.0.1:9001".into());
    let reader = secret("WAYFRAME_READER_TOKEN")?;
    let operator = secret("WAYFRAME_OPERATOR_TOKEN")?;
    let upstream_token = secret("WAYFRAME_UPSTREAM_TOKEN")?;
    ensure!(
        reader != operator && reader != upstream_token && operator != upstream_token,
        "all three credentials must be distinct"
    );
    let origins = vec![
        format!("http://127.0.0.1:{}", listen.port()),
        format!("http://localhost:{}", listen.port()),
    ];
    let preflight = Pipeline::compile(vec![
        Arc::new(OriginGuard::new("origin".into(), origins).map_err(anyhow::Error::msg)?)
            as Arc<dyn Block<RequestContext>>,
        Arc::new(
            BearerTokens::new(
                "auth".into(),
                vec![
                    (
                        reader,
                        Identity::new("reader", "demo").with_permissions(["tools:read"]),
                    ),
                    (
                        operator,
                        Identity::new("operator", "demo")
                            .with_permissions(["tools:read", "tools:admin"]),
                    ),
                ],
            )
            .map_err(anyhow::Error::msg)?,
        ),
        Arc::new(RateLimit::new("rate".into(), 10.0, 20, 128).map_err(anyhow::Error::msg)?),
    ])?;
    let mut policy: Vec<Arc<dyn Block<RequestContext>>> = vec![
        Arc::new(
            McpGuard::new("mcp-envelope".into(), 32768)
                .map_err(anyhow::Error::msg)?
                .allow_legacy(true),
        ),
        Arc::new(
            ToolPermissions::new(
                "tool-permissions".into(),
                HashMap::from([
                    ("add".into(), "tools:read".into()),
                    ("search_docs".into(), "tools:read".into()),
                    ("admin_preview".into(), "tools:admin".into()),
                ]),
            )
            .map_err(anyhow::Error::msg)?,
        ),
    ];
    #[cfg(feature = "wasm")]
    if let Ok(path) = std::env::var("WAYFRAME_PLUGIN_MANIFEST") {
        // Compilation and clean instance preparation finish BEFORE binding the listener.
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
        "rebuild the example with --features wasm to load a plugin"
    );
    // Keep this mutable for the optional plugin, without imposing a feature on native builds.
    let _ = &mut policy;
    let gateway = HttpGateway::builder(upstream)
        .preflight(preflight)
        .policy(Pipeline::compile(policy)?)
        .inspect_body(32768)
        .limits(Limits {
            max_request_bytes: 32768,
            max_response_bytes: 1024 * 1024,
            request_timeout: Duration::from_secs(15),
            ..Limits::default()
        })
        .authorities(vec![
            format!("127.0.0.1:{}", listen.port()),
            format!("localhost:{}", listen.port()),
        ])
        .upstream_bearer(upstream_token)
        .build()?;
    let app = Router::new()
        .route("/mcp", post(handle))
        .route(
            "/healthz",
            get(|| async {
                Json(serde_json::json!({"ready":true,"example":"wayframe-mcp-gateway"}))
            }),
        )
        .with_state(gateway.clone());
    let listener = tokio::net::TcpListener::bind(listen).await?;
    tracing::info!(%listen,"MCP reference gateway ready (loopback development profile)");
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(async move {
        stop_signal().await;
        gateway.stop_admission();
    })
    .await?;
    Ok(())
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
