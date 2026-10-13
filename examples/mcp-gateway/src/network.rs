//! Custom network bindings reuse the same authenticated HTTP execution path.
use anyhow::{ensure, Result};
use axum::{
    body::{to_bytes, Body},
    extract::{ws::WebSocketUpgrade, ConnectInfo, State},
    http::{HeaderMap, Request, StatusCode},
    response::{IntoResponse, Response},
};
use serde_json::{json, Value};
use std::{net::SocketAddr, sync::Arc};
pub async fn call(
    app: crate::App,
    headers: HeaderMap,
    peer: SocketAddr,
    value: Value,
) -> Result<Value> {
    let method = value["method"].as_str().unwrap_or("");
    let version = value
        .pointer("/params/_meta/io.modelcontextprotocol~1protocolVersion")
        .and_then(Value::as_str)
        .unwrap_or(wayframe::mcp::LEGACY_VERSION);
    let mut request = Request::builder()
        .method("POST")
        .uri("/mcp")
        .header("content-type", "application/json")
        .header("accept", "application/json, text/event-stream");
    for name in ["authorization", "host", "origin", "mcp-session-id"] {
        for header in headers.get_all(name) {
            request = request.header(name, header);
        }
    }
    if method != "initialize" {
        request = request.header("mcp-protocol-version", version);
    }
    if version == wayframe::mcp::MODERN_VERSION {
        request = request.header("mcp-method", method);
        if let Some(target) = value
            .pointer("/params/name")
            .or_else(|| value.pointer("/params/uri"))
            .and_then(Value::as_str)
        {
            use base64::Engine;
            request = request.header(
                "mcp-name",
                format!(
                    "=?base64?{}?=",
                    base64::engine::general_purpose::STANDARD.encode(target)
                ),
            );
        }
    }
    let response = app
        .forward(peer, request.body(Body::from(serde_json::to_vec(&value)?))?)
        .await;
    let status = response.status();
    let session = response
        .headers()
        .get("mcp-session-id")
        .and_then(|h| h.to_str().ok())
        .map(str::to_owned);
    let sse = response
        .headers()
        .get("content-type")
        .and_then(|h| h.to_str().ok())
        .is_some_and(|h| h.starts_with("text/event-stream"));
    let bytes = to_bytes(response.into_body(), 1024 * 1024).await?;
    if !status.is_success() {
        return Ok(json!({"status":status.as_u16(),"error":"gateway_rejected"}));
    }
    let messages = if sse {
        let mut decoder = wayframe::codec::Sse::new(1024 * 1024).map_err(anyhow::Error::msg)?;
        let mut values = Vec::new();
        for piece in bytes.chunks(16384) {
            for event in decoder.push(piece).map_err(anyhow::Error::msg)? {
                if !event.data.is_empty() {
                    values.push(wayframe::mcp::parse_unique(event.data.as_bytes())?);
                }
            }
        }
        decoder.finish().map_err(anyhow::Error::msg)?;
        values
    } else if bytes.is_empty() {
        vec![]
    } else {
        vec![wayframe::mcp::parse_unique(&bytes)?]
    };
    Ok(json!({"status":status.as_u16(),"session":session,"messages":messages}))
}
pub async fn upgrade(
    State(app): State<crate::App>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> Response {
    let deployment = app.live.snapshot();
    let mut context = wayframe::http::RequestContext::new(
        axum::http::Method::GET,
        "/mcp".into(),
        peer.ip(),
        tokio::time::Instant::now() + std::time::Duration::from_secs(5),
    );
    context.headers = headers.clone();
    let decision = if let Some(auth) = &deployment.value.oidc {
        auth.authenticate(&mut context).await
    } else {
        app.store.authenticate(&mut context)
    };
    if decision != wayframe::Decision::Continue {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    if headers.get("origin").is_some_and(|v| {
        !deployment
            .value
            .origins
            .iter()
            .any(|o| v.as_bytes() == o.as_bytes())
    }) {
        return StatusCode::FORBIDDEN.into_response();
    }
    ws.max_message_size(1024 * 1024)
        .max_frame_size(1024 * 1024)
        .on_upgrade(move |socket| async move {
            let handler = Arc::new(move |value| call(app.clone(), headers.clone(), peer, value));
            let _ =
                wayframe::transport::network::websocket(socket, handler, Default::default()).await;
        })
}
pub async fn tcp(app: crate::App, address: SocketAddr) -> Result<()> {
    ensure!(
        address.ip().is_loopback(),
        "custom TCP binding must bind to loopback or a TLS tunnel"
    );
    let listener = tokio::net::TcpListener::bind(address).await?;
    let slots = Arc::new(tokio::sync::Semaphore::new(128));
    let mut tasks = tokio::task::JoinSet::new();
    loop {
        tokio::select! {
            _=crate::stop_signal()=>break,
            _=tasks.join_next(),if !tasks.is_empty()=>{},
            accepted=listener.accept()=>{let (io,peer)=accepted?;let Ok(permit)=slots.clone().try_acquire_owned() else {continue;};let app=app.clone();tasks.spawn(async move {let _permit=permit;let handler=Arc::new(move |value:Value|{
                let app=app.clone();async move {let token=value["authorization"].as_str().ok_or_else(||anyhow::anyhow!("authorization required"))?;let mut headers=HeaderMap::new();headers.insert("authorization",token.parse()?);headers.insert("host",app.live.snapshot().value.hosts[0].parse()?);let message=value.get("message").cloned().ok_or_else(||anyhow::anyhow!("message required"))?;call(app,headers,peer,message).await}
            });let _=wayframe::transport::network::framed(io,handler,Default::default()).await;});}
        }
    }
    tasks.abort_all();
    while tasks.join_next().await.is_some() {}
    Ok(())
}
