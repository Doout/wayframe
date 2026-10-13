//! Private loopback bridge for an operator-configured supervised stdio upstream.
use anyhow::Result;
use axum::{
    body::{to_bytes, Body},
    extract::State,
    http::{Request, StatusCode},
    response::{IntoResponse, Response},
    routing::post,
    Router,
};
use std::sync::Arc;
#[derive(Clone)]
struct StateData {
    process: Arc<wayframe::transport::stdio::Stdio>,
    token: String,
    maximum: usize,
    initialized: Option<serde_json::Value>,
}
pub struct Bridge {
    task: tokio::task::JoinHandle<Result<()>>,
    pub process: Arc<wayframe::transport::stdio::Stdio>,
    pub endpoint: String,
    pub token: String,
}
impl Drop for Bridge {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Bridge {
    pub async fn prepare(config: wayframe::transport::stdio::Config) -> Result<Self> {
        let maximum = config.max_message_bytes;
        let process = wayframe::transport::stdio::Stdio::spawn(config)?;
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        let discover = serde_json::json!({"jsonrpc":"2.0","id":0,"method":"server/discover","params":{"_meta":{"io.modelcontextprotocol/protocolVersion":wayframe::mcp::MODERN_VERSION,"io.modelcontextprotocol/clientCapabilities":{}}}});
        let response = process
            .call(discover, deadline)
            .await?
            .ok_or_else(|| anyhow::anyhow!("MCP discovery response missing"))?;
        let initialized = if response.get("error").is_some() {
            let initialize = serde_json::json!({"jsonrpc":"2.0","id":0,"method":"initialize","params":{"protocolVersion":wayframe::mcp::LEGACY_VERSION,"capabilities":{},"clientInfo":{"name":"wayframe","version":"0.1"}}});
            let response = process
                .call(initialize, deadline)
                .await?
                .ok_or_else(|| anyhow::anyhow!("MCP initialization response missing"))?;
            anyhow::ensure!(
                response
                    .pointer("/result/protocolVersion")
                    .and_then(serde_json::Value::as_str)
                    == Some(wayframe::mcp::LEGACY_VERSION),
                "stdio server does not support configured legacy profile"
            );
            process
                .call(
                    serde_json::json!({"jsonrpc":"2.0","method":"notifications/initialized"}),
                    deadline,
                )
                .await?;
            Some(response["result"].clone())
        } else {
            anyhow::ensure!(
                response.get("result").is_some(),
                "invalid discovery response"
            );
            None
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}/mcp", listener.local_addr()?);
        use rand::RngCore;
        let mut bytes = [0u8; 48];
        rand::rng().fill_bytes(&mut bytes);
        let token = hex::encode(bytes);
        let state = StateData {
            process: process.clone(),
            token: token.clone(),
            maximum,
            initialized,
        };
        let router = Router::new().route("/mcp", post(forward)).with_state(state);
        let task = tokio::spawn(wayframe::listeners::serve_tcp(
            listener,
            router,
            Default::default(),
            None,
            std::future::pending(),
        ));
        Ok(Self {
            task,
            process,
            endpoint,
            token,
        })
    }
}
async fn forward(State(state): State<StateData>, request: Request<Body>) -> Response {
    let expected = format!("Bearer {}", state.token);
    use subtle::ConstantTimeEq;
    let valid = request
        .headers()
        .get("authorization")
        .is_some_and(|v| bool::from(v.as_bytes().ct_eq(expected.as_bytes())))
        && request.headers().get_all("authorization").iter().count() == 1;
    if !valid {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let bytes = match to_bytes(request.into_body(), state.maximum).await {
        Ok(bytes) => bytes,
        Err(_) => return StatusCode::PAYLOAD_TOO_LARGE.into_response(),
    };
    let value = match wayframe::mcp::parse_unique(&bytes) {
        Ok(value) => value,
        Err(_) => return StatusCode::BAD_REQUEST.into_response(),
    };
    if value["method"] == "initialize" {
        if let Some(result) = &state.initialized {
            return axum::Json(
                serde_json::json!({"jsonrpc":"2.0","id":value["id"],"result":result}),
            )
            .into_response();
        }
    }
    let notification = value.get("id").is_none();
    match state
        .process
        .responses(
            value,
            tokio::time::Instant::now() + std::time::Duration::from_secs(120),
        )
        .await
    {
        Ok(stream) if notification => {
            drop(stream);
            StatusCode::ACCEPTED.into_response()
        }
        Ok(stream) => {
            use futures_util::StreamExt;
            let body = stream.map(|value| {
                value.and_then(|value| {
                    Ok::<_, anyhow::Error>(format!(
                        "event: message\ndata: {}\n\n",
                        serde_json::to_string(&value)?
                    ))
                })
            });
            let mut response = Response::new(Body::from_stream(body));
            response.headers_mut().insert(
                "content-type",
                "text/event-stream".parse().expect("literal"),
            );
            response
        }
        Err(_) => StatusCode::BAD_GATEWAY.into_response(),
    }
}
