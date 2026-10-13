//! MCP JSON-lines frontend. The local client selects a configured bearer identity.
use anyhow::{ensure, Result};
use axum::{
    body::{to_bytes, Body},
    http::Request,
};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use futures_util::StreamExt;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use wayframe::mcp::{parse_unique, LEGACY_VERSION};

fn event_value(data: &str) -> Result<Option<Value>> {
    let data = data.trim_end();
    if data.is_empty() {
        Ok(None)
    } else {
        Ok(Some(parse_unique(data.as_bytes())?))
    }
}
async fn emit(output: &mut tokio::io::Stdout, value: &Value) -> Result<()> {
    output.write_all(&serde_json::to_vec(value)?).await?;
    output.write_all(b"\n").await?;
    output.flush().await?;
    Ok(())
}
pub async fn run(
    app: crate::App,
    token: String,
    maximum: usize,
    response_maximum: usize,
    host: &str,
) -> Result<()> {
    let mut input = BufReader::new(tokio::io::stdin());
    let mut output = tokio::io::stdout();
    let mut session_id: Option<String> = None;
    loop {
        let mut line = Vec::new();
        let read = (&mut input)
            .take(maximum as u64 + 1)
            .read_until(b'\n', &mut line)
            .await?;
        if read == 0 {
            if let Some(id) = session_id {
                let request = Request::builder()
                    .method("DELETE")
                    .uri("/mcp")
                    .header("host", host)
                    .header("authorization", format!("Bearer {token}"))
                    .header("mcp-protocol-version", LEGACY_VERSION)
                    .header("mcp-session-id", id)
                    .body(Body::empty())?;
                let _ = app.forward("127.0.0.1:0".parse()?, request).await;
            }
            return Ok(());
        }
        ensure!(
            line.len() <= maximum && line.ends_with(b"\n"),
            "stdio envelope exceeds configured limit or is not newline terminated"
        );
        let value = match parse_unique(&line) {
            Ok(value) => value,
            Err(_) => {
                emit(&mut output, &json!({"jsonrpc":"2.0","id":null,"error":{"code":-32700,"message":"Parse error"}})).await?;
                continue;
            }
        };
        let method = value.get("method").and_then(Value::as_str).unwrap_or("");
        let meta = value
            .pointer("/params/_meta/io.modelcontextprotocol~1protocolVersion")
            .and_then(Value::as_str);
        let mut builder = Request::builder()
            .method("POST")
            .uri("/mcp")
            .header("host", host)
            .header("authorization", format!("Bearer {token}"))
            .header("content-type", "application/json")
            .header("accept", "application/json, text/event-stream");
        if method == "initialize" {
            if let Some(id) = session_id.take() {
                app.sessions.remove(&id);
            }
        } else if let Some(id) = &session_id {
            builder = builder.header("mcp-session-id", id);
        }
        if let Some(version) = meta {
            builder = builder
                .header("mcp-protocol-version", version)
                .header("mcp-method", method);
            if let Some(target) = value.pointer("/params/name").and_then(Value::as_str) {
                let name = if target.bytes().all(|b| (0x20..=0x7e).contains(&b))
                    && !target.starts_with("=?base64?")
                {
                    target.to_owned()
                } else {
                    format!("=?base64?{}?=", STANDARD.encode(target))
                };
                builder = builder.header("mcp-name", name);
            }
        } else if method != "initialize" {
            builder = builder.header("mcp-protocol-version", LEGACY_VERSION);
        }
        let response = app
            .forward("127.0.0.1:0".parse()?, builder.body(Body::from(line))?)
            .await;
        if let Some(id) = response.headers().get("mcp-session-id") {
            session_id = Some(id.to_str()?.to_owned());
        }
        if response.status() == axum::http::StatusCode::NOT_FOUND {
            session_id = None;
        }
        if !response.status().is_success() {
            if let Some(id) = value.get("id") {
                emit(&mut output, &json!({"jsonrpc":"2.0","id":id,"error":{"code":-32000,"message":"Gateway rejected request","data":{"http_status":response.status().as_u16()}}})).await?;
            }
            continue;
        }
        let sse = response
            .headers()
            .get("content-type")
            .and_then(|h| h.to_str().ok())
            .is_some_and(|h| h.starts_with("text/event-stream"));
        if sse {
            let mut stream = response.into_body().into_data_stream();
            let mut pending = String::new();
            let mut bytes = Vec::new();
            while let Some(chunk) = stream.next().await {
                bytes.extend_from_slice(&chunk?);
                ensure!(
                    bytes.len() <= response_maximum,
                    "stdio SSE event limit exceeded"
                );
                // Decode only complete lines; a UTF-8 code point may span body chunks.
                while let Some(end) = bytes.iter().position(|b| *b == b'\n') {
                    let line: Vec<u8> = bytes.drain(..=end).collect();
                    let line = std::str::from_utf8(&line)?.trim_end_matches(['\r', '\n']);
                    if line.is_empty() {
                        if let Some(value) = event_value(&pending)? {
                            emit(&mut output, &value).await?;
                        }
                        pending.clear();
                    } else if let Some(data) = line.strip_prefix("data:") {
                        pending.push_str(data.strip_prefix(' ').unwrap_or(data));
                        pending.push('\n');
                        ensure!(
                            pending.len() <= response_maximum,
                            "stdio SSE event limit exceeded"
                        );
                    }
                }
            }
            ensure!(
                bytes.is_empty() && pending.is_empty(),
                "truncated upstream SSE event"
            );
        } else {
            let body = to_bytes(response.into_body(), response_maximum).await?;
            if !body.is_empty() {
                emit(&mut output, &parse_unique(&body)?).await?;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn sse_primers_do_not_become_invalid_jsonrpc_messages() {
        assert!(event_value("\n").unwrap().is_none());
        assert_eq!(
            event_value(r#"{"jsonrpc":"2.0","id":12,"result":{}}"#)
                .unwrap()
                .unwrap()["id"],
            12
        );
        assert!(event_value(r#"{"id":1,"id":2}"#).is_err());
    }
}
