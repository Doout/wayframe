//! Bounded custom JSON message bindings. These are optional gateway bindings, not MCP standard transports.
use anyhow::{ensure, Result};
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use std::{future::Future, pin::Pin, sync::Arc, time::Duration};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::{Framed, LengthDelimitedCodec};
pub type Reply = Pin<Box<dyn Future<Output = Result<Value>> + Send>>;
pub trait Handler: Send + Sync + 'static {
    fn handle(&self, message: Value) -> Reply;
}
impl<F, Fut> Handler for F
where
    F: Fn(Value) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Value>> + Send + 'static,
{
    fn handle(&self, message: Value) -> Reply {
        Box::pin(self(message))
    }
}
#[derive(Clone)]
pub struct Limits {
    pub maximum: usize,
    pub idle: Duration,
    pub request: Duration,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            maximum: 1024 * 1024,
            idle: Duration::from_secs(15),
            request: Duration::from_secs(15),
        }
    }
}
impl Limits {
    fn validate(&self) -> Result<()> {
        ensure!(
            (1024..=1024 * 1024).contains(&self.maximum)
                && !self.idle.is_zero()
                && self.idle <= Duration::from_secs(600)
                && !self.request.is_zero()
                && self.request <= Duration::from_secs(120),
            "invalid network limits"
        );
        Ok(())
    }
}
/// Four-byte big-endian length. Reads, policy execution and writes all have bounded deadlines.
pub async fn framed<T: AsyncRead + AsyncWrite + Unpin>(
    io: T,
    handler: Arc<dyn Handler>,
    limits: Limits,
) -> Result<()> {
    limits.validate()?;
    let mut wire = Framed::new(
        io,
        LengthDelimitedCodec::builder()
            .max_frame_length(limits.maximum)
            .new_codec(),
    );
    loop {
        let Some(message) = tokio::time::timeout(limits.idle, wire.next()).await? else {
            return Ok(());
        };
        let message = wayframe_codec::parse_unique(&message?)?;
        let response = tokio::time::timeout(limits.request, handler.handle(message)).await??;
        let response = serde_json::to_vec(&response)?;
        ensure!(response.len() <= limits.maximum, "response frame too large");
        tokio::time::timeout(limits.idle, wire.send(Bytes::from(response))).await??;
    }
}
/// WebSocket JSON text/binary records; Ping/Pong and close remain protocol controls.
pub async fn websocket(
    mut socket: axum::extract::ws::WebSocket,
    handler: Arc<dyn Handler>,
    limits: Limits,
) -> Result<()> {
    use axum::extract::ws::Message;
    limits.validate()?;
    loop {
        let Some(message) = tokio::time::timeout(limits.idle, socket.recv()).await? else {
            return Ok(());
        };
        let bytes = match message? {
            Message::Text(s) => s.as_bytes().to_vec(),
            Message::Binary(b) => b.to_vec(),
            Message::Close(_) => return Ok(()),
            Message::Ping(b) => {
                tokio::time::timeout(limits.idle, socket.send(Message::Pong(b))).await??;
                continue;
            }
            Message::Pong(_) => continue,
        };
        ensure!(bytes.len() <= limits.maximum, "WebSocket frame too large");
        let value = wayframe_codec::parse_unique(&bytes)?;
        let reply = tokio::time::timeout(limits.request, handler.handle(value)).await??;
        let bytes = serde_json::to_string(&reply)?;
        ensure!(
            bytes.len() <= limits.maximum,
            "WebSocket response too large"
        );
        tokio::time::timeout(limits.idle, socket.send(Message::Text(bytes.into()))).await??;
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn fragmented_frames_and_slow_peers_are_bounded() {
        let (client, server) = tokio::io::duplex(16);
        let worker = tokio::spawn(framed(
            server,
            Arc::new(|value| async move { Ok(value) }),
            Limits {
                idle: Duration::from_millis(100),
                ..Default::default()
            },
        ));
        let mut wire = Framed::new(client, LengthDelimitedCodec::new());
        wire.send(Bytes::from_static(br#"{"id":1}"#)).await.unwrap();
        let reply = wire.next().await.unwrap().unwrap();
        assert_eq!(wayframe_codec::parse_unique(&reply).unwrap()["id"], 1);
        assert!(worker.await.unwrap().is_err());
    }
}
