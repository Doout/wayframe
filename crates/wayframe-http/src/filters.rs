//! Explicit bounded response hooks for trusted native code. Authorization headers are never projected.
use crate::stream::StreamError;
use crate::{Identity, Operation};
use bytes::Bytes;
use futures_util::{Stream, StreamExt};
use http::{HeaderMap, StatusCode};
use std::sync::Arc;
use wayframe_core::Decision;
#[derive(Clone)]
pub struct ResponseContext {
    pub identity: Option<Identity>,
    pub operation: Option<Operation>,
    pub status: StatusCode,
    pub origin: Option<String>,
    pub request: Option<serde_json::Value>,
    pub generation: u64,
    pub backend: Option<String>,
    pub protocol_error: Arc<std::sync::atomic::AtomicBool>,
    pub terminal_seen: Arc<std::sync::atomic::AtomicBool>,
    pub records_seen: Arc<std::sync::atomic::AtomicU64>,
}
pub trait ResponseFilter: Send + Sync + 'static {
    fn id(&self) -> &str;
    fn preflight(&self, _origin: &str, _headers: &mut HeaderMap) -> bool {
        false
    }
    fn headers(&self, _context: &ResponseContext, _headers: &mut HeaderMap) -> Decision {
        Decision::Continue
    }
    fn records(&self, _context: &ResponseContext) -> bool {
        false
    }
    fn record(&self, _context: &ResponseContext, _value: &mut serde_json::Value) -> Decision {
        Decision::Continue
    }
    fn end(&self, _context: &ResponseContext) -> Decision {
        Decision::Continue
    }
    fn complete(&self, _context: &ResponseContext, _outcome: &'static str) {}
}
/// Validators and range metadata describe the upstream bytes, not a transformed representation.
pub(crate) fn strip_representation_metadata(headers: &mut HeaderMap) {
    for name in [
        "etag",
        "last-modified",
        "content-md5",
        "digest",
        "content-digest",
        "repr-digest",
        "accept-ranges",
        "content-range",
    ] {
        headers.remove(name);
    }
}
fn record(
    filters: &[Arc<dyn ResponseFilter>],
    context: &ResponseContext,
    value: &mut serde_json::Value,
) -> Result<(), StreamError> {
    if value.get("error").is_some()
        || value.pointer("/result/isError") == Some(&serde_json::Value::Bool(true))
        || value.pointer("/result/status") == Some(&serde_json::Value::String("failed".into()))
    {
        context
            .protocol_error
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }
    context
        .records_seen
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    for filter in filters {
        let span = tracing::info_span!("response.filter", plugin.id = filter.id());
        let decision = span.in_scope(|| filter.record(context, value));
        if decision != Decision::Continue {
            return Err(StreamError::Filter);
        }
    }
    Ok(())
}
fn end(filters: &[Arc<dyn ResponseFilter>], context: &ResponseContext) -> Result<(), StreamError> {
    for filter in filters {
        if filter.end(context) != Decision::Continue {
            context
                .protocol_error
                .store(true, std::sync::atomic::Ordering::Relaxed);
            return Err(StreamError::Filter);
        }
    }
    Ok(())
}
pub fn transform<S>(
    source: S,
    sse: bool,
    maximum: usize,
    filters: Vec<Arc<dyn ResponseFilter>>,
    context: ResponseContext,
) -> impl Stream<Item = Result<Bytes, StreamError>> + Send
where
    S: Stream<Item = Result<Bytes, StreamError>> + Send + 'static,
{
    async_stream::try_stream! {
        tokio::pin!(source);
        let mut total = 0usize;
        if sse {
            let mut decoder = wayframe_codec::Sse::new(maximum).map_err(|_| StreamError::Filter)?;
            while let Some(chunk) = source.next().await {
                let chunk = chunk?;
                // Feed chunks in bounded pieces; a network chunk can contain many complete events.
                for piece in chunk.chunks(16 * 1024) {
                    for mut event in decoder.push(piece).map_err(|_| StreamError::Filter)? {
                        if !event.data.is_empty() {
                            let mut value = wayframe_codec::parse_unique(event.data.as_bytes())
                                .map_err(|_| StreamError::Filter)?;
                            record(&filters, &context, &mut value)?;
                            event.data =
                                serde_json::to_string(&value).map_err(|_| StreamError::Filter)?;
                        }
                        let bytes = event.encode();
                        total = total
                            .checked_add(bytes.len())
                            .ok_or(StreamError::SizeLimit)?;
                        if total > maximum {
                            Err(StreamError::SizeLimit)?;
                        }
                        yield Bytes::from(bytes);
                    }
                }
            }
            decoder.finish().map_err(|_| StreamError::Filter)?;
            end(&filters, &context)?;
        } else {
            let mut bytes = Vec::new();
            while let Some(chunk) = source.next().await {
                let chunk = chunk?;
                if bytes.len().saturating_add(chunk.len()) > maximum {
                    Err(StreamError::SizeLimit)?;
                }
                bytes.extend_from_slice(&chunk);
            }
            if bytes.is_empty() {
                end(&filters, &context)?;
            }
            if !bytes.is_empty() {
                let mut value =
                    wayframe_codec::parse_unique(&bytes).map_err(|_| StreamError::Filter)?;
                record(&filters, &context, &mut value)?;
                end(&filters, &context)?;
                let bytes = serde_json::to_vec(&value).map_err(|_| StreamError::Filter)?;
                if bytes.len() > maximum {
                    Err(StreamError::SizeLimit)?;
                }
                yield Bytes::from(bytes);
            }
        }
    }
}
