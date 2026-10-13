use crate::Metrics;
use bytes::Bytes;
use futures_util::{Stream, StreamExt};
use http::{header::HeaderName, HeaderMap};
use std::sync::atomic::Ordering;
use std::{sync::Arc, time::Duration};
use tokio::time::{sleep_until, timeout_at, Instant};

/// Conservative path contract shared by policies and routing. Ambiguous encodings are rejected.
pub fn canonical_path(raw: &str) -> Option<String> {
    if !raw.starts_with('/') || raw.len() > 8192 {
        return None;
    }
    let input = raw.as_bytes();
    let mut out = Vec::with_capacity(input.len());
    let mut i = 0;
    while i < input.len() {
        let c = if input[i] == b'%' {
            if i + 2 >= input.len() {
                return None;
            }
            let h = (input[i + 1] as char).to_digit(16)?;
            let l = (input[i + 2] as char).to_digit(16)?;
            let v = (h * 16 + l) as u8;
            // Reject encoded separators and double decoding.
            if matches!(v, b'/' | b'\\' | b'%') {
                return None;
            }
            i += 3;
            v
        } else {
            let v = input[i];
            i += 1;
            v
        };
        if c.is_ascii_control() || matches!(c, b'\\' | b'?' | b'#') {
            return None;
        }
        out.push(c);
    }
    let path = String::from_utf8(out).ok()?;
    if path.contains("//") || path.split('/').any(|x| x == "." || x == "..") {
        return None;
    }
    Some(path)
}
pub fn strip_hop_headers(headers: &mut HeaderMap) {
    let nominated: Vec<HeaderName> = headers
        .get_all(http::header::CONNECTION)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .filter_map(|s| HeaderName::from_bytes(s.trim().as_bytes()).ok())
        .collect();
    for name in nominated {
        headers.remove(name);
    }
    for name in [
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
    ] {
        headers.remove(name);
    }
}
pub fn strip_client_identity(headers: &mut HeaderMap) {
    let names: Vec<HeaderName> = headers
        .keys()
        .filter(|n| n.as_str().starts_with("x-wayframe-") || n.as_str().starts_with("x-forwarded-"))
        .cloned()
        .collect();
    for n in names {
        headers.remove(n);
    }
    for n in ["authorization", "cookie", "forwarded", "host"] {
        headers.remove(n);
    }
}
/// Error provenance is retained so gateway budgets do not eject healthy upstreams.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamError {
    Deadline,
    PacingDeadline,
    SizeLimit,
    Filter,
    Source(String),
}
impl std::fmt::Display for StreamError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Deadline => f.write_str("body deadline"),
            Self::PacingDeadline => f.write_str("pacing deadline"),
            Self::SizeLimit => f.write_str("body size limit exceeded"),
            Self::Filter => f.write_str("response filter failed"),
            Self::Source(_) => f.write_str("body source failed"),
        }
    }
}
impl std::error::Error for StreamError {}
impl StreamError {
    pub fn is_source_failure(&self) -> bool {
        matches!(self, Self::Source(_))
    }
}
/// Bounded body stream with per-direction, per-stream pacing. It does not buffer full payloads.
pub fn constrained<S, E>(
    source: S,
    maximum: u64,
    bytes_per_second: u64,
    deadline: Instant,
    metrics: Arc<Metrics>,
    outbound: bool,
) -> impl Stream<Item = Result<Bytes, StreamError>> + Send
where
    S: Stream<Item = Result<Bytes, E>> + Send + 'static,
    E: std::fmt::Display + Send + 'static,
{
    async_stream::try_stream! {
        tokio::pin!(source);
        let mut total = 0u64;
        let mut scheduled = Instant::now();
        loop {
            let item = timeout_at(deadline, source.next())
                .await
                .map_err(|_| StreamError::Deadline)?;
            let Some(item) = item else {
                break;
            };
            let mut data = item.map_err(|e| StreamError::Source(e.to_string()))?;
            total = total
                .checked_add(data.len() as u64)
                .ok_or(StreamError::SizeLimit)?;
            if total > maximum {
                Err(StreamError::SizeLimit)?;
            }
            while !data.is_empty() {
                let chunk = data.split_to(data.len().min(16 * 1024));
                if bytes_per_second != 0 {
                    scheduled = scheduled.max(Instant::now())
                        + Duration::from_secs_f64(chunk.len() as f64 / bytes_per_second as f64);
                    timeout_at(deadline, sleep_until(scheduled))
                        .await
                        .map_err(|_| StreamError::PacingDeadline)?;
                }
                if outbound {
                    metrics
                        .bytes_out
                        .fetch_add(chunk.len() as u64, Ordering::Relaxed);
                } else {
                    metrics
                        .bytes_in
                        .fetch_add(chunk.len() as u64, Ordering::Relaxed);
                }
                yield chunk;
            }
        }
    }
}
/// Preserve HTTP/2 and gRPC trailers while applying the same body budgets.
pub fn constrained_frames<B>(
    body: B,
    maximum: u64,
    rate: u64,
    deadline: Instant,
    metrics: Option<Arc<Metrics>>,
) -> impl Stream<Item = Result<http_body::Frame<Bytes>, StreamError>> + Send
where
    B: http_body::Body<Data = Bytes> + Send + 'static,
    B::Error: std::fmt::Display + Send,
{
    use http_body_util::BodyExt;
    async_stream::try_stream! {
        tokio::pin!(body);
        let mut total = 0u64;
        let mut scheduled = Instant::now();
        let mut trailers_seen = false;
        while let Some(frame) = timeout_at(deadline, body.frame())
            .await
            .map_err(|_| StreamError::Deadline)?
        {
            let frame = frame.map_err(|e| StreamError::Source(e.to_string()))?;
            match frame.into_data() {
                Ok(mut data) => {
                    if trailers_seen {
                        Err(StreamError::Filter)?;
                    }
                    total = total
                        .checked_add(data.len() as u64)
                        .ok_or(StreamError::SizeLimit)?;
                    if total > maximum {
                        Err(StreamError::SizeLimit)?;
                    }
                    while !data.is_empty() {
                        let chunk = data.split_to(data.len().min(16384));
                        if rate != 0 {
                            scheduled = scheduled.max(Instant::now())
                                + Duration::from_secs_f64(chunk.len() as f64 / rate as f64);
                            timeout_at(deadline, sleep_until(scheduled))
                                .await
                                .map_err(|_| StreamError::PacingDeadline)?;
                        }
                        if let Some(metrics) = &metrics {
                            metrics
                                .bytes_in
                                .fetch_add(chunk.len() as u64, Ordering::Relaxed);
                        }
                        yield http_body::Frame::data(chunk);
                    }
                }
                Err(frame) => {
                    if let Ok(mut trailers) = frame.into_trailers() {
                        if trailers_seen
                            || trailers.len() > 128
                            || trailers
                                .iter()
                                .map(|(k, v)| k.as_str().len() + v.as_bytes().len())
                                .sum::<usize>()
                                > 16384
                        {
                            Err(StreamError::SizeLimit)?;
                        }
                        trailers_seen = true;
                        strip_hop_headers(&mut trailers);
                        strip_client_identity(&mut trailers);
                        yield http_body::Frame::trailers(trailers);
                    }
                }
            }
        }
    }
}
/// A synchronized polling wrapper allows streaming request bodies in reqwest without buffering.
pub struct FrameBody<S>(std::sync::Mutex<std::pin::Pin<Box<S>>>);
impl<S> FrameBody<S> {
    pub fn new(stream: S) -> Self {
        Self(std::sync::Mutex::new(Box::pin(stream)))
    }
}
impl<S> http_body::Body for FrameBody<S>
where
    S: Stream<Item = Result<http_body::Frame<Bytes>, StreamError>> + Send,
{
    type Data = Bytes;
    type Error = StreamError;
    fn poll_frame(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<http_body::Frame<Bytes>, StreamError>>> {
        match self.0.lock() {
            Ok(mut stream) => stream.as_mut().poll_next(cx),
            Err(_) => std::task::Poll::Ready(Some(Err(StreamError::Filter))),
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::io;
    #[test]
    fn ambiguous_paths_are_rejected() {
        for p in [
            "/a/../admin",
            "/%2e%2e/admin",
            "/%2fadmin",
            "/%252fadmin",
            "//admin",
            "/a\\b",
            "/%00",
            "/%",
        ] {
            assert!(canonical_path(p).is_none(), "{p}");
        }
        assert_eq!(canonical_path("/%61dmin").unwrap(), "/admin");
    }
    #[test]
    fn connection_nominated_headers_are_removed() {
        let mut h = HeaderMap::new();
        h.insert("connection", "x-private, keep-alive".parse().unwrap());
        h.insert("x-private", "sensitive".parse().unwrap());
        h.insert("keep-alive", "10".parse().unwrap());
        h.insert("content-type", "text/plain".parse().unwrap());
        strip_hop_headers(&mut h);
        assert!(!h.contains_key("x-private"));
        assert!(!h.contains_key("connection"));
        assert!(h.contains_key("content-type"));
    }
    #[tokio::test]
    async fn streaming_limit_is_enforced() {
        let s = futures_util::stream::iter(vec![Ok::<_, io::Error>(Bytes::from_static(b"1234"))]);
        let output = constrained(
            s,
            3,
            0,
            Instant::now() + Duration::from_secs(1),
            Arc::new(Metrics::default()),
            false,
        );
        tokio::pin!(output);
        assert!(output.next().await.unwrap().is_err());
    }
}
