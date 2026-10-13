use axum::{
    body::Body,
    http::{HeaderMap, Request},
    response::Response,
    routing::post,
    Router,
};
use bytes::Bytes;
use http_body::Frame;
use http_body_util::{BodyExt, StreamBody};
use std::{convert::Infallible, sync::Arc};
use wayframe_http::HttpGateway;
use wayframe_runtime::Pipeline;
fn framed() -> Body {
    let mut trailers = HeaderMap::new();
    trailers.insert("grpc-status", "13".parse().unwrap());
    Body::new(StreamBody::new(futures_util::stream::iter([
        Ok::<_, Infallible>(Frame::data(Bytes::from_static(b"grpc-payload"))),
        Ok(Frame::trailers(trailers)),
    ])))
}
#[tokio::test]
async fn real_http2_round_trip_preserves_request_and_response_trailers() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let observed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let saved = observed.clone();
    let router = Router::new().route(
        "/grpc",
        post(move |request: Request<Body>| {
            let saved = saved.clone();
            async move {
                let collected = request.into_body().collect().await.unwrap();
                assert_eq!(collected.trailers().unwrap()["grpc-status"], "13");
                saved.store(true, std::sync::atomic::Ordering::Relaxed);
                let mut response = Response::new(framed());
                response
                    .headers_mut()
                    .insert("content-type", "application/grpc".parse().unwrap());
                response
            }
        }),
    );
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let gateway = HttpGateway::builder(origin)
        .upstream_http2(true)
        .preflight(Pipeline::compile(vec![]).unwrap())
        .build()
        .unwrap();
    let response = gateway
        .handle(
            "127.0.0.1:1".parse().unwrap(),
            Request::builder()
                .method("POST")
                .uri("/grpc")
                .header("content-type", "application/grpc")
                .body(framed())
                .unwrap(),
        )
        .await;
    assert_eq!(response.status(), 200);
    let collected = response.into_body().collect().await.unwrap();
    assert_eq!(collected.trailers().unwrap()["grpc-status"], "13");
    assert_eq!(collected.to_bytes(), Bytes::from_static(b"grpc-payload"));
    assert!(observed.load(std::sync::atomic::Ordering::Relaxed));
    assert_eq!(gateway.metrics().snapshot()["active"], 0);
    server.abort();
}

struct RemoveSecret;
impl wayframe_http::ResponseFilter for RemoveSecret {
    fn id(&self) -> &str {
        "remove-secret"
    }
    fn records(&self, _: &wayframe_http::ResponseContext) -> bool {
        true
    }
    fn record(
        &self,
        _: &wayframe_http::ResponseContext,
        value: &mut serde_json::Value,
    ) -> wayframe_core::Decision {
        value.as_object_mut().unwrap().remove("secret");
        wayframe_core::Decision::Continue
    }
}
#[tokio::test]
async fn transformed_http2_json_keeps_safe_trailers_and_removes_stale_validators() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let router = Router::new().route(
        "/json",
        post(|request: Request<Body>| async move {
            assert_eq!(request.headers()["accept-encoding"], "identity");
            let mut trailers = HeaderMap::new();
            trailers.insert("x-completion", "done".parse().unwrap());
            trailers.insert("content-digest", "stale".parse().unwrap());
            let mut response =
                Response::new(Body::new(StreamBody::new(futures_util::stream::iter([
                    Ok::<_, Infallible>(Frame::data(Bytes::from_static(
                        br#"{"visible":1,"secret":2}"#,
                    ))),
                    Ok(Frame::trailers(trailers)),
                ]))));
            for (name, value) in [
                ("content-type", "application/json"),
                ("etag", "old"),
                ("digest", "stale"),
                ("accept-ranges", "bytes"),
            ] {
                response.headers_mut().insert(name, value.parse().unwrap());
            }
            response
        }),
    );
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let gateway = HttpGateway::builder(origin)
        .upstream_http2(true)
        .preflight(Pipeline::compile(vec![]).unwrap())
        .response_filters(vec![Arc::new(RemoveSecret)])
        .build()
        .unwrap();
    let response = gateway
        .handle(
            "127.0.0.1:1".parse().unwrap(),
            Request::builder()
                .method("POST")
                .uri("/json")
                .header("accept-encoding", "gzip")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
    assert_eq!(response.status(), 200);
    for name in ["etag", "digest", "accept-ranges", "content-length"] {
        assert!(!response.headers().contains_key(name));
    }
    let collected = response.into_body().collect().await.unwrap();
    assert_eq!(collected.trailers().unwrap()["x-completion"], "done");
    assert!(!collected.trailers().unwrap().contains_key("content-digest"));
    assert_eq!(
        collected.to_bytes(),
        Bytes::from_static(br#"{"visible":1}"#)
    );
    assert_eq!(gateway.metrics().snapshot()["active"], 0);
    server.abort();
}
