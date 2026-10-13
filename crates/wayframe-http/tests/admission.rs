use axum::{
    body::Body, extract::ConnectInfo, http::Request, response::Response, routing::get, Router,
};
use bytes::Bytes;
use http_body_util::Empty;
use std::{net::SocketAddr, time::Duration};
use wayframe_http::{HttpGateway, Limits};
use wayframe_runtime::Pipeline;
#[tokio::test]
async fn stalled_http2_stream_releases_admission_even_while_connection_is_active() {
    let upstream = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", upstream.local_addr().unwrap());
    let backend = tokio::spawn(async move {
        axum::serve(
            upstream,
            Router::new()
                .route(
                    "/large",
                    get(|| async {
                        Response::new(Body::from_stream(futures_util::stream::repeat(Ok::<
                            _,
                            std::convert::Infallible,
                        >(
                            Bytes::from(vec![1; 8192]),
                        ))))
                    }),
                )
                .route("/small", get(|| async { "ok" })),
        )
        .await
        .unwrap()
    });
    let gateway = HttpGateway::builder(origin)
        .preflight(Pipeline::compile(vec![]).unwrap())
        .limits(Limits {
            max_in_flight: 1,
            request_timeout: Duration::from_millis(200),
            ..Default::default()
        })
        .build()
        .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let shared = gateway.clone();
    let router = Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .fallback(
            move |ConnectInfo(peer): ConnectInfo<SocketAddr>, request: Request<Body>| {
                let gateway = shared.clone();
                async move { gateway.handle(peer, request).await }
            },
        );
    let frontend = tokio::spawn(wayframe_listeners::serve_tcp(
        listener,
        router,
        wayframe_listeners::Limits {
            io_idle_timeout: Duration::from_secs(5),
            ..Default::default()
        },
        None,
        std::future::pending(),
    ));
    let io = tokio::net::TcpStream::connect(address).await.unwrap();
    let mut builder =
        hyper::client::conn::http2::Builder::new(hyper_util::rt::TokioExecutor::new());
    builder.initial_stream_window_size(1024);
    let (mut sender, connection) = builder
        .handshake::<_, Empty<Bytes>>(hyper_util::rt::TokioIo::new(io))
        .await
        .unwrap();
    let connection = tokio::spawn(connection);
    let response = sender
        .send_request(
            Request::builder()
                .uri(format!("http://{address}/large"))
                .body(Empty::new())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    // Do not read the large body. Keep other traffic on the connection so an idle-I/O timer alone cannot fix this.
    for _ in 0..3 {
        let response = sender
            .send_request(
                Request::builder()
                    .uri(format!("http://{address}/healthz"))
                    .body(Empty::new())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(gateway.metrics().snapshot()["active"], 0);
    let next = reqwest::get(format!("http://{address}/small"))
        .await
        .unwrap();
    assert_eq!(next.status(), 200);
    assert_eq!(next.text().await.unwrap(), "ok");
    drop(response);
    connection.abort();
    frontend.abort();
    backend.abort();
}
