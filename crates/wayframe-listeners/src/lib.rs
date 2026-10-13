//! HTTP/1+HTTP/2 listeners with connection admission, header deadlines and stalled-I/O cancellation.
use anyhow::{ensure, Result};
use axum::{body::Body, extract::ConnectInfo, Router};
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use sha2::{Digest, Sha256};
use std::{
    future::Future,
    io,
    net::SocketAddr,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    sync::Semaphore,
    time::{Instant, Sleep},
};
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;

#[derive(Clone, Debug)]
pub struct Limits {
    pub max_connections: usize,
    pub header_timeout: Duration,
    pub io_idle_timeout: Duration,
    pub handshake_timeout: Duration,
    pub drain_timeout: Duration,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            max_connections: 512,
            header_timeout: Duration::from_secs(5),
            io_idle_timeout: Duration::from_secs(15),
            handshake_timeout: Duration::from_secs(5),
            drain_timeout: Duration::from_secs(10),
        }
    }
}
impl Limits {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            (1..=100000).contains(&self.max_connections),
            "invalid connection capacity"
        );
        for d in [
            self.header_timeout,
            self.io_idle_timeout,
            self.handshake_timeout,
            self.drain_timeout,
        ] {
            ensure!(
                !d.is_zero() && d <= Duration::from_secs(600),
                "invalid listener deadline"
            );
        }
        Ok(())
    }
}
#[derive(Clone)]
pub struct Tls {
    acceptor: Arc<arc_swap::ArcSwap<tokio_rustls::TlsAcceptor>>,
}
/// Fingerprint of the verified leaf certificate, present only after mandatory mTLS verification.
#[derive(Clone, Debug)]
pub struct ClientCertificate(pub String);
impl Tls {
    /// Existing connections retain their negotiated keys; new handshakes use the prepared certificate.
    pub fn replace(&self, prepared: &Tls) {
        self.acceptor.store(prepared.acceptor.load_full());
    }
    pub fn from_pem(certificate: &[u8], key: &[u8], client_ca: Option<&[u8]>) -> Result<Self> {
        ensure!(
            certificate.len() <= 65536 && key.len() <= 65536,
            "TLS input too large"
        );
        let certs = rustls_pemfile::certs(&mut &*certificate)
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let key = rustls_pemfile::private_key(&mut &*key)?
            .ok_or_else(|| anyhow::anyhow!("private key required"))?;
        let builder = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()?;
        let builder = if let Some(ca) = client_ca {
            ensure!(ca.len() <= 65536, "client CA too large");
            let mut roots = rustls::RootCertStore::empty();
            for certificate in rustls_pemfile::certs(&mut &*ca) {
                roots.add(certificate?)?;
            }
            builder.with_client_cert_verifier(
                rustls::server::WebPkiClientVerifier::builder_with_provider(
                    Arc::new(roots),
                    Arc::new(rustls::crypto::ring::default_provider()),
                )
                .build()?,
            )
        } else {
            builder.with_no_client_auth()
        };
        let mut config = builder.with_single_cert(certs, key)?;
        config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
        Ok(Self {
            acceptor: Arc::new(arc_swap::ArcSwap::from_pointee(
                tokio_rustls::TlsAcceptor::from(Arc::new(config)),
            )),
        })
    }
}
/// Timers are polled by I/O even while a response body is backpressured and unpolled.
pub struct TimedIo<T> {
    inner: T,
    read: Option<Pin<Box<Sleep>>>,
    write: Option<Pin<Box<Sleep>>>,
    idle: Duration,
}
impl<T> TimedIo<T> {
    pub fn new(inner: T, idle: Duration) -> Self {
        Self {
            inner,
            read: None,
            write: None,
            idle,
        }
    }
    fn stalled(
        timer: &mut Option<Pin<Box<Sleep>>>,
        idle: Duration,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<()>> {
        let timer = timer.get_or_insert_with(|| Box::pin(tokio::time::sleep(idle)));
        match timer.as_mut().poll(cx) {
            Poll::Ready(()) => Poll::Ready(Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "connection I/O idle deadline",
            ))),
            Poll::Pending => Poll::Pending,
        }
    }
}
impl<T: AsyncRead + Unpin> AsyncRead for TimedIo<T> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll_read(cx, buf) {
            Poll::Ready(result) => {
                this.read = None;
                Poll::Ready(result)
            }
            Poll::Pending => Self::stalled(&mut this.read, this.idle, cx),
        }
    }
}
impl<T: AsyncWrite + Unpin> AsyncWrite for TimedIo<T> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll_write(cx, buf) {
            Poll::Ready(r) => {
                this.write = None;
                Poll::Ready(r)
            }
            Poll::Pending => match Self::stalled(&mut this.write, this.idle, cx) {
                Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
                _ => Poll::Pending,
            },
        }
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll_flush(cx) {
            Poll::Ready(r) => {
                this.write = None;
                Poll::Ready(r)
            }
            Poll::Pending => Self::stalled(&mut this.write, this.idle, cx),
        }
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}
async fn connection<T>(
    io: T,
    peer: SocketAddr,
    certificate: Option<ClientCertificate>,
    router: Router,
    limits: Limits,
    stop: CancellationToken,
) where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let terminate = CancellationToken::new();
    let request_terminate = terminate.clone();
    let service =
        hyper::service::service_fn(move |request: hyper::Request<hyper::body::Incoming>| {
            let router = router.clone();
            let certificate = certificate.clone();
            let request_terminate = request_terminate.clone();
            async move {
                let mut request = request.map(Body::new);
                if wayframe_codec::metadata::normalize_h2(&mut request).is_err() {
                    use axum::response::IntoResponse;
                    return Ok(axum::http::StatusCode::BAD_REQUEST.into_response());
                }
                request.extensions_mut().insert(ConnectInfo(peer));
                request
                    .extensions_mut()
                    .insert(wayframe_core::TransportCancellation(Arc::new(move || {
                        request_terminate.cancel()
                    })));
                if let Some(cert) = certificate {
                    request.extensions_mut().insert(cert);
                }
                router.oneshot(request).await
            }
        });
    let mut builder = hyper_util::server::conn::auto::Builder::new(TokioExecutor::new());
    builder
        .http1()
        .timer(TokioTimer::new())
        .header_read_timeout(limits.header_timeout)
        .max_buf_size(65536);
    builder
        .http2()
        .max_concurrent_streams(128)
        .max_header_list_size(65536)
        .max_send_buf_size(65536);
    let connection = builder.serve_connection_with_upgrades(
        TokioIo::new(TimedIo::new(io, limits.io_idle_timeout)),
        service,
    );
    tokio::pin!(connection);
    tokio::select! { _=&mut connection=>{}, _=terminate.cancelled()=>{}, _=stop.cancelled()=>{connection.as_mut().graceful_shutdown();let _=tokio::time::timeout(limits.drain_timeout,connection).await;} }
}
pub async fn serve_tcp<F>(
    listener: tokio::net::TcpListener,
    router: Router,
    limits: Limits,
    tls: Option<Tls>,
    shutdown: F,
) -> Result<()>
where
    F: Future<Output = ()> + Send,
{
    limits.validate()?;
    let slots = Arc::new(Semaphore::new(limits.max_connections));
    let stop = CancellationToken::new();
    let mut tasks = tokio::task::JoinSet::new();
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            _=&mut shutdown=>break,
            _=tasks.join_next(),if !tasks.is_empty()=>{},
            accepted=listener.accept()=>{
                let (mut io,peer)=accepted?;let Ok(permit)=slots.clone().try_acquire_owned() else {let _=tokio::time::timeout(Duration::from_millis(50),tokio::io::AsyncWriteExt::write_all(&mut io,b"HTTP/1.1 503 Service Unavailable\r\nConnection: close\r\nContent-Length: 0\r\n\r\n")).await;continue;};
                let router=router.clone();let limits=limits.clone();let tls=tls.clone();let stop=stop.clone();
                tasks.spawn(async move {let _permit=permit;if let Some(tls)=tls {if let Ok(Ok(io))=tokio::time::timeout(limits.handshake_timeout,tls.acceptor.load_full().accept(io)).await {let certificate=io.get_ref().1.peer_certificates().and_then(|c|c.first()).map(|c|ClientCertificate(hex::encode(Sha256::digest(c.as_ref()))));connection(io,peer,certificate,router,limits,stop).await;}} else {connection(io,peer,None,router,limits,stop).await;}});
            }
        }
    }
    stop.cancel();
    let deadline = Instant::now() + limits.drain_timeout;
    while !tasks.is_empty() {
        if tokio::time::timeout_at(deadline, tasks.join_next())
            .await
            .is_err()
        {
            tasks.abort_all();
            while tasks.join_next().await.is_some() {}
            break;
        }
    }
    Ok(())
}
#[cfg(unix)]
pub async fn serve_unix<F>(
    listener: tokio::net::UnixListener,
    router: Router,
    limits: Limits,
    shutdown: F,
) -> Result<()>
where
    F: Future<Output = ()> + Send,
{
    limits.validate()?;
    let slots = Arc::new(Semaphore::new(limits.max_connections));
    let stop = CancellationToken::new();
    let mut tasks = tokio::task::JoinSet::new();
    tokio::pin!(shutdown);
    loop {
        tokio::select! {_=&mut shutdown=>break,_=tasks.join_next(),if !tasks.is_empty()=>{},accepted=listener.accept()=>{let (io,_)=accepted?;let Ok(permit)=slots.clone().try_acquire_owned() else {continue;};let router=router.clone();let limits=limits.clone();let stop=stop.clone();tasks.spawn(async move {let _permit=permit;connection(io,"127.0.0.1:0".parse().expect("literal"),None,router,limits,stop).await;});}}
    }
    stop.cancel();
    let deadline = Instant::now() + limits.drain_timeout;
    while !tasks.is_empty() {
        if tokio::time::timeout_at(deadline, tasks.join_next())
            .await
            .is_err()
        {
            tasks.abort_all();
            while tasks.join_next().await.is_some() {}
            break;
        }
    }
    Ok(())
}
