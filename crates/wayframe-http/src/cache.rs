//! Cache hooks run only after all request policy checks. Cache implementations own explicit scope.
use crate::RequestContext;
use std::{future::Future, pin::Pin, sync::Arc};
use wayframe_core::Decision;
pub type Ticket = Arc<dyn std::any::Any + Send + Sync>;
pub enum Access {
    Bypass,
    Hit(bytes::Bytes),
    Miss(Ticket),
}
pub type Lookup<'a> = Pin<Box<dyn Future<Output = Result<Access, Decision>> + Send + 'a>>;
pub trait ResponseCache: Send + Sync + 'static {
    fn stats(&self) -> serde_json::Value {
        serde_json::json!({})
    }
    fn begin<'a>(&'a self, context: &'a RequestContext) -> Lookup<'a>;
    /// Called only after successful EOF, with bounded, filtered JSON bytes. Errors never become hits.
    fn store(
        &self,
        ticket: &Ticket,
        status: http::StatusCode,
        headers: &http::HeaderMap,
        bytes: &[u8],
    );
}
pub struct CacheHooks(pub Arc<dyn ResponseCache>);
