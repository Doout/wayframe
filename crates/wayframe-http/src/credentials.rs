//! Gateway-to-service credentials are independent of authenticated client credentials.
use std::{future::Future, pin::Pin, sync::Arc};
pub type Credential<'a> = Pin<Box<dyn Future<Output = anyhow::Result<String>> + Send + 'a>>;
pub trait Provider: Send + Sync + 'static {
    fn bearer(&self, deadline: tokio::time::Instant) -> Credential<'_>;
}
pub struct Hooks(pub Arc<dyn Provider>);
