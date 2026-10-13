//! Registered destinations. No client-provided URL can become a forwarding target.
use anyhow::{ensure, Result};
use std::time::Duration;

pub struct Upstream {
    pub id: String,
    pub endpoint: String,
    pub bearer: Option<String>,
    pub provider: Option<std::sync::Arc<dyn crate::credentials::Provider>>,
    pub ca: Option<Vec<u8>>,
    pub http2_prior_knowledge: bool,
}
pub(crate) struct Prepared {
    pub endpoint: reqwest::Url,
    pub client: reqwest::Client,
    pub bearer: Option<String>,
    pub provider: Option<std::sync::Arc<dyn crate::credentials::Provider>>,
}
impl Upstream {
    pub fn new(id: impl Into<String>, endpoint: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            endpoint: endpoint.into(),
            bearer: None,
            provider: None,
            ca: None,
            http2_prior_knowledge: false,
        }
    }
    pub(crate) fn prepare(self) -> Result<(String, Prepared)> {
        ensure!(
            !self.id.is_empty()
                && self.id.len() <= 64
                && self
                    .id
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b)),
            "invalid backend id"
        );
        let endpoint: reqwest::Url = self.endpoint.parse()?;
        ensure!(
            matches!(endpoint.scheme(), "http" | "https")
                && endpoint.host_str().is_some()
                && endpoint.username().is_empty()
                && endpoint.password().is_none()
                && endpoint.query().is_none()
                && endpoint.fragment().is_none()
                && crate::stream::canonical_path(endpoint.path()).as_deref()
                    == Some(endpoint.path()),
            "invalid backend endpoint"
        );
        if let Some(token) = &self.bearer {
            ensure!(
                (32..=8192).contains(&token.len())
                    && token.bytes().all(|b| (0x21..=0x7e).contains(&b)),
                "invalid backend credential"
            );
        }
        let mut client = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(3))
            .pool_max_idle_per_host(32);
        if self.http2_prior_knowledge {
            client = client.http2_prior_knowledge();
        }
        if let Some(ca) = self.ca {
            ensure!(ca.len() <= 64 * 1024, "backend CA too large");
            client = client.add_root_certificate(reqwest::Certificate::from_pem(&ca)?);
        }
        Ok((
            self.id,
            Prepared {
                endpoint,
                client: client.build()?,
                bearer: self.bearer,
                provider: self.provider,
            },
        ))
    }
}
#[derive(Clone)]
pub struct BackendId(pub String);
