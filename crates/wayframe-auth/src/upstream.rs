//! Client-credentials tokens from an explicitly configured authorization server. No caller token is exchanged.
use anyhow::{ensure, Result};
use serde::Deserialize;
use std::{path::PathBuf, sync::Arc, time::Duration};
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub token_endpoint: String,
    pub client_id: String,
    pub secret_file: PathBuf,
    pub resource: String,
    #[serde(default)]
    pub scopes: Vec<String>,
    pub ca: Option<PathBuf>,
}
pub struct OAuth {
    config: Config,
    client: reqwest::Client,
    token: tokio::sync::Mutex<Option<(String, tokio::time::Instant)>>,
}
impl Config {
    pub fn secret(&self) -> Result<String> {
        let meta = self.secret_file.symlink_metadata()?;
        ensure!(
            self.secret_file.is_absolute()
                && meta.is_file()
                && !meta.file_type().is_symlink()
                && meta.len() <= 8192,
            "invalid client secret file"
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            ensure!(
                meta.permissions().mode() & 0o077 == 0,
                "client secret file must be private"
            );
        }
        let secret = std::fs::read_to_string(&self.secret_file)?
            .trim_end_matches(['\n', '\r'])
            .to_owned();
        ensure!(
            (32..=8192).contains(&secret.len())
                && secret.bytes().all(|b| (0x21..=0x7e).contains(&b)),
            "invalid client secret"
        );
        Ok(secret)
    }
}
impl OAuth {
    pub async fn prepare(config: Config) -> Result<Arc<Self>> {
        let endpoint = super::trusted_url(&config.token_endpoint)?;
        ensure!(
            endpoint.scheme() == "https",
            "OAuth token endpoint requires HTTPS"
        );
        super::trusted_url(&config.resource)?;
        ensure!(
            !config.client_id.is_empty()
                && config.client_id.len() <= 256
                && config.scopes.len() <= 128
                && config
                    .scopes
                    .iter()
                    .all(|s| !s.is_empty() && s.len() <= 256 && !s.contains(char::is_whitespace)),
            "invalid OAuth client"
        );
        config.secret()?;
        let mut builder = reqwest::Client::builder()
            .no_proxy()
            .https_only(true)
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(5));
        if let Some(path) = &config.ca {
            ensure!(path.metadata()?.len() <= 65536, "CA too large");
            builder = builder
                .add_root_certificate(reqwest::Certificate::from_pem(&std::fs::read(path)?)?);
        }
        let auth = Arc::new(Self {
            config,
            client: builder.build()?,
            token: tokio::sync::Mutex::new(None),
        });
        use wayframe_http::credentials::Provider;
        auth.bearer(tokio::time::Instant::now() + Duration::from_secs(5))
            .await?;
        Ok(auth)
    }
}
impl wayframe_http::credentials::Provider for OAuth {
    fn bearer(&self, deadline: tokio::time::Instant) -> wayframe_http::credentials::Credential<'_> {
        Box::pin(async move {
            tokio::time::timeout_at(deadline, async {
                let mut cached = self.token.lock().await;
                let now = tokio::time::Instant::now();
                if let Some((token, until)) = &*cached {
                    if *until > now {
                        return Ok(token.clone());
                    }
                }
                let secret = self.config.secret()?;
                let scopes = self.config.scopes.join(" ");
                let parameters = [
                    ("grant_type", "client_credentials"),
                    ("client_id", self.config.client_id.as_str()),
                    ("client_secret", secret.as_str()),
                    ("resource", self.config.resource.as_str()),
                    ("scope", scopes.as_str()),
                ];
                let mut response = self
                    .client
                    .post(&self.config.token_endpoint)
                    .form(&parameters)
                    .send()
                    .await?
                    .error_for_status()?;
                let mut bytes = Vec::new();
                while let Some(chunk) = response.chunk().await? {
                    ensure!(
                        bytes.len().saturating_add(chunk.len()) <= 16384,
                        "token response too large"
                    );
                    bytes.extend_from_slice(&chunk);
                }
                let value = serde_json::from_slice::<serde_json::Value>(&bytes)?;
                let token = value["access_token"]
                    .as_str()
                    .ok_or_else(|| anyhow::anyhow!("access token missing"))?;
                let lifetime = value["expires_in"]
                    .as_u64()
                    .ok_or_else(|| anyhow::anyhow!("token expiry required"))?;
                ensure!(
                    value["token_type"]
                        .as_str()
                        .is_some_and(|t| t.eq_ignore_ascii_case("Bearer"))
                        && (1..=86400).contains(&lifetime)
                        && (32..=8192).contains(&token.len())
                        && token.bytes().all(|b| (0x21..=0x7e).contains(&b)),
                    "invalid service token"
                );
                let token = token.to_owned();
                *cached = Some((
                    token.clone(),
                    tokio::time::Instant::now()
                        + Duration::from_secs(
                            lifetime.saturating_sub((lifetime / 10).clamp(1, 30)),
                        ),
                ));
                Ok(token)
            })
            .await?
        })
    }
}
