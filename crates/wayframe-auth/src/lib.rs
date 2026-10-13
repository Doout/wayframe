//! Resource-bound RS256 bearer validation with bounded JWKS refresh and trusted permission mapping.
use anyhow::{ensure, Result};
use jsonwebtoken::{jwk::JwkSet, Algorithm, DecodingKey, Validation};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeSet, HashMap},
    sync::Arc,
    time::Duration,
};
use tokio::{
    sync::{Mutex, RwLock},
    time::Instant,
};
use wayframe_core::{Block, Decision, Execution, Phase, Reason};
use wayframe_http::{Identity, RequestContext};
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub issuer: String,
    pub audience: String,
    pub jwks_uri: String,
    pub tenant_claim: String,
    pub permissions_claim: String,
    pub permission_map: HashMap<String, String>,
    #[serde(default = "refresh")]
    pub refresh_seconds: u64,
    #[serde(default)]
    pub ca: Option<String>,
}
fn refresh() -> u64 {
    300
}
fn trusted_url(raw: &str) -> Result<reqwest::Url> {
    let url: reqwest::Url = raw.parse()?;
    let loopback = url
        .host_str()
        .is_some_and(|h| matches!(h, "localhost" | "127.0.0.1" | "[::1]"));
    ensure!(
        (url.scheme() == "https" || (url.scheme() == "http" && loopback))
            && url.host_str().is_some()
            && url.username().is_empty()
            && url.password().is_none()
            && url.fragment().is_none()
            && url.query().is_none(),
        "identity endpoint must use HTTPS (HTTP is loopback-only)"
    );
    Ok(url)
}
struct Cache {
    keys: HashMap<String, Arc<DecodingKey>>,
    expires: Instant,
    next_refresh: Instant,
}
pub struct JwksAuth {
    id: String,
    config: Config,
    client: reqwest::Client,
    validation: Validation,
    cache: RwLock<Cache>,
    refresh: Mutex<()>,
    background: std::sync::Mutex<Option<tokio::task::AbortHandle>>,
}
impl JwksAuth {
    pub async fn prepare(id: impl Into<String>, config: Config) -> Result<Arc<Self>> {
        trusted_url(&config.issuer)?;
        trusted_url(&config.jwks_uri)?;
        ensure!(
            !config.audience.is_empty()
                && config.audience.len() <= 2048
                && (1..=3600).contains(&config.refresh_seconds)
                && !config.tenant_claim.is_empty()
                && !config.permissions_claim.is_empty()
                && config.permission_map.len() <= 128
                && config.permission_map.iter().all(|(k, v)| !k.is_empty()
                    && k.len() <= 256
                    && !v.is_empty()
                    && v.len() <= 256),
            "invalid identity configuration"
        );
        let mut client = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(5));
        if let Some(path) = &config.ca {
            ensure!(
                std::fs::metadata(path)?.len() <= 65536,
                "identity CA too large"
            );
            client =
                client.add_root_certificate(reqwest::Certificate::from_pem(&std::fs::read(path)?)?);
        }
        let mut validation = Validation::new(Algorithm::RS256);
        validation.set_issuer(&[&config.issuer]);
        validation.set_audience(&[&config.audience]);
        validation.set_required_spec_claims(&["exp", "iss", "aud", "sub"]);
        validation.validate_nbf = true;
        validation.leeway = 30;
        let now = Instant::now();
        let auth = Arc::new(Self {
            id: id.into(),
            config,
            client: client.build()?,
            validation,
            cache: RwLock::new(Cache {
                keys: HashMap::new(),
                expires: now,
                next_refresh: now,
            }),
            refresh: Mutex::new(()),
            background: std::sync::Mutex::new(None),
        });
        auth.refresh().await?;
        let weak = Arc::downgrade(&auth);
        let interval = Duration::from_secs((auth.config.refresh_seconds / 2).max(1));
        let task = tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                let Some(auth) = weak.upgrade() else {
                    return;
                };
                let _ = auth.refresh().await;
            }
        });
        *auth
            .background
            .lock()
            .map_err(|_| anyhow::anyhow!("refresh lifecycle poisoned"))? =
            Some(task.abort_handle());
        Ok(auth)
    }
    pub async fn refresh(&self) -> Result<()> {
        let _lock = self.refresh.lock().await;
        let now = Instant::now();
        if now < self.cache.read().await.next_refresh {
            return Ok(());
        }
        // Invalid kids cannot make each request perform remote discovery.
        self.cache.write().await.next_refresh = now + Duration::from_secs(1);
        let mut response = self
            .client
            .get(&self.config.jwks_uri)
            .send()
            .await?
            .error_for_status()?;
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await? {
            ensure!(
                bytes.len().saturating_add(chunk.len()) <= 65536,
                "JWKS response too large"
            );
            bytes.extend_from_slice(&chunk);
        }
        let set: JwkSet = serde_json::from_slice(&bytes)?;
        ensure!(
            !set.keys.is_empty() && set.keys.len() <= 64,
            "invalid JWKS key count"
        );
        let mut keys = HashMap::new();
        for key in set.keys {
            let Some(kid) = key.common.key_id.clone() else {
                continue;
            };
            if key
                .common
                .key_algorithm
                .is_some_and(|a| a != jsonwebtoken::jwk::KeyAlgorithm::RS256)
            {
                continue;
            }
            if key
                .common
                .public_key_use
                .as_ref()
                .is_some_and(|usage| !matches!(usage, jsonwebtoken::jwk::PublicKeyUse::Signature))
            {
                continue;
            }
            if key
                .common
                .key_operations
                .as_ref()
                .is_some_and(|ops| !ops.contains(&jsonwebtoken::jwk::KeyOperations::Verify))
            {
                continue;
            }
            if !matches!(
                key.algorithm,
                jsonwebtoken::jwk::AlgorithmParameters::RSA(_)
            ) {
                continue;
            }
            ensure!(!kid.is_empty() && kid.len() <= 128, "invalid key id");
            ensure!(!keys.contains_key(&kid), "duplicate key id");
            keys.insert(kid, Arc::new(DecodingKey::from_jwk(&key)?));
        }
        ensure!(!keys.is_empty(), "JWKS contains no usable RSA keys");
        let mut cache = self.cache.write().await;
        cache.keys = keys;
        cache.expires = now + Duration::from_secs(self.config.refresh_seconds);
        Ok(())
    }
    pub async fn authenticate(&self, ctx: &mut RequestContext) -> Decision {
        ctx.identity = None;
        ctx.extensions.remove::<wayframe_http::VerifiedIssuer>();
        let denied = || Decision::deny(Reason::Unauthenticated, "invalid_access_token");
        let mut auth = ctx.headers.get_all("authorization").iter();
        let token = auth
            .next()
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.split_once(' '))
            .filter(|(scheme, token)| {
                scheme.eq_ignore_ascii_case("bearer")
                    && token.len() <= 8192
                    && !token.is_empty()
                    && !token.contains(char::is_whitespace)
            });
        let Some((_, token)) = token else {
            return denied();
        };
        if auth.next().is_some() {
            return denied();
        }
        let Ok(header) = jsonwebtoken::decode_header(token) else {
            return denied();
        };
        if header.alg != Algorithm::RS256 {
            return denied();
        }
        let Some(kid) = header.kid.filter(|k| !k.is_empty() && k.len() <= 128) else {
            return denied();
        };
        let needs_refresh = {
            let cache = self.cache.read().await;
            cache.expires <= Instant::now() || !cache.keys.contains_key(&kid)
        };
        if needs_refresh && self.refresh().await.is_err() {
            return denied();
        }
        let key = {
            let cache = self.cache.read().await;
            if cache.expires <= Instant::now() {
                return denied();
            }
            cache.keys.get(&kid).cloned()
        };
        let Some(key) = key else {
            return denied();
        };
        use base64::Engine;
        if token
            .split('.')
            .nth(1)
            .and_then(|payload| {
                base64::engine::general_purpose::URL_SAFE_NO_PAD
                    .decode(payload)
                    .ok()
            })
            .is_none_or(|bytes| wayframe_codec::parse_unique(&bytes).is_err())
        {
            return denied();
        }
        let Ok(token) = jsonwebtoken::decode::<serde_json::Value>(token, &key, &self.validation)
        else {
            return denied();
        };
        let claims = token.claims;
        let subject = claims.get("sub").and_then(serde_json::Value::as_str);
        let tenant = claims
            .get(&self.config.tenant_claim)
            .and_then(serde_json::Value::as_str);
        let (Some(subject), Some(tenant)) = (subject, tenant) else {
            return denied();
        };
        if subject.is_empty()
            || tenant.is_empty()
            || subject.len() > 256
            || tenant.len() > 256
            || subject.chars().any(char::is_control)
            || tenant.chars().any(char::is_control)
        {
            return denied();
        }
        let scopes: Vec<&str> = match claims.get(&self.config.permissions_claim) {
            Some(serde_json::Value::String(s)) => s.split_whitespace().collect(),
            Some(serde_json::Value::Array(values)) => {
                if values.len() > 128 || values.iter().any(|v| !v.is_string()) {
                    return denied();
                }
                values
                    .iter()
                    .filter_map(serde_json::Value::as_str)
                    .collect()
            }
            None => vec![],
            _ => return denied(),
        };
        if scopes.len() > 128 || scopes.iter().any(|s| s.len() > 256) {
            return denied();
        }
        let permissions: BTreeSet<_> = scopes
            .into_iter()
            .filter_map(|s| self.config.permission_map.get(s).cloned())
            .collect();
        ctx.identity = Some(Identity::new(subject, tenant).with_permissions(permissions));
        ctx.extensions
            .insert(wayframe_http::VerifiedIssuer(self.config.issuer.clone()));
        Decision::Continue
    }
    pub async fn readiness(&self) -> bool {
        let cache = self.cache.read().await;
        !cache.keys.is_empty() && cache.expires > Instant::now()
    }
}
impl Drop for JwksAuth {
    fn drop(&mut self) {
        if let Ok(background) = self.background.lock() {
            if let Some(task) = &*background {
                task.abort();
            }
        }
    }
}
impl Block<RequestContext> for JwksAuth {
    fn id(&self) -> &str {
        &self.id
    }
    fn phase(&self) -> Phase {
        Phase::Authenticate
    }
    fn execute<'a>(&'a self, ctx: &'a mut RequestContext) -> Execution<'a> {
        Execution::Deferred(Box::pin(self.authenticate(ctx)))
    }
}
#[derive(Clone, Serialize)]
pub struct ProtectedResource {
    pub resource: String,
    pub authorization_servers: Vec<String>,
    pub scopes_supported: Vec<String>,
    pub bearer_methods_supported: Vec<String>,
}
impl ProtectedResource {
    pub fn new(resource: String, issuers: Vec<String>, scopes: Vec<String>) -> Result<Self> {
        trusted_url(&resource)?;
        ensure!(
            !issuers.is_empty()
                && issuers.len() <= 16
                && scopes.len() <= 128
                && scopes
                    .iter()
                    .all(|s| !s.is_empty() && s.len() <= 256 && !s.chars().any(char::is_control)),
            "invalid resource metadata"
        );
        for issuer in &issuers {
            trusted_url(issuer)?;
        }
        Ok(Self {
            resource,
            authorization_servers: issuers,
            scopes_supported: scopes,
            bearer_methods_supported: vec!["header".into()],
        })
    }
    pub fn challenge(&self, scope: Option<&str>) -> Result<String> {
        let resource = trusted_url(&self.resource)?;
        let metadata = format!(
            "{}/.well-known/oauth-protected-resource{}",
            resource.origin().ascii_serialization(),
            resource.path().trim_end_matches('/')
        );
        ensure!(
            !metadata.contains(['"', '\\', '\r', '\n']),
            "invalid resource URL"
        );
        let mut value = format!("Bearer resource_metadata=\"{metadata}\"");
        if let Some(scope) = scope {
            ensure!(
                !scope.contains(['"', '\\', '\r', '\n']),
                "invalid challenge scope"
            );
            value.push_str(&format!(", scope=\"{scope}\""));
        }
        Ok(value)
    }
}

pub mod upstream;
