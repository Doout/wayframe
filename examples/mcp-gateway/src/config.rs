use anyhow::{ensure, Context as _, Result};
use serde::Deserialize;
use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    path::Path,
    time::Duration,
};
use wayframe::http::{stream::canonical_path, Identity, Limits};

pub struct Credentials {
    pub clients: Vec<(String, Identity)>,
    pub upstream_token: Option<String>,
    pub backend_tokens: HashMap<String, String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Client {
    pub token_env: String,
    pub subject: String,
    pub tenant: String,
    pub permissions: Vec<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub listen: SocketAddr,
    pub upstream: String,
    pub upstream_token_env: Option<String>,
    pub upstream_ca: Option<String>,
    pub load_balancer: Option<LoadBalancerConfig>,
    #[serde(default)]
    pub allowed_origins: Vec<String>,
    #[serde(default)]
    pub allowed_hosts: Vec<String>,
    pub clients: Vec<Client>,
    pub tools: HashMap<String, String>,
    #[serde(default = "default_rate")]
    pub requests_per_second: f64,
    #[serde(default = "default_burst")]
    pub burst: u32,
    #[serde(default = "default_in_flight")]
    pub max_in_flight: usize,
    #[serde(default = "default_request")]
    pub max_request_bytes: usize,
    #[serde(default = "default_response")]
    pub max_response_bytes: u64,
    #[serde(default = "default_timeout")]
    pub timeout_seconds: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LoadBalancerConfig {
    pub strategy: wayframe::lb::Strategy,
    pub backends: Vec<BackendConfig>,
    #[serde(default = "default_cooldown")]
    pub failure_cooldown_ms: u64,
}
fn default_cooldown() -> u64 {
    1000
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackendConfig {
    pub id: String,
    pub endpoint: String,
    pub token_env: Option<String>,
    pub ca: Option<String>,
    #[serde(default = "default_weight")]
    pub weight: u32,
    #[serde(default)]
    pub quota: u32,
    #[serde(default)]
    pub priority: u32,
    #[serde(default = "default_enabled")]
    pub enabled: bool,
}
fn default_weight() -> u32 {
    1
}
fn default_enabled() -> bool {
    true
}
impl BackendConfig {
    pub fn metadata(&self) -> wayframe::lb::Backend {
        wayframe::lb::Backend {
            id: self.id.clone(),
            weight: self.weight,
            quota: self.quota,
            priority: self.priority,
            enabled: self.enabled,
        }
    }
}
impl LoadBalancerConfig {
    pub fn pool(&self) -> Result<wayframe::lb::Pool> {
        wayframe::lb::Pool::new(self.backends.iter().map(BackendConfig::metadata).collect())
            .map_err(anyhow::Error::msg)
    }
}
fn default_rate() -> f64 {
    100.0
}
fn default_burst() -> u32 {
    200
}
fn default_in_flight() -> usize {
    128
}
fn default_request() -> usize {
    32768
}
fn default_response() -> u64 {
    1024 * 1024
}
fn default_timeout() -> u64 {
    15
}

pub fn secret(name: &str) -> Result<String> {
    ensure!(
        !name.is_empty()
            && name.len() <= 128
            && name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_'),
        "invalid credential environment name"
    );
    let value = std::env::var(name)
        .with_context(|| format!("set credential environment variable {name}"))?;
    ensure!(
        (32..=8192).contains(&value.len()) && value.bytes().all(|b| (0x21..=0x7e).contains(&b)),
        "invalid credential in {name}"
    );
    Ok(value)
}
impl Config {
    pub fn load() -> Result<Self> {
        let mut config = if let Ok(path) = std::env::var("WAYFRAME_CONFIG") {
            let metadata = std::fs::metadata(&path)?;
            ensure!(
                metadata.is_file() && metadata.len() <= 64 * 1024,
                "configuration must be a regular file under 64 KiB"
            );
            toml::from_str(&std::fs::read_to_string(path)?)?
        } else {
            Self::demo()?
        };
        if config.allowed_origins.is_empty() {
            config.allowed_origins = vec![
                format!("http://127.0.0.1:{}", config.listen.port()),
                format!("http://localhost:{}", config.listen.port()),
            ];
        }
        if config.allowed_hosts.is_empty() {
            config.allowed_hosts = vec![
                format!("127.0.0.1:{}", config.listen.port()),
                format!("localhost:{}", config.listen.port()),
                "localhost".into(),
            ];
        }
        config.validate()?;
        Ok(config)
    }
    fn demo() -> Result<Self> {
        let origin =
            std::env::var("WAYFRAME_UPSTREAM").unwrap_or_else(|_| "http://127.0.0.1:9001".into());
        Ok(Self {
            listen: std::env::var("WAYFRAME_LISTEN")
                .unwrap_or_else(|_| "127.0.0.1:8080".into())
                .parse()?,
            upstream: format!("{}/mcp", origin.trim_end_matches('/')),
            upstream_token_env: Some("WAYFRAME_UPSTREAM_TOKEN".into()),
            upstream_ca: None,
            load_balancer: None,
            allowed_origins: vec![],
            allowed_hosts: vec![],
            clients: vec![
                Client {
                    token_env: "WAYFRAME_READER_TOKEN".into(),
                    subject: "reader".into(),
                    tenant: "demo".into(),
                    permissions: vec!["tools:read".into()],
                },
                Client {
                    token_env: "WAYFRAME_OPERATOR_TOKEN".into(),
                    subject: "operator".into(),
                    tenant: "demo".into(),
                    permissions: vec!["tools:read".into(), "tools:admin".into()],
                },
            ],
            tools: HashMap::from([
                ("add".into(), "tools:read".into()),
                ("search_docs".into(), "tools:read".into()),
                ("admin_preview".into(), "tools:admin".into()),
            ]),
            requests_per_second: 10.0,
            burst: 20,
            max_in_flight: default_in_flight(),
            max_request_bytes: default_request(),
            max_response_bytes: default_response(),
            timeout_seconds: default_timeout(),
        })
    }
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.listen.ip().is_loopback(),
            "bind to loopback and use a TLS ingress for remote clients"
        );
        ensure!(
            !self.clients.is_empty() && self.clients.len() <= 128,
            "configure 1..128 clients"
        );
        ensure!(
            self.allowed_hosts.len() <= 64
                && self
                    .allowed_hosts
                    .iter()
                    .all(|h| !h.is_empty() && h.len() <= 256 && !h.contains('*')),
            "invalid host allowlist"
        );
        ensure!(
            (1024..=1024 * 1024).contains(&self.max_request_bytes),
            "invalid request limit"
        );
        ensure!(
            self.max_response_bytes > 0 && self.max_response_bytes <= 64 * 1024 * 1024,
            "invalid response limit"
        );
        ensure!((1..=120).contains(&self.timeout_seconds), "invalid timeout");
        let mut subjects = HashSet::new();
        for client in &self.clients {
            ensure!(
                !client.subject.is_empty()
                    && client.subject.len() <= 256
                    && !client.tenant.is_empty()
                    && client.tenant.len() <= 256,
                "invalid client identity"
            );
            ensure!(
                subjects.insert((&client.tenant, &client.subject)),
                "duplicate client identity"
            );
            ensure!(
                client.permissions.len() <= 128
                    && client
                        .permissions
                        .iter()
                        .all(|p| !p.is_empty() && p.len() <= 256),
                "invalid client permissions"
            );
        }
        self.endpoint()?;
        if let Some(lb) = &self.load_balancer {
            lb.pool()?;
            ensure!(
                lb.failure_cooldown_ms <= 600_000,
                "invalid backend cooldown"
            );
            for backend in &lb.backends {
                endpoint(&backend.endpoint)?;
            }
        }
        Ok(())
    }
    pub fn endpoint(&self) -> Result<(String, String)> {
        endpoint(&self.upstream)
    }
    pub fn credentials(&self) -> Result<Credentials> {
        let restored =
            std::env::var_os("WAYFRAME_STATE_FILE").is_some_and(|p| Path::new(&p).exists());
        let mut seen = HashSet::new();
        let mut clients = Vec::new();
        for c in self.clients.iter().filter(|_| !restored) {
            let token = secret(&c.token_env)?;
            ensure!(
                seen.insert(token.clone()),
                "client credentials must be distinct"
            );
            clients.push((
                token,
                Identity::new(&c.subject, &c.tenant)
                    .with_permissions(c.permissions.iter().cloned()),
            ));
        }
        let upstream = self.upstream_token_env.as_deref().map(secret).transpose()?;
        if let Some(token) = &upstream {
            ensure!(
                !seen.contains(token),
                "upstream credential must differ from client credentials"
            );
        }
        let mut backend_tokens = HashMap::new();
        if let Some(lb) = &self.load_balancer {
            for backend in &lb.backends {
                if let Some(name) = &backend.token_env {
                    let token = secret(name)?;
                    ensure!(
                        !seen.contains(&token),
                        "backend credential must differ from client credentials"
                    );
                    backend_tokens.insert(backend.id.clone(), token);
                }
            }
        }
        Ok(Credentials {
            clients,
            upstream_token: upstream,
            backend_tokens,
        })
    }
    pub fn ca(&self) -> Result<Option<Vec<u8>>> {
        self.upstream_ca
            .as_deref()
            .map(|path| {
                let p = Path::new(path);
                ensure!(p.metadata()?.len() <= 64 * 1024, "CA file too large");
                Ok(std::fs::read(p)?)
            })
            .transpose()
    }
    pub fn limits(&self) -> Limits {
        Limits {
            max_in_flight: self.max_in_flight,
            max_request_bytes: self.max_request_bytes as u64,
            max_response_bytes: self.max_response_bytes,
            request_timeout: Duration::from_secs(self.timeout_seconds),
            ..Limits::default()
        }
    }
}

fn endpoint(value: &str) -> Result<(String, String)> {
    let url: reqwest::Url = value.parse()?;
    ensure!(
        matches!(url.scheme(), "http" | "https")
            && url.host_str().is_some()
            && url.username().is_empty()
            && url.password().is_none()
            && url.query().is_none()
            && url.fragment().is_none(),
        "upstream must be a fixed HTTP(S) endpoint without credentials/query/fragment"
    );
    ensure!(
        canonical_path(url.path()).as_deref() == Some(url.path()),
        "upstream path must be canonical"
    );
    Ok((url.origin().ascii_serialization(), url.path().into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn endpoint_configuration_rejects_credentials_and_ambiguous_paths() {
        let mut c = Config::demo().unwrap();
        for url in [
            "file:///tmp/server",
            "http://u:p@localhost/mcp",
            "http://localhost/mcp?token=x",
            "http://localhost/a%2fb",
            "http://localhost/a%252fb",
        ] {
            c.upstream = url.into();
            assert!(c.endpoint().is_err(), "{url}");
        }
        c.upstream = "https://example.com/api/mcp".into();
        assert_eq!(
            c.endpoint().unwrap(),
            ("https://example.com".into(), "/api/mcp".into())
        );
    }
    #[test]
    fn unknown_config_fields_and_unbounded_values_fail() {
        assert!(toml::from_str::<Config>("unexpected = true").is_err());
        let mut c = Config::demo().unwrap();
        c.max_request_bytes = usize::MAX;
        assert!(c.validate().is_err());
    }
    #[test]
    fn load_balancer_config_rejects_unknown_strategies_and_unsafe_backends() {
        assert!(toml::from_str::<LoadBalancerConfig>("strategy = 'typo'\nbackends = []").is_err());
        let mut c = Config::demo().unwrap();
        c.load_balancer = Some(LoadBalancerConfig {
            strategy: wayframe::lb::Strategy::RoundRobin,
            failure_cooldown_ms: 1000,
            backends: vec![BackendConfig {
                id: "a".into(),
                endpoint: "https://example.com/mcp".into(),
                token_env: None,
                ca: None,
                weight: 1,
                quota: 0,
                priority: 0,
                enabled: true,
            }],
        });
        assert!(c.validate().is_ok());
        c.load_balancer.as_mut().unwrap().backends[0].endpoint =
            "https://u:p@example.com/mcp".into();
        assert!(c.validate().is_err());
        c.load_balancer.as_mut().unwrap().backends[0].endpoint = "https://example.com/mcp".into();
        c.load_balancer.as_mut().unwrap().backends[0].weight = 0;
        assert!(c.validate().is_err());
    }
}
