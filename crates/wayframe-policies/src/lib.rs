//! Configurable native schema validation, response redaction and explicit browser response headers.
use anyhow::ensure;
use std::{collections::HashMap, sync::Arc};
use wayframe_core::{Block, Decision, Execution, Phase, Reason};
use wayframe_http::{RequestContext, ResponseContext, ResponseFilter};
use wayframe_plugins::{AsyncResult, Config, Descriptor, Factory, PhaseInfo, Plugin};
pub struct HttpHooks(pub Vec<Arc<dyn ResponseFilter>>);
struct Schema {
    id: String,
    tools: HashMap<String, jsonschema::Validator>,
}
impl Block<RequestContext> for Schema {
    fn id(&self) -> &str {
        &self.id
    }
    fn phase(&self) -> Phase {
        Phase::Authorize
    }
    fn execute<'a>(&'a self, c: &'a mut RequestContext) -> Execution<'a> {
        let decision = if let Some(op) = c.operation.as_ref().filter(|o| o.action == "tools/call") {
            if let Some(schema) = op.target.as_ref().and_then(|t| self.tools.get(t)) {
                let value = c
                    .body
                    .as_ref()
                    .and_then(|b| wayframe_codec::parse_unique(b).ok());
                if value
                    .as_ref()
                    .and_then(|v| v.pointer("/params/arguments"))
                    .is_some_and(|v| schema.is_valid(v))
                {
                    Decision::Continue
                } else {
                    Decision::deny(Reason::InvalidInput, "tool_schema_invalid")
                }
            } else {
                Decision::Continue
            }
        } else {
            Decision::Continue
        };
        Execution::Immediate(decision)
    }
}
struct Redact {
    id: String,
    pointers: Vec<String>,
}
impl ResponseFilter for Redact {
    fn id(&self) -> &str {
        &self.id
    }
    fn records(&self, c: &ResponseContext) -> bool {
        c.operation.is_some()
    }
    fn record(&self, _: &ResponseContext, value: &mut serde_json::Value) -> Decision {
        for pointer in &self.pointers {
            if let Some(value) = value.pointer_mut(pointer) {
                *value = serde_json::Value::String("[redacted]".into());
            }
        }
        Decision::Continue
    }
}
struct Cors {
    id: String,
    origin: String,
}
impl ResponseFilter for Cors {
    fn id(&self) -> &str {
        &self.id
    }
    fn preflight(&self, origin: &str, headers: &mut http::HeaderMap) -> bool {
        if origin != self.origin {
            return false;
        }
        headers.insert(
            "access-control-allow-origin",
            self.origin.parse().expect("validated"),
        );
        headers.insert("vary", "Origin".parse().expect("literal"));
        headers.insert(
            "access-control-allow-methods",
            "POST, GET, DELETE".parse().expect("literal"),
        );
        headers.insert("access-control-allow-headers","authorization, content-type, accept, mcp-protocol-version, mcp-method, mcp-name, mcp-session-id, last-event-id".parse().expect("literal"));
        true
    }
    fn headers(&self, c: &ResponseContext, headers: &mut http::HeaderMap) -> Decision {
        if c.origin.as_deref() != Some(&self.origin) {
            return Decision::Continue;
        }
        headers.insert(
            "access-control-allow-origin",
            self.origin.parse().expect("validated origin"),
        );
        headers.append("vary", "Origin".parse().expect("literal"));
        headers.insert(
            "access-control-expose-headers",
            "mcp-session-id, x-wayframe-generation, www-authenticate"
                .parse()
                .expect("literal"),
        );
        headers.remove("access-control-allow-credentials");
        Decision::Continue
    }
}
struct Instance {
    id: String,
    block: Option<Arc<dyn Block<RequestContext>>>,
    hooks: Option<Arc<HttpHooks>>,
    cache: Option<Arc<wayframe_http::cache::CacheHooks>>,
}
impl Plugin<RequestContext> for Instance {
    fn id(&self) -> &str {
        &self.id
    }
    fn block(&self) -> Option<Arc<dyn Block<RequestContext>>> {
        self.block.clone()
    }
    fn stats(&self) -> serde_json::Value {
        self.cache
            .as_ref()
            .map_or_else(|| serde_json::json!({}), |c| c.0.stats())
    }
    fn exports(&self) -> Vec<Arc<dyn std::any::Any + Send + Sync>> {
        self.cache
            .iter()
            .map(|h| h.clone() as Arc<dyn std::any::Any + Send + Sync>)
            .chain(
                self.hooks
                    .iter()
                    .map(|h| h.clone() as Arc<dyn std::any::Any + Send + Sync>),
            )
            .collect()
    }
}
#[derive(Clone, Copy)]
pub enum Native {
    Schema,
    Redact,
    Cors,
    Cache,
}
impl Native {
    pub fn all() -> [Self; 4] {
        [Self::Schema, Self::Redact, Self::Cors, Self::Cache]
    }
}
impl Factory<RequestContext> for Native {
    fn descriptor(&self) -> Descriptor {
        let (kind, phase, schema) = match self {
            Self::Cache => (
                "response-cache",
                None,
                serde_json::json!({"type":"object","required":["tools","ttl_ms"],"properties":{"tools":{"type":"array","maxItems":128,"items":{"type":"string"}},"ttl_ms":{"type":"integer","minimum":1,"maximum":3600000}},"additionalProperties":false}),
            ),
            Self::Schema => (
                "tool-schema",
                Some(PhaseInfo::Authorize),
                serde_json::json!({"type":"object","required":["tools"],"properties":{"tools":{"type":"object","maxProperties":128}},"additionalProperties":false}),
            ),
            Self::Redact => (
                "response-redaction",
                None,
                serde_json::json!({"type":"object","required":["pointers"],"properties":{"pointers":{"type":"array","maxItems":128,"items":{"type":"string"}}},"additionalProperties":false}),
            ),
            Self::Cors => (
                "cors",
                None,
                serde_json::json!({"type":"object","required":["origin"],"properties":{"origin":{"type":"string"}},"additionalProperties":false}),
            ),
        };
        Descriptor {
            kind: kind.into(),
            version: "0.1".into(),
            phase,
            requires: Default::default(),
            provides: Default::default(),
            settings_schema: schema,
        }
    }
    fn prepare<'a>(&'a self, c: &'a Config) -> AsyncResult<'a, Arc<dyn Plugin<RequestContext>>> {
        Box::pin(async move {
            let mut instance = Instance {
                id: c.id.clone(),
                block: None,
                hooks: None,
                cache: None,
            };
            match self {
                Self::Cache => {
                    #[derive(serde::Deserialize)]
                    #[serde(deny_unknown_fields)]
                    struct Settings {
                        tools: Vec<String>,
                        ttl_ms: u64,
                    }
                    let settings: Settings = serde_json::from_value(c.settings.clone())?;
                    ensure!(
                        settings.tools.len() <= 128
                            && settings
                                .tools
                                .iter()
                                .all(|t| !t.is_empty() && t.len() <= 1024),
                        "invalid cache tool allowlist"
                    );
                    instance.cache = Some(Arc::new(wayframe_http::cache::CacheHooks(Arc::new(
                        ResponseCache {
                            cache: wayframe_cache::Cache::new(1024, 16 * 1024 * 1024, 1024)?,
                            tools: settings.tools.into_iter().collect(),
                            ttl: std::time::Duration::from_millis(settings.ttl_ms),
                        },
                    ))));
                }
                Self::Schema => {
                    #[derive(serde::Deserialize)]
                    #[serde(deny_unknown_fields)]
                    struct Settings {
                        tools: HashMap<String, serde_json::Value>,
                    }
                    let s: Settings = serde_json::from_value(c.settings.clone())?;
                    ensure!(s.tools.len() <= 128, "schema capacity");
                    let mut tools = HashMap::new();
                    for (name, schema) in s.tools {
                        ensure!(
                            !name.is_empty()
                                && name.len() <= 1024
                                && serde_json::to_vec(&schema)?.len() <= 65536,
                            "invalid tool schema"
                        );
                        tools.insert(
                            name,
                            jsonschema::validator_for(&schema)
                                .map_err(|_| anyhow::anyhow!("invalid tool schema"))?,
                        );
                    }
                    instance.block = Some(Arc::new(Schema {
                        id: c.id.clone(),
                        tools,
                    }));
                }
                Self::Redact => {
                    #[derive(serde::Deserialize)]
                    #[serde(deny_unknown_fields)]
                    struct Settings {
                        pointers: Vec<String>,
                    }
                    let s: Settings = serde_json::from_value(c.settings.clone())?;
                    ensure!(
                        s.pointers.iter().all(|p| p.starts_with("/result/")
                            && p.len() <= 1024
                            && !p.contains(char::is_control)),
                        "redaction must target result fields"
                    );
                    instance.hooks = Some(Arc::new(HttpHooks(vec![Arc::new(Redact {
                        id: c.id.clone(),
                        pointers: s.pointers,
                    })])));
                }
                Self::Cors => {
                    #[derive(serde::Deserialize)]
                    #[serde(deny_unknown_fields)]
                    struct Settings {
                        origin: String,
                    }
                    let s: Settings = serde_json::from_value(c.settings.clone())?;
                    let _: http::HeaderValue = s.origin.parse()?;
                    ensure!(
                        (s.origin.starts_with("https://")
                            || s.origin.starts_with("http://localhost:")
                            || s.origin.starts_with("http://127.0.0.1:"))
                            && !s.origin.contains(['*', ',', ' ', '\r', '\n'])
                            && s.origin.len() <= 2048,
                        "invalid CORS origin"
                    );
                    instance.hooks = Some(Arc::new(HttpHooks(vec![Arc::new(Cors {
                        id: c.id.clone(),
                        origin: s.origin,
                    })])));
                }
            }
            Ok(Arc::new(instance) as Arc<dyn Plugin<RequestContext>>)
        })
    }
}
struct ResponseCache {
    cache: wayframe_cache::Cache<serde_json::Value>,
    tools: std::collections::HashSet<String>,
    ttl: std::time::Duration,
}
struct CacheTicket {
    key: wayframe_cache::Key,
    _flight: tokio::sync::OwnedMutexGuard<()>,
}
impl wayframe_http::cache::ResponseCache for ResponseCache {
    fn stats(&self) -> serde_json::Value {
        self.cache
            .stats()
            .unwrap_or_else(|_| serde_json::json!({"ready":false}))
    }
    fn begin<'a>(&'a self, c: &'a RequestContext) -> wayframe_http::cache::Lookup<'a> {
        Box::pin(async move {
            use wayframe_http::cache::Access;
            let Some(op) = c.operation.as_ref().filter(|o| {
                o.action == "tools/call"
                    && o.target.as_ref().is_some_and(|t| self.tools.contains(t))
            }) else {
                return Ok(Access::Bypass);
            };
            if c.headers
                .get("mcp-protocol-version")
                .is_none_or(|v| v != "2026-07-28")
            {
                return Ok(Access::Bypass);
            }
            let Some(identity) = &c.identity else {
                return Ok(Access::Bypass);
            };
            let Some(value) = c
                .body
                .as_ref()
                .and_then(|b| wayframe_codec::parse_unique(b).ok())
            else {
                return Ok(Access::Bypass);
            };
            if value.pointer("/params/requestState").is_some()
                || value.pointer("/params/inputResponses").is_some()
                || value.pointer("/params/task").is_some()
            {
                return Ok(Access::Bypass);
            }
            let parameters = value["params"].clone();
            let route = c
                .extensions
                .get::<wayframe_routing::Selected>()
                .map_or("default", |r| r.route.as_str());
            let backend = c
                .extensions
                .get::<wayframe_lb::Selection>()
                .map_or("default", |s| s.backend_id());
            let scope = format!("{route}/{backend}");
            let generation = c
                .extensions
                .get::<wayframe_http::DeploymentGeneration>()
                .map_or(1, |g| g.0);
            let key = wayframe_cache::Key::new(
                wayframe_cache::Scope {
                    tenant: &identity.tenant,
                    subject: &identity.subject,
                    permissions: &identity.permissions,
                    generation,
                    route: &scope,
                },
                &op.action,
                op.target.as_deref().unwrap_or(""),
                &parameters,
            )
            .map_err(|_| Decision::unavailable())?;
            let flight = self
                .cache
                .singleflight(&key, c.deadline)
                .await
                .map_err(|_| Decision::unavailable())?;
            if let Some(cached) = self.cache.get(&key).map_err(|_| Decision::unavailable())? {
                let mut response = (*cached).clone();
                response["id"] = value["id"].clone();
                let bytes = serde_json::to_vec(&response).map_err(|_| Decision::unavailable())?;
                return Ok(Access::Hit(bytes.into()));
            }
            Ok(Access::Miss(Arc::new(CacheTicket {
                key,
                _flight: flight,
            })))
        })
    }
    fn store(
        &self,
        ticket: &wayframe_http::cache::Ticket,
        status: http::StatusCode,
        headers: &http::HeaderMap,
        bytes: &[u8],
    ) {
        if status != http::StatusCode::OK
            || !headers
                .get("content-type")
                .is_some_and(|v| v.to_str().is_ok_and(|v| v.starts_with("application/json")))
        {
            return;
        }
        let Some(ticket) = ticket.downcast_ref::<CacheTicket>() else {
            return;
        };
        let Ok(value) = wayframe_codec::parse_unique(bytes) else {
            return;
        };
        if value.get("error").is_some()
            || value.get("result").is_none()
            || value.pointer("/result/isError") == Some(&serde_json::Value::Bool(true))
            || value
                .pointer("/result/resultType")
                .is_some_and(|t| t == "task" || t == "input_required")
        {
            return;
        }
        let _ = self
            .cache
            .insert(ticket.key.clone(), value, bytes.len(), self.ttl);
    }
}
