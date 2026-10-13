//! Native policy blocks. Authentication never trusts client-supplied identity headers.
use jsonwebtoken::{Algorithm, DecodingKey, Validation};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    sync::Mutex,
    time::{Duration, Instant},
};
use subtle::ConstantTimeEq;
use wayframe_core::{Block, Decision, Execution, Phase, Reason};
use wayframe_http::{Identity, RequestContext};

fn bearer(ctx: &RequestContext) -> Option<&str> {
    let mut values = ctx.headers.get_all(http::header::AUTHORIZATION).iter();
    let raw = values.next()?.to_str().ok()?;
    if values.next().is_some() || raw.len() > 8192 {
        return None;
    }
    let (scheme, token) = raw.split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("bearer")
        || token.is_empty()
        || token.contains(char::is_whitespace)
    {
        return None;
    }
    Some(token)
}
fn unauthorized() -> Decision {
    Decision::Deny {
        reason: Reason::Unauthenticated,
        code: "unauthorized",
    }
}

/// Local/service-token option. Configuration stores an environment-variable name, not a secret.
pub struct StaticBearer {
    id: String,
    digest: [u8; 32],
    identity: Identity,
}
impl StaticBearer {
    pub fn new(id: String, secret: &str, identity: Identity) -> Result<Self, String> {
        if !(32..=8192).contains(&secret.len()) || secret.contains(char::is_whitespace) {
            return Err("bearer secrets require 32..8192 bytes without whitespace".into());
        }
        if identity.subject.is_empty()
            || identity.subject.len() > 256
            || identity.tenant.is_empty()
            || identity.tenant.len() > 256
        {
            return Err("empty identity".into());
        }
        Ok(Self {
            id,
            digest: Sha256::digest(secret.as_bytes()).into(),
            identity,
        })
    }
}
impl Block<RequestContext> for StaticBearer {
    fn id(&self) -> &str {
        &self.id
    }
    fn phase(&self) -> Phase {
        Phase::Authenticate
    }
    fn execute<'a>(&'a self, ctx: &'a mut RequestContext) -> Execution<'a> {
        let good = bearer(ctx)
            .map(|s| {
                let supplied: [u8; 32] = Sha256::digest(s.as_bytes()).into();
                bool::from(self.digest.ct_eq(&supplied))
            })
            .unwrap_or(false);
        ctx.identity = if good {
            Some(self.identity.clone())
        } else {
            None
        };
        Execution::Immediate(if good {
            Decision::Continue
        } else {
            unauthorized()
        })
    }
}

/// Static RS256 public key, explicit issuer/audience/algorithm. JWKS rotation is a separate roadmap item.
pub struct JwtAuth {
    id: String,
    key: DecodingKey,
    validation: Validation,
}
#[derive(Clone, Deserialize)]
struct Claims {
    sub: String,
    tenant: String,
}
impl JwtAuth {
    pub fn new(
        id: String,
        public_pem: &[u8],
        issuer: &str,
        audience: &str,
    ) -> Result<Self, String> {
        if issuer.is_empty() || audience.is_empty() {
            return Err("issuer and audience are required".into());
        }
        let key = DecodingKey::from_rsa_pem(public_pem).map_err(|_| "invalid RSA public key")?;
        let mut validation = Validation::new(Algorithm::RS256);
        validation.set_issuer(&[issuer]);
        validation.set_audience(&[audience]);
        validation.set_required_spec_claims(&["exp", "iss", "aud", "sub"]);
        validation.validate_nbf = true;
        validation.leeway = 30;
        Ok(Self {
            id,
            key,
            validation,
        })
    }
}
impl Block<RequestContext> for JwtAuth {
    fn id(&self) -> &str {
        &self.id
    }
    fn phase(&self) -> Phase {
        Phase::Authenticate
    }
    fn execute<'a>(&'a self, ctx: &'a mut RequestContext) -> Execution<'a> {
        let claims = bearer(ctx).and_then(|token| {
            jsonwebtoken::decode::<Claims>(token, &self.key, &self.validation)
                .ok()
                .map(|x| x.claims)
        });
        ctx.identity = claims
            .filter(|c| {
                !c.sub.is_empty()
                    && c.sub.len() <= 256
                    && !c.tenant.is_empty()
                    && c.tenant.len() <= 256
            })
            .map(|c| Identity {
                subject: c.sub,
                tenant: c.tenant,
                permissions: Default::default(),
            });
        Execution::Immediate(if ctx.identity.is_some() {
            Decision::Continue
        } else {
            unauthorized()
        })
    }
}

pub struct RequireIdentity {
    pub id: String,
}
impl Block<RequestContext> for RequireIdentity {
    fn id(&self) -> &str {
        &self.id
    }
    fn phase(&self) -> Phase {
        Phase::Authorize
    }
    fn execute<'a>(&'a self, ctx: &'a mut RequestContext) -> Execution<'a> {
        Execution::Immediate(if ctx.identity.is_some() {
            Decision::Continue
        } else {
            unauthorized()
        })
    }
}

#[derive(Debug)]
struct Bucket {
    tokens: f64,
    last: Instant,
}
impl Bucket {
    fn consume(&mut self, now: Instant, rate: f64, burst: f64) -> bool {
        self.tokens = (self.tokens + now.saturating_duration_since(self.last).as_secs_f64() * rate)
            .min(burst);
        self.last = now;
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}
struct LimiterState {
    buckets: HashMap<String, Bucket>,
    last_sweep: Instant,
}
/// Bounded-cardinality, local request quota. It is not an exact fleet-wide quota.
pub struct RateLimit {
    id: String,
    rate: f64,
    burst: f64,
    max_keys: usize,
    state: Mutex<LimiterState>,
}
impl RateLimit {
    pub fn new(id: String, per_second: f64, burst: u32, max_keys: usize) -> Result<Self, String> {
        if !per_second.is_finite()
            || per_second <= 0.0
            || burst == 0
            || !(1..=1_000_000).contains(&max_keys)
        {
            return Err("invalid rate limiter bounds".into());
        }
        Ok(Self {
            id,
            rate: per_second,
            burst: f64::from(burst),
            max_keys,
            state: Mutex::new(LimiterState {
                buckets: HashMap::new(),
                last_sweep: Instant::now(),
            }),
        })
    }
}
impl Block<RequestContext> for RateLimit {
    fn id(&self) -> &str {
        &self.id
    }
    fn phase(&self) -> Phase {
        Phase::Limit
    }
    fn execute<'a>(&'a self, ctx: &'a mut RequestContext) -> Execution<'a> {
        let key = match &ctx.identity {
            // Length-prefixed tenant prevents ambiguous concatenations.
            Some(i) => format!("{}:{}{}", i.tenant.len(), i.tenant, i.subject),
            None => format!("ip:{}", ctx.peer_ip),
        };
        let Ok(mut state) = self.state.lock() else {
            return Execution::Immediate(Decision::unavailable());
        };
        let now = Instant::now();
        // Evict ONLY after enough idle time to refill completely; eviction cannot reset a depleted quota.
        if now.duration_since(state.last_sweep) >= Duration::from_secs(30) {
            let idle = (self.burst / self.rate).max(60.0);
            state
                .buckets
                .retain(|_, b| now.duration_since(b.last).as_secs_f64() < idle);
            state.last_sweep = now;
        }
        if !state.buckets.contains_key(&key) && state.buckets.len() >= self.max_keys {
            return Execution::Immediate(Decision::unavailable());
        }
        let bucket = state.buckets.entry(key).or_insert(Bucket {
            tokens: self.burst,
            last: now,
        });
        Execution::Immediate(if bucket.consume(now, self.rate, self.burst) {
            Decision::Continue
        } else {
            Decision::Deny {
                reason: Reason::RateLimited,
                code: "rate_limited",
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn token_bucket_burst_refill_and_cap() {
        let now = Instant::now();
        let mut b = Bucket {
            tokens: 2.0,
            last: now,
        };
        assert!(b.consume(now, 2.0, 2.0));
        assert!(b.consume(now, 2.0, 2.0));
        assert!(!b.consume(now, 2.0, 2.0));
        assert!(b.consume(now + Duration::from_millis(500), 2.0, 2.0));
        assert!(!b.consume(now + Duration::from_millis(500), 2.0, 2.0));
        assert!(b.consume(now + Duration::from_secs(100), 2.0, 2.0));
        assert_eq!(b.tokens, 1.0);
    }
    #[test]
    fn invalid_bounds_are_rejected() {
        assert!(RateLimit::new("x".into(), f64::NAN, 1, 1).is_err());
        assert!(RateLimit::new("x".into(), 1.0, 0, 1).is_err());
        assert!(StaticBearer::new(
            "x".into(),
            "weak",
            Identity {
                subject: "s".into(),
                tenant: "t".into(),
                permissions: Default::default()
            }
        )
        .is_err());
    }
}

#[cfg(test)]
mod auth_tests {
    use super::*;
    fn context() -> RequestContext {
        RequestContext {
            request_id: 1,
            method: http::Method::GET,
            path: "/".into(),
            peer_ip: "127.0.0.1".parse().unwrap(),
            headers: Default::default(),
            identity: None,
            body: None,
            operation: None,
            deadline: tokio::time::Instant::now() + Duration::from_secs(1),
        }
    }
    #[tokio::test]
    async fn credentials_are_required_and_spoofed_identity_is_ignored() {
        let token = "0123456789abcdef0123456789abcdef";
        let auth = StaticBearer::new(
            "auth".into(),
            token,
            Identity {
                subject: "svc".into(),
                tenant: "t".into(),
                permissions: Default::default(),
            },
        )
        .unwrap();
        let mut c = context();
        c.headers
            .insert("x-wayframe-tenant", "attacker".parse().unwrap());
        assert_eq!(auth.execute(&mut c).resolve().await, unauthorized());
        c.headers
            .insert("authorization", format!("Bearer {token}").parse().unwrap());
        assert_eq!(auth.execute(&mut c).resolve().await, Decision::Continue);
        assert_eq!(c.identity.unwrap().tenant, "t");
    }
    #[tokio::test]
    async fn duplicate_credentials_fail_closed() {
        let token = "0123456789abcdef0123456789abcdef";
        let auth = StaticBearer::new(
            "auth".into(),
            token,
            Identity {
                subject: "svc".into(),
                tenant: "t".into(),
                permissions: Default::default(),
            },
        )
        .unwrap();
        let mut c = context();
        c.headers
            .append("authorization", format!("Bearer {token}").parse().unwrap());
        c.headers
            .append("authorization", format!("Bearer {token}").parse().unwrap());
        assert_eq!(auth.execute(&mut c).resolve().await, unauthorized());
        assert!(c.identity.is_none());
    }
    #[tokio::test]
    async fn limiter_is_bounded_and_isolates_identity_keys() {
        let limit = RateLimit::new("r".into(), 0.001, 1, 1).unwrap();
        let mut c = context();
        c.identity = Some(Identity {
            subject: "s".into(),
            tenant: "t".into(),
            permissions: Default::default(),
        });
        assert_eq!(limit.execute(&mut c).resolve().await, Decision::Continue);
        assert!(matches!(
            limit.execute(&mut c).resolve().await,
            Decision::Deny {
                reason: Reason::RateLimited,
                ..
            }
        ));
        c.identity.as_mut().unwrap().subject = "other".into();
        assert_eq!(
            limit.execute(&mut c).resolve().await,
            Decision::unavailable()
        );
    }
}

/// A bounded set of development/service credentials. This is not an OAuth server.
pub struct BearerTokens {
    id: String,
    entries: Vec<StaticBearer>,
}
impl BearerTokens {
    pub fn new(id: String, entries: Vec<(String, Identity)>) -> Result<Self, String> {
        if entries.is_empty() || entries.len() > 64 {
            return Err("1..64 token entries required".into());
        }
        let mut seen = std::collections::HashSet::new();
        let mut tokens = Vec::with_capacity(entries.len());
        for (secret, identity) in entries {
            let entry = StaticBearer::new(id.clone(), &secret, identity)?;
            if !seen.insert(entry.digest) {
                return Err("duplicate token credentials".into());
            }
            tokens.push(entry);
        }
        Ok(Self {
            id,
            entries: tokens,
        })
    }
}
impl Block<RequestContext> for BearerTokens {
    fn id(&self) -> &str {
        &self.id
    }
    fn phase(&self) -> Phase {
        Phase::Authenticate
    }
    fn execute<'a>(&'a self, ctx: &'a mut RequestContext) -> Execution<'a> {
        let digest = bearer(ctx).map(|s| <[u8; 32]>::from(Sha256::digest(s.as_bytes())));
        ctx.identity = None;
        if let Some(digest) = digest {
            // Do not stop at a matching token; compare every fixed-size digest.
            for entry in &self.entries {
                if bool::from(entry.digest.ct_eq(&digest)) {
                    ctx.identity = Some(entry.identity.clone());
                }
            }
        }
        Execution::Immediate(if ctx.identity.is_some() {
            Decision::Continue
        } else {
            unauthorized()
        })
    }
}

/// Exact Origin validation. An absent Origin is allowed for non-browser clients;
/// authentication is still mandatory in the reference application.
pub struct OriginGuard {
    id: String,
    allowed: std::collections::HashSet<String>,
}
impl OriginGuard {
    pub fn new(id: String, allowed: Vec<String>) -> Result<Self, String> {
        if allowed.len() > 64
            || allowed
                .iter()
                .any(|o| o.is_empty() || o.len() > 2048 || o.contains('*') || o == "null")
        {
            return Err("invalid origin allowlist".into());
        }
        Ok(Self {
            id,
            allowed: allowed.into_iter().collect(),
        })
    }
}
impl Block<RequestContext> for OriginGuard {
    fn id(&self) -> &str {
        &self.id
    }
    fn phase(&self) -> Phase {
        Phase::Admit
    }
    fn execute<'a>(&'a self, ctx: &'a mut RequestContext) -> Execution<'a> {
        let mut origins = ctx.headers.get_all("origin").iter();
        let decision = match origins.next() {
            None => Decision::Continue,
            Some(value)
                if origins.next().is_none()
                    && value
                        .to_str()
                        .ok()
                        .is_some_and(|v| self.allowed.contains(v)) =>
            {
                Decision::Continue
            }
            _ => Decision::deny(Reason::Forbidden, "origin_not_allowed"),
        };
        Execution::Immediate(decision)
    }
}
