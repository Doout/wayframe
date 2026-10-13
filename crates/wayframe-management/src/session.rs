//! Bounded session ownership for the handshake-era HTTP transport.
use std::{
    collections::HashMap,
    sync::Mutex,
    time::{Duration, Instant},
};
use wayframe_core::{Block, Decision, Execution, Phase, Reason};
use wayframe_http::{Identity, RequestContext};

struct Session {
    generation: u64,
    owner: (String, String),
    touched: Instant,
    backend: Option<String>,
}
pub struct Sessions {
    generation: std::sync::atomic::AtomicU64,
    entries: Mutex<HashMap<String, Session>>,
    maximum: usize,
    lifetime: Duration,
}
impl Default for Sessions {
    fn default() -> Self {
        Self::new()
    }
}
impl Sessions {
    pub fn new() -> Self {
        Self {
            generation: std::sync::atomic::AtomicU64::new(1),
            entries: Mutex::new(HashMap::new()),
            maximum: 4096,
            lifetime: Duration::from_secs(1800),
        }
    }
    #[cfg(test)]
    pub fn bind(&self, id: &str, identity: &Identity) -> bool {
        self.bind_backend(id, identity, None)
    }
    pub fn bind_backend(&self, id: &str, identity: &Identity, backend: Option<String>) -> bool {
        self.bind_generation(
            id,
            identity,
            backend,
            self.generation.load(std::sync::atomic::Ordering::Acquire),
        )
    }
    pub fn set_generation(&self, generation: u64) {
        if let Ok(mut entries) = self.entries.lock() {
            let current = self.generation.load(std::sync::atomic::Ordering::Acquire);
            if generation <= current {
                return;
            }
            self.generation
                .store(generation, std::sync::atomic::Ordering::Release);
            entries.retain(|_, s| s.generation == generation);
        }
    }
    pub fn bind_generation(
        &self,
        id: &str,
        identity: &Identity,
        backend: Option<String>,
        generation: u64,
    ) -> bool {
        if generation != self.generation.load(std::sync::atomic::Ordering::Acquire) {
            return false;
        }
        if id.is_empty() || id.len() > 512 || !id.bytes().all(|b| (0x21..=0x7e).contains(&b)) {
            return false;
        }
        let Ok(mut entries) = self.entries.lock() else {
            return false;
        };
        if generation != self.generation.load(std::sync::atomic::Ordering::Acquire) {
            return false;
        }
        entries.retain(|_, s| s.touched.elapsed() < self.lifetime);
        if entries.len() >= self.maximum || entries.contains_key(id) {
            return false;
        }
        entries.insert(
            id.into(),
            Session {
                generation,
                owner: (identity.tenant.clone(), identity.subject.clone()),
                touched: Instant::now(),
                backend,
            },
        );
        true
    }
    pub fn remove(&self, id: &str) {
        if let Ok(mut entries) = self.entries.lock() {
            entries.remove(id);
        }
    }
}
impl Block<RequestContext> for Sessions {
    fn id(&self) -> &str {
        "session-owner"
    }
    fn phase(&self) -> Phase {
        Phase::Authorize
    }
    fn execute<'a>(&'a self, ctx: &'a mut RequestContext) -> Execution<'a> {
        let mut decide = || {
            let Some(identity) = &ctx.identity else {
                return Decision::deny(Reason::Unauthenticated, "unauthorized");
            };
            let mut ids = ctx.headers.get_all("mcp-session-id").iter();
            let Some(id) = ids.next() else {
                return Decision::Continue;
            };
            let Ok(id) = id.to_str() else {
                return Decision::deny(Reason::InvalidInput, "invalid_session");
            };
            if ids.next().is_some() || id.is_empty() || id.len() > 512 {
                return Decision::deny(Reason::InvalidInput, "invalid_session");
            }
            let Ok(mut entries) = self.entries.lock() else {
                return Decision::unavailable();
            };
            let Some(session) = entries.get_mut(id) else {
                return Decision::deny(Reason::NotFound, "unknown_session");
            };
            if session.touched.elapsed() >= self.lifetime {
                entries.remove(id);
                return Decision::deny(Reason::NotFound, "expired_session");
            }
            if ctx
                .extensions
                .get::<wayframe_http::DeploymentGeneration>()
                .is_some_and(|g| g.0 != session.generation)
            {
                return Decision::deny(Reason::NotFound, "session_generation_changed");
            }
            if session.owner != (identity.tenant.clone(), identity.subject.clone()) {
                return Decision::deny(Reason::Forbidden, "session_not_owned");
            }
            if let Some(backend) = &session.backend {
                ctx.extensions
                    .insert(wayframe_http::PinnedBackend(backend.clone()));
            }
            session.touched = Instant::now();
            Decision::Continue
        };
        Execution::Immediate(decide())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn sessions_are_bound_to_tenant_and_subject_and_capacity_is_bounded() {
        let mut sessions = Sessions::new();
        sessions.maximum = 1;
        let owner = Identity::new("reader", "a");
        assert!(sessions.bind("s1", &owner));
        assert!(!sessions.bind("s2", &owner));
        let mut ctx = RequestContext::new(
            "GET".parse().unwrap(),
            "/mcp".into(),
            "127.0.0.1".parse().unwrap(),
            tokio::time::Instant::now() + Duration::from_secs(1),
        );
        ctx.headers.insert("mcp-session-id", "s1".parse().unwrap());
        for other in [Identity::new("operator", "a"), Identity::new("reader", "b")] {
            ctx.identity = Some(other);
            assert!(matches!(
                sessions.execute(&mut ctx).resolve().await,
                Decision::Deny {
                    reason: Reason::Forbidden,
                    ..
                }
            ));
        }
        ctx.identity = Some(owner.clone());
        assert_eq!(
            sessions.execute(&mut ctx).resolve().await,
            Decision::Continue
        );
        sessions.remove("s1");
        assert!(sessions.bind("s2", &owner));
        assert!(matches!(
            sessions.execute(&mut ctx).resolve().await,
            Decision::Deny {
                reason: Reason::NotFound,
                ..
            }
        ));
    }
}
