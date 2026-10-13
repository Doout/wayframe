//! Bounded session ownership for the handshake-era HTTP transport.
use std::{
    collections::HashMap,
    sync::Mutex,
    time::{Duration, Instant},
};
use wayframe::{
    http::{Identity, RequestContext},
    prelude::*,
};

struct Session {
    owner: (String, String),
    touched: Instant,
}
pub struct Sessions {
    entries: Mutex<HashMap<String, Session>>,
    maximum: usize,
    lifetime: Duration,
}
impl Sessions {
    pub fn new() -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            maximum: 4096,
            lifetime: Duration::from_secs(1800),
        }
    }
    pub fn bind(&self, id: &str, identity: &Identity) -> bool {
        if id.is_empty() || id.len() > 512 || !id.bytes().all(|b| (0x21..=0x7e).contains(&b)) {
            return false;
        }
        let Ok(mut entries) = self.entries.lock() else {
            return false;
        };
        entries.retain(|_, s| s.touched.elapsed() < self.lifetime);
        if entries.len() >= self.maximum || entries.contains_key(id) {
            return false;
        }
        entries.insert(
            id.into(),
            Session {
                owner: (identity.tenant.clone(), identity.subject.clone()),
                touched: Instant::now(),
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
        let decide = || {
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
            if session.owner != (identity.tenant.clone(), identity.subject.clone()) {
                return Decision::deny(Reason::Forbidden, "session_not_owned");
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
