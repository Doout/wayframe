//! Opaque continuation and task ownership. State bytes are hashed, never interpreted or logged.
use super::{hash, Store};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use wayframe_core::{Block, Decision, Execution, Phase, Reason};
use wayframe_http::{Identity, Operation, RequestContext, ResponseContext, ResponseFilter};
#[derive(Clone)]
struct Owned {
    identity: Identity,
    operation: Operation,
    parameters: String,
    backend: Option<String>,
    generation: u64,
    expires: Instant,
}
pub struct Continuations {
    store: Arc<Store>,
    entries: Mutex<HashMap<String, Owned>>,
}
fn parameters(value: &serde_json::Value) -> String {
    let mut params = value
        .get("params")
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));
    if let Some(params) = params.as_object_mut() {
        params.remove("requestState");
        params.remove("inputResponses");
        params.remove("_meta");
    }
    hash(&serde_json::to_string(&params).expect("JSON"))
}
struct Final(Arc<Continuations>);
impl Block<RequestContext> for Final {
    fn id(&self) -> &str {
        "current-continuation-owner"
    }
    fn phase(&self) -> Phase {
        Phase::Limit
    }
    fn execute<'a>(&'a self, c: &'a mut RequestContext) -> Execution<'a> {
        self.0.execute(c)
    }
}
impl Continuations {
    pub fn final_check(self: &Arc<Self>) -> Arc<dyn Block<RequestContext>> {
        Arc::new(Final(self.clone()))
    }
    pub fn new(store: Arc<Store>) -> Arc<Self> {
        Arc::new(Self {
            store,
            entries: Mutex::new(HashMap::new()),
        })
    }
    fn put(&self, key: String, owned: Owned) -> Decision {
        let Ok(mut entries) = self.entries.lock() else {
            return Decision::unavailable();
        };
        entries.retain(|_, v| v.expires > Instant::now());
        if let Some(old) = entries.get(&key) {
            if old.identity.tenant != owned.identity.tenant
                || old.identity.subject != owned.identity.subject
                || old.backend != owned.backend
                || old.generation != owned.generation
                || old.operation != owned.operation
                || old.parameters != owned.parameters
            {
                return Decision::unavailable();
            }
        }
        if !entries.contains_key(&key) && entries.len() >= 8192 {
            return Decision::unavailable();
        }
        entries.insert(key, owned);
        Decision::Continue
    }
}
impl Block<RequestContext> for Continuations {
    fn id(&self) -> &str {
        "mcp-continuation-owner"
    }
    fn phase(&self) -> Phase {
        Phase::Authorize
    }
    fn execute<'a>(&'a self, c: &'a mut RequestContext) -> Execution<'a> {
        let denied = || Decision::deny(Reason::Forbidden, "continuation_not_owned");
        let decision = (|| {
            let Some(operation) = &c.operation else {
                return denied();
            };
            let value = c
                .body
                .as_ref()
                .and_then(|b| wayframe_mcp::parse_unique(b).ok());
            let task = operation.action.starts_with("tasks/");
            let key = if task {
                operation
                    .target
                    .as_deref()
                    .map(|s| format!("task:{}", hash(s)))
            } else {
                value
                    .as_ref()
                    .and_then(|v| v.pointer("/params/requestState"))
                    .and_then(serde_json::Value::as_str)
                    .map(|s| format!("state:{}", hash(s)))
            };
            let Some(key) = key else {
                return if task { denied() } else { Decision::Continue };
            };
            let owned = self
                .entries
                .lock()
                .ok()
                .and_then(|entries| entries.get(&key).cloned());
            let Some(owned) = owned else {
                return denied();
            };
            let Some(identity) = &c.identity else {
                return denied();
            };
            let generation = c
                .extensions
                .get::<wayframe_http::DeploymentGeneration>()
                .map_or(1, |g| g.0);
            if owned.expires <= Instant::now()
                || owned.generation != generation
                || owned.identity.tenant != identity.tenant
                || owned.identity.subject != identity.subject
            {
                return denied();
            }
            if !task
                && (owned.operation != *operation
                    || value
                        .as_ref()
                        .is_none_or(|v| parameters(v) != owned.parameters))
            {
                return denied();
            }
            let state = self.store.current.load();
            let map = match owned.operation.action.as_str() {
                "tools/call" => Some(&state.rules.tools),
                "resources/read" => Some(&state.rules.resources),
                "prompts/get" => Some(&state.rules.prompts),
                _ => None,
            };
            if let Some(map) = map {
                if owned.operation.target.as_ref().is_none_or(|name| {
                    !map.get(name)
                        .is_some_and(|p| identity.permissions.contains(p))
                        || (owned.operation.action == "tools/call"
                            && state.rules.denied_tools.contains(name))
                }) {
                    return denied();
                }
            }
            if let Some(backend) = owned.backend {
                c.extensions.insert(wayframe_http::PinnedBackend(backend));
            }
            Decision::Continue
        })();
        Execution::Immediate(decision)
    }
}
impl ResponseFilter for Continuations {
    fn id(&self) -> &str {
        "mcp-continuations"
    }
    fn records(&self, c: &ResponseContext) -> bool {
        c.status.is_success()
            && c.operation.as_ref().is_some_and(|o| {
                matches!(
                    o.action.as_str(),
                    "tools/call" | "resources/read" | "prompts/get"
                ) || o.action.starts_with("tasks/")
            })
    }
    fn record(&self, c: &ResponseContext, value: &mut serde_json::Value) -> Decision {
        let (Some(identity), Some(operation)) = (&c.identity, &c.operation) else {
            return Decision::unavailable();
        };
        if let Some(profile) =
            self.store.current.load().users.values().find(|u| {
                u.profile.tenant == identity.tenant && u.profile.subject == identity.subject
            })
        {
            if !profile.profile.enabled
                || !identity.permissions.is_subset(&profile.profile.permissions)
            {
                return Decision::deny(Reason::Forbidden, "grants_changed");
            }
        }
        let Some(result) = value.get("result") else {
            return Decision::Continue;
        };
        let owned = Owned {
            identity: Identity::new(&identity.subject, &identity.tenant),
            operation: operation.clone(),
            parameters: c.request.as_ref().map_or_else(String::new, parameters),
            backend: c.backend.clone(),
            generation: c.generation,
            expires: Instant::now() + Duration::from_secs(300),
        };
        if let Some(state) = result
            .get("requestState")
            .and_then(serde_json::Value::as_str)
        {
            if state.is_empty() || state.len() > 65536 {
                return Decision::unavailable();
            }
            let decision = self.put(format!("state:{}", hash(state)), owned.clone());
            if decision != Decision::Continue {
                return decision;
            }
        }
        let task = result
            .get("taskId")
            .or_else(|| result.pointer("/task/taskId"))
            .and_then(serde_json::Value::as_str);
        if let Some(task) = task {
            if task.is_empty() || task.len() > 1024 {
                return Decision::unavailable();
            }
            // Polling cannot widen the original task's owner or lifetime.
            if operation.action.starts_with("tasks/") {
                return Decision::Continue;
            }
            if self
                .store
                .current
                .load()
                .rules
                .tasks_permission
                .as_ref()
                .is_none_or(|p| !identity.permissions.contains(p))
            {
                return Decision::deny(Reason::Forbidden, "tasks_not_permitted");
            }
            let ttl = result
                .get("ttlMs")
                .or_else(|| result.pointer("/task/ttlMs"))
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(300000)
                .clamp(1, 3600000);
            let mut owned = owned;
            owned.expires = Instant::now() + Duration::from_millis(ttl);
            return self.put(format!("task:{}", hash(task)), owned);
        }
        Decision::Continue
    }
}
