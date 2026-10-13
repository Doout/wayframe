//! Validated route tables and named backend groups, independent of a wire transport.
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use wayframe_core::{Block, Context, Decision, Execution, Phase, Reason};
use wayframe_lb::{Pool, RoutingContext, Strategy};
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Route {
    pub id: String,
    pub group: String,
    #[serde(default)]
    pub hosts: Vec<String>,
    #[serde(default)]
    pub methods: Vec<String>,
    pub path: String,
    #[serde(default)]
    pub prefix: bool,
    #[serde(default)]
    pub tenants: Vec<String>,
    #[serde(default)]
    pub operations: Vec<String>,
    #[serde(default)]
    pub targets: Vec<String>,
}
pub struct Input<'a> {
    pub host: Option<&'a str>,
    pub method: &'a str,
    pub path: &'a str,
    pub tenant: Option<&'a str>,
    pub operation: Option<&'a str>,
    pub target: Option<&'a str>,
}
#[derive(Clone, Debug)]
pub struct Selected {
    pub route: String,
    pub group: String,
}
pub struct Table {
    routes: Vec<Route>,
}
fn label(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 64
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
}
impl Table {
    /// Configuration order resolves intentional overlap; an unmatched route always denies.
    pub fn new(routes: Vec<Route>) -> Result<Self, String> {
        if routes.is_empty() || routes.len() > 256 {
            return Err("configure 1..256 routes".into());
        }
        let mut ids = HashSet::new();
        for r in &routes {
            if !label(&r.id)
                || !label(&r.group)
                || !ids.insert(&r.id)
                || !r.path.starts_with('/')
                || r.path.len() > 8192
                || r.path.contains(['%', '?', '#', '\\'])
                || r.path.contains("//")
                || r.path.split('/').any(|p| matches!(p, "." | ".."))
            {
                return Err("invalid route id/group/path".into());
            }
            for values in [&r.hosts, &r.methods, &r.tenants, &r.operations, &r.targets] {
                if values.len() > 128
                    || values
                        .iter()
                        .any(|s| s.is_empty() || s.len() > 1024 || s.chars().any(char::is_control))
                {
                    return Err("invalid route selector".into());
                }
            }
        }
        Ok(Self { routes })
    }
    pub fn select(&self, input: &Input<'_>) -> Option<Selected> {
        self.routes
            .iter()
            .find(|r| {
                let path = input.path == r.path
                    || (r.prefix
                        && input
                            .path
                            .starts_with(&format!("{}/", r.path.trim_end_matches('/'))));
                path && (r.hosts.is_empty()
                    || input
                        .host
                        .is_some_and(|v| r.hosts.iter().any(|h| h.eq_ignore_ascii_case(v))))
                    && (r.methods.is_empty() || r.methods.iter().any(|v| v == input.method))
                    && (r.tenants.is_empty()
                        || input
                            .tenant
                            .is_some_and(|v| r.tenants.iter().any(|t| t == v)))
                    && (r.operations.is_empty()
                        || input
                            .operation
                            .is_some_and(|v| r.operations.iter().any(|o| o == v)))
                    && (r.targets.is_empty()
                        || input
                            .target
                            .is_some_and(|v| r.targets.iter().any(|t| t == v)))
            })
            .map(|r| Selected {
                route: r.id.clone(),
                group: r.group.clone(),
            })
    }
    pub fn routes(&self) -> &[Route] {
        &self.routes
    }
}
pub trait RouteContext: Context {
    fn route_input(&self) -> Input<'_>;
    fn set_route(&mut self, selection: Selected);
    fn selected_route(&self) -> Option<&Selected>;
}
pub struct RouteBlock {
    id: String,
    table: Table,
}
impl RouteBlock {
    pub fn new(id: impl Into<String>, table: Table) -> Self {
        Self {
            id: id.into(),
            table,
        }
    }
}
impl<C: RouteContext> Block<C> for RouteBlock {
    fn id(&self) -> &str {
        &self.id
    }
    fn phase(&self) -> Phase {
        Phase::Dispatch
    }
    fn execute<'a>(&'a self, c: &'a mut C) -> Execution<'a> {
        Execution::Immediate(
            if let Some(selected) = self.table.select(&c.route_input()) {
                c.set_route(selected);
                Decision::Continue
            } else {
                Decision::deny(Reason::NotFound, "route_not_found")
            },
        )
    }
}
pub struct Groups {
    id: String,
    groups: HashMap<String, (Pool, Strategy)>,
}
impl Groups {
    pub fn new(
        id: impl Into<String>,
        groups: HashMap<String, (Pool, Strategy)>,
    ) -> Result<Self, String> {
        if groups.is_empty() || groups.len() > 128 || groups.keys().any(|g| !label(g)) {
            return Err("invalid backend groups".into());
        }
        Ok(Self {
            id: id.into(),
            groups,
        })
    }
}
impl<C: RouteContext + RoutingContext> Block<C> for Groups {
    fn id(&self) -> &str {
        &self.id
    }
    fn phase(&self) -> Phase {
        Phase::Dispatch
    }
    fn execute<'a>(&'a self, c: &'a mut C) -> Execution<'a> {
        let choice = c
            .selected_route()
            .and_then(|r| self.groups.get(&r.group))
            .and_then(|(pool, strategy)| pool.select(*strategy, c.pinned_backend()).ok());
        Execution::Immediate(if let Some(choice) = choice {
            c.set_selection(choice);
            Decision::Continue
        } else {
            Decision::deny(Reason::Unavailable, "route_group_unavailable")
        })
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn selectors_preserve_tenant_and_path_segment_boundaries() {
        let table = Table::new(vec![Route {
            id: "a".into(),
            group: "a".into(),
            hosts: vec![],
            methods: vec!["POST".into()],
            path: "/a".into(),
            prefix: true,
            tenants: vec!["tenant-a".into()],
            operations: vec![],
            targets: vec![],
        }])
        .unwrap();
        let mut i = Input {
            host: None,
            method: "POST",
            path: "/a/mcp",
            tenant: Some("tenant-a"),
            operation: None,
            target: None,
        };
        assert!(table.select(&i).is_some());
        i.path = "/ab";
        assert!(table.select(&i).is_none());
        i.path = "/a/mcp";
        i.tenant = Some("tenant-b");
        assert!(table.select(&i).is_none());
    }
}
