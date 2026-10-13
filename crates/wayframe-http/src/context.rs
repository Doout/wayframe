use bytes::Bytes;
use http::{HeaderMap, Method};
use std::{collections::BTreeSet, net::IpAddr};
use tokio::time::Instant;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Identity {
    pub subject: String,
    pub tenant: String,
    pub permissions: BTreeSet<String>,
}
/// Populated only by a trusted cryptographic authenticator; never sourced from request headers.
#[derive(Clone)]
pub struct VerifiedIssuer(pub String);
#[derive(Clone)]
pub struct DeploymentGeneration(pub u64);
impl Identity {
    pub fn new(subject: impl Into<String>, tenant: impl Into<String>) -> Self {
        Self {
            subject: subject.into(),
            tenant: tenant.into(),
            permissions: BTreeSet::new(),
        }
    }
    pub fn with_permissions(
        mut self,
        permissions: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.permissions = permissions.into_iter().map(Into::into).collect();
        self
    }
}
/// Only a trusted decoder sets this after validating the body, not from an unverified header.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Operation {
    pub protocol: &'static str,
    pub action: String,
    pub target: Option<String>,
}

#[derive(Clone)]
pub struct RequestContext {
    pub request_id: u64,
    pub method: Method,
    pub path: String,
    pub peer_ip: IpAddr,
    pub headers: HeaderMap,
    pub identity: Option<Identity>,
    pub deadline: Instant,
    /// Populated only on explicitly body-inspecting routes, after preflight admission/auth.
    pub body: Option<Bytes>,
    pub operation: Option<Operation>,
    /// Application-owned trusted per-request state; never serialized or copied to headers.
    pub extensions: http::Extensions,
}
impl RequestContext {
    pub fn new(method: Method, path: String, peer_ip: IpAddr, deadline: Instant) -> Self {
        Self {
            request_id: 0,
            method,
            path,
            peer_ip,
            headers: HeaderMap::new(),
            identity: None,
            deadline,
            body: None,
            operation: None,
            extensions: http::Extensions::new(),
        }
    }
}
impl wayframe_core::Context for RequestContext {
    fn deadline(&self) -> std::time::Instant {
        self.deadline.into_std()
    }
}

/// Only trusted session/routing code supplies this hint; client headers cannot select an origin.
#[cfg(feature = "lb")]
#[derive(Clone)]
pub struct PinnedBackend(pub String);
#[cfg(feature = "lb")]
impl wayframe_lb::RoutingContext for RequestContext {
    fn pinned_backend(&self) -> Option<&str> {
        self.extensions.get::<PinnedBackend>().map(|p| p.0.as_str())
    }
    fn set_selection(&mut self, selection: wayframe_lb::Selection) {
        self.extensions.insert(selection);
    }
}
#[cfg(feature = "routing")]
impl wayframe_routing::RouteContext for RequestContext {
    fn route_input(&self) -> wayframe_routing::Input<'_> {
        wayframe_routing::Input {
            host: self.headers.get("host").and_then(|v| v.to_str().ok()),
            method: self.method.as_str(),
            path: &self.path,
            tenant: self.identity.as_ref().map(|i| i.tenant.as_str()),
            operation: self.operation.as_ref().map(|o| o.action.as_str()),
            target: self.operation.as_ref().and_then(|o| o.target.as_deref()),
        }
    }
    fn set_route(&mut self, route: wayframe_routing::Selected) {
        self.extensions.insert(route);
    }
    fn selected_route(&self) -> Option<&wayframe_routing::Selected> {
        self.extensions.get()
    }
}
