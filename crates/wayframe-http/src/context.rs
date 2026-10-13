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
        }
    }
}
impl wayframe_core::Context for RequestContext {
    fn deadline(&self) -> std::time::Instant {
        self.deadline.into_std()
    }
}
