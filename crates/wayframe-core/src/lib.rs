//! Protocol-independent building blocks. This crate has no external dependencies.
//!
//! A context is application-owned: it can represent HTTP metadata, a message,
//! a connection, or a typed work item. Blocks sharing a pipeline must agree on
//! its concrete type. Native blocks are trusted, cooperative code.
use std::{collections::HashSet, fmt, future::Future, pin::Pin, sync::Arc, time::Instant};

/// Work's deadline. It must not be extended by a policy block.
pub trait Context: Send + 'static {
    fn deadline(&self) -> Instant;
}

/// Transport-independent rejection categories; adapters choose a wire representation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Reason {
    InvalidInput,
    MethodNotAllowed,
    Unauthenticated,
    Forbidden,
    NotFound,
    Conflict,
    PayloadTooLarge,
    RateLimited,
    Unavailable,
    DeadlineExceeded,
    Internal,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Decision {
    /// Continue to the next mandatory control; not a final authorization override.
    Continue,
    Deny {
        reason: Reason,
        code: &'static str,
    },
}
impl Decision {
    pub const fn deny(reason: Reason, code: &'static str) -> Self {
        Self::Deny { reason, code }
    }
    pub const fn unavailable() -> Self {
        Self::deny(Reason::Unavailable, "control_unavailable")
    }
    pub const fn expired() -> Self {
        Self::deny(Reason::DeadlineExceeded, "deadline_exceeded")
    }
}
/// Immediate native results avoid allocating a future for every block transition.
/// A deferred block is an explicit scheduling/I/O boundary.
pub enum Execution<'a> {
    Immediate(Decision),
    Deferred(Pin<Box<dyn Future<Output = Decision> + Send + 'a>>),
}
/// A work result, separate from a mandatory policy decision. Owned inputs allow parallel work.
pub enum Task<T, E> {
    Immediate(Result<T, E>),
    Deferred(Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'static>>),
}
impl<T, E> Task<T, E> {
    pub async fn resolve(self) -> Result<T, E> {
        match self {
            Self::Immediate(result) => result,
            Self::Deferred(future) => future.await,
        }
    }
}
impl Execution<'_> {
    pub async fn resolve(self) -> Decision {
        match self {
            Self::Immediate(d) => d,
            Self::Deferred(f) => f.await,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Phase {
    Admit,
    Authenticate,
    Inspect,
    Authorize,
    Limit,
    Dispatch,
}

/// Implement this trait for a trusted native block. Untrusted code uses a sandbox adapter.
pub trait Block<C: Context>: Send + Sync + 'static {
    fn id(&self) -> &str;
    fn phase(&self) -> Phase;
    fn execute<'a>(&'a self, context: &'a mut C) -> Execution<'a>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlanError {
    TooManyBlocks,
    InvalidId(String),
    DuplicateId(String),
    InvalidOrder(String),
}
impl fmt::Display for PlanError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooManyBlocks => write!(f, "a pipeline supports at most 64 blocks"),
            Self::InvalidId(id) => write!(f, "invalid block identifier: {id:?}"),
            Self::DuplicateId(id) => write!(f, "duplicate block identifier: {id}"),
            Self::InvalidOrder(id) => write!(f, "phase order moves backwards at block: {id}"),
        }
    }
}
impl std::error::Error for PlanError {}

/// Validated immutable linear plan. Typed branching and fan-out live in the optional flow crate.
pub struct Plan<C: Context> {
    blocks: Vec<Arc<dyn Block<C>>>,
}
impl<C: Context> Plan<C> {
    pub fn compile(blocks: Vec<Arc<dyn Block<C>>>) -> Result<Self, PlanError> {
        if blocks.len() > 64 {
            return Err(PlanError::TooManyBlocks);
        }
        let mut ids = HashSet::new();
        let mut previous = Phase::Admit;
        for block in &blocks {
            let id = block.id();
            if id.is_empty()
                || id.len() > 64
                || !id
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
            {
                return Err(PlanError::InvalidId(id.into()));
            }
            if !ids.insert(id.to_owned()) {
                return Err(PlanError::DuplicateId(id.into()));
            }
            if block.phase() < previous {
                return Err(PlanError::InvalidOrder(id.into()));
            }
            previous = block.phase();
        }
        Ok(Self { blocks })
    }
    pub fn blocks(&self) -> &[Arc<dyn Block<C>>] {
        &self.blocks
    }
    pub fn len(&self) -> usize {
        self.blocks.len()
    }
    pub fn is_empty(&self) -> bool {
        self.blocks.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    struct Work(Instant);
    impl Context for Work {
        fn deadline(&self) -> Instant {
            self.0
        }
    }
    struct Noop(&'static str, Phase);
    impl Block<Work> for Noop {
        fn id(&self) -> &str {
            self.0
        }
        fn phase(&self) -> Phase {
            self.1
        }
        fn execute<'a>(&'a self, _: &'a mut Work) -> Execution<'a> {
            Execution::Immediate(Decision::Continue)
        }
    }
    fn b(id: &'static str, phase: Phase) -> Arc<dyn Block<Work>> {
        Arc::new(Noop(id, phase))
    }
    #[test]
    fn rejects_duplicate_ids() {
        assert!(matches!(
            Plan::compile(vec![b("a", Phase::Admit), b("a", Phase::Admit)]),
            Err(PlanError::DuplicateId(_))
        ));
    }
    #[test]
    fn rejects_backwards_phases() {
        assert!(matches!(
            Plan::compile(vec![b("a", Phase::Limit), b("b", Phase::Authenticate)]),
            Err(PlanError::InvalidOrder(_))
        ));
    }
    #[test]
    fn rejects_bad_ids_and_unbounded_plans() {
        assert!(Plan::compile(vec![b("injected\nlabel", Phase::Admit)]).is_err());
        assert!(matches!(
            Plan::compile((0..65).map(|_| b("a", Phase::Admit)).collect()),
            Err(PlanError::TooManyBlocks)
        ));
    }
    #[test]
    fn empty_plan_is_valid() {
        assert!(Plan::<Work>::compile(vec![]).unwrap().is_empty());
    }
}

/// Trusted listener hook for cancelling an unresponsive transport even when a body is unpolled.
#[derive(Clone)]
pub struct TransportCancellation(pub std::sync::Arc<dyn Fn() + Send + Sync>);
