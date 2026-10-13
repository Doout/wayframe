//! Transport-independent routing blocks. A reservation lasts until its final clone is dropped.
use serde::{Deserialize, Serialize};
use std::{
    fmt,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};
use wayframe_core::{Block, Context, Decision, Execution, Phase, Reason};

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Strategy {
    RoundRobin,
    LeastConnections,
    WeightedRoundRobin,
    QuotaPriority,
}
impl Strategy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::RoundRobin => "round_robin",
            Self::LeastConnections => "least_connections",
            Self::WeightedRoundRobin => "weighted_round_robin",
            Self::QuotaPriority => "quota_priority",
        }
    }
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Backend {
    pub id: String,
    #[serde(default = "one")]
    pub weight: u32,
    /// A quota-priority preference. Zero means unlimited; overflow selects the last eligible backend.
    #[serde(default)]
    pub quota: u32,
    #[serde(default)]
    pub priority: u32,
    #[serde(default = "enabled")]
    pub enabled: bool,
}
fn one() -> u32 {
    1
}
fn enabled() -> bool {
    true
}
impl Backend {
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            weight: 1,
            quota: 0,
            priority: 0,
            enabled: true,
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SelectError {
    NoBackends,
    PinnedUnavailable,
    Poisoned,
    CounterExhausted,
}
impl fmt::Display for SelectError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "backend selection failed: {self:?}")
    }
}
impl std::error::Error for SelectError {}
struct Entry {
    enabled: bool,
    unavailable_until: Option<Instant>,
    epoch: u64,
    active: u64,
    current_weight: i64,
    selected: u64,
    failures: u64,
    overflow: u64,
}
struct State {
    entries: Vec<Entry>,
    cursor: u64,
}
struct Inner {
    backends: Vec<Backend>,
    state: Mutex<State>,
}
#[derive(Clone)]
pub struct Pool(Arc<Inner>);
#[derive(Debug, Serialize)]
pub struct BackendStats {
    pub id: String,
    pub enabled: bool,
    pub healthy: bool,
    pub active: u64,
    pub selected: u64,
    pub failures: u64,
    pub overflow: u64,
}
impl Pool {
    pub fn new(backends: Vec<Backend>) -> Result<Self, String> {
        if backends.is_empty() || backends.len() > 128 {
            return Err("configure 1..128 backends".into());
        }
        let mut ids = std::collections::HashSet::new();
        for b in &backends {
            if b.id.is_empty()
                || b.id.len() > 64
                || !b
                    .id
                    .bytes()
                    .all(|c| c.is_ascii_alphanumeric() || b"-_.".contains(&c))
                || !ids.insert(&b.id)
                || !(1..=1_000_000).contains(&b.weight)
                || b.quota > 1_000_000
            {
                return Err("invalid/duplicate backend id, weight or quota".into());
            }
        }
        let entries = backends
            .iter()
            .map(|b| Entry {
                enabled: b.enabled,
                unavailable_until: None,
                epoch: 0,
                active: 0,
                current_weight: 0,
                selected: 0,
                failures: 0,
                overflow: 0,
            })
            .collect();
        Ok(Self(Arc::new(Inner {
            backends,
            state: Mutex::new(State { entries, cursor: 0 }),
        })))
    }
    /// Operator/health-check input; an already admitted reservation remains counted.
    pub fn set_enabled(&self, id: &str, enabled: bool) -> Result<(), SelectError> {
        let index = self
            .0
            .backends
            .iter()
            .position(|b| b.id == id)
            .ok_or(SelectError::NoBackends)?;
        let mut state = self.0.state.lock().map_err(|_| SelectError::Poisoned)?;
        let entry = &mut state.entries[index];
        entry.enabled = enabled;
        entry.current_weight = 0;
        Ok(())
    }
    pub fn snapshot(&self) -> Result<Vec<BackendStats>, SelectError> {
        let state = self.0.state.lock().map_err(|_| SelectError::Poisoned)?;
        let now = Instant::now();
        Ok(self
            .0
            .backends
            .iter()
            .zip(&state.entries)
            .map(|(b, e)| BackendStats {
                id: b.id.clone(),
                enabled: e.enabled,
                healthy: e.unavailable_until.is_none_or(|until| until <= now),
                active: e.active,
                selected: e.selected,
                failures: e.failures,
                overflow: e.overflow,
            })
            .collect())
    }
    pub fn select(
        &self,
        strategy: Strategy,
        pinned: Option<&str>,
    ) -> Result<Selection, SelectError> {
        let mut state = self.0.state.lock().map_err(|_| SelectError::Poisoned)?;
        let now = Instant::now();
        let eligible = |i: usize, state: &State| {
            state.entries[i].enabled
                && state.entries[i]
                    .unavailable_until
                    .is_none_or(|until| until <= now)
        };
        let mut indices = [0usize; 128];
        let mut n = 0;
        for i in 0..state.entries.len() {
            if eligible(i, &state) {
                indices[n] = i;
                n += 1;
            }
        }
        let mut overflow = false;
        let index = if let Some(id) = pinned {
            let index = self
                .0
                .backends
                .iter()
                .position(|b| b.id == id)
                .filter(|i| eligible(*i, &state))
                .ok_or(SelectError::PinnedUnavailable)?;
            let quota = self.0.backends[index].quota;
            overflow = strategy == Strategy::QuotaPriority
                && quota > 0
                && state.entries[index].active >= quota as u64;
            index
        } else {
            if n == 0 {
                return Err(SelectError::NoBackends);
            }
            match strategy {
                Strategy::RoundRobin => indices[(state.cursor % n as u64) as usize],
                Strategy::LeastConnections => {
                    let start = (state.cursor % n as u64) as usize;
                    (0..n)
                        .map(|offset| indices[(start + offset) % n])
                        .min_by_key(|i| state.entries[*i].active)
                        .expect("nonempty")
                }
                Strategy::WeightedRoundRobin => {
                    let mut selected = indices[0];
                    let mut total = 0i64;
                    let mut highest = i64::MIN;
                    for i in &indices[..n] {
                        let weight = self.0.backends[*i].weight as i64;
                        total += weight;
                        state.entries[*i].current_weight += weight;
                        if state.entries[*i].current_weight > highest {
                            highest = state.entries[*i].current_weight;
                            selected = *i;
                        }
                    }
                    state.entries[selected].current_weight -= total;
                    selected
                }
                Strategy::QuotaPriority => {
                    indices[..n].sort_unstable_by_key(|i| (self.0.backends[*i].priority, *i));
                    let under = indices[..n].iter().find(|i| {
                        self.0.backends[**i].quota == 0
                            || state.entries[**i].active < self.0.backends[**i].quota as u64
                    });
                    match under {
                        Some(i) => *i,
                        None => {
                            overflow = true;
                            indices[n - 1]
                        }
                    }
                }
            }
        };
        state.cursor = state.cursor.wrapping_add(1);
        let entry = &mut state.entries[index];
        entry.active = entry
            .active
            .checked_add(1)
            .ok_or(SelectError::CounterExhausted)?;
        entry.selected = entry.selected.saturating_add(1);
        if overflow {
            entry.overflow = entry.overflow.saturating_add(1);
        }
        let epoch = entry.epoch;
        Ok(Selection(Arc::new(Reservation {
            inner: self.0.clone(),
            index,
            epoch,
            strategy,
            overflow,
            failed: AtomicBool::new(false),
        })))
    }
}
struct Reservation {
    inner: Arc<Inner>,
    index: usize,
    epoch: u64,
    strategy: Strategy,
    overflow: bool,
    failed: AtomicBool,
}
impl Drop for Reservation {
    fn drop(&mut self) {
        if let Ok(mut state) = self.inner.state.lock() {
            state.entries[self.index].active -= 1;
        }
    }
}
#[derive(Clone)]
pub struct Selection(Arc<Reservation>);
impl Selection {
    pub fn backend_id(&self) -> &str {
        &self.0.inner.backends[self.0.index].id
    }
    pub fn strategy(&self) -> Strategy {
        self.0.strategy
    }
    pub fn overflow(&self) -> bool {
        self.0.overflow
    }
    /// Reports a transport/availability failure once, skipping this backend during cooldown.
    pub fn failed(&self, cooldown: Duration) {
        if self.0.failed.swap(true, Ordering::Relaxed) {
            return;
        }
        if let Ok(mut state) = self.0.inner.state.lock() {
            let entry = &mut state.entries[self.0.index];
            entry.failures = entry.failures.saturating_add(1);
            entry.epoch = entry.epoch.saturating_add(1);
            entry.unavailable_until =
                Instant::now().checked_add(cooldown.min(Duration::from_secs(86400)));
            entry.current_weight = 0;
        }
    }
    /// A stale success cannot clear a more recent request's failure/cooldown.
    pub fn succeeded(&self) {
        if self.0.failed.load(Ordering::Relaxed) {
            return;
        }
        if let Ok(mut state) = self.0.inner.state.lock() {
            let entry = &mut state.entries[self.0.index];
            if entry.epoch == self.0.epoch {
                entry.unavailable_until = None;
            }
        }
    }
}
/// An application-owned context maps the selected ID to its registered destination/worker/channel.
pub trait RoutingContext: Context {
    fn pinned_backend(&self) -> Option<&str> {
        None
    }
    fn set_selection(&mut self, selection: Selection);
}
pub struct LoadBalance {
    id: String,
    pool: Pool,
    strategy: Strategy,
}
impl LoadBalance {
    pub fn new(id: impl Into<String>, pool: Pool, strategy: Strategy) -> Self {
        Self {
            id: id.into(),
            pool,
            strategy,
        }
    }
}
impl<C: RoutingContext> Block<C> for LoadBalance {
    fn id(&self) -> &str {
        &self.id
    }
    fn phase(&self) -> Phase {
        Phase::Dispatch
    }
    fn execute<'a>(&'a self, ctx: &'a mut C) -> Execution<'a> {
        match self.pool.select(self.strategy, ctx.pinned_backend()) {
            Ok(selection) => {
                #[cfg(feature = "tracing")]
                {
                    let span = tracing::Span::current();
                    span.record("lb.strategy", self.strategy.as_str());
                    span.record("lb.backend.id", selection.backend_id());
                    span.record("lb.overflow", selection.overflow());
                }
                ctx.set_selection(selection);
                Execution::Immediate(Decision::Continue)
            }
            Err(error) => Execution::Immediate(Decision::deny(
                Reason::Unavailable,
                match error {
                    SelectError::PinnedUnavailable => "pinned_backend_unavailable",
                    _ => "no_backends_available",
                },
            )),
        }
    }
}
macro_rules! routing_block {
    ($name:ident,$strategy:ident) => {
        pub struct $name(LoadBalance);
        impl $name {
            pub fn new(id: impl Into<String>, pool: Pool) -> Self {
                Self(LoadBalance::new(id, pool, Strategy::$strategy))
            }
        }
        impl<C: RoutingContext> Block<C> for $name {
            fn id(&self) -> &str {
                &self.0.id
            }
            fn phase(&self) -> Phase {
                Phase::Dispatch
            }
            fn execute<'a>(&'a self, ctx: &'a mut C) -> Execution<'a> {
                self.0.execute(ctx)
            }
        }
    };
}
routing_block!(RoundRobin, RoundRobin);
routing_block!(LeastConnections, LeastConnections);
routing_block!(WeightedRoundRobin, WeightedRoundRobin);
routing_block!(QuotaPriority, QuotaPriority);

#[cfg(test)]
mod tests {
    use super::*;
    fn pool() -> Pool {
        Pool::new(vec![
            Backend::new("a"),
            Backend::new("b"),
            Backend::new("c"),
        ])
        .unwrap()
    }
    #[test]
    fn round_robin_and_least_connections_rotate_ties() {
        for strategy in [Strategy::RoundRobin, Strategy::LeastConnections] {
            let pool = pool();
            for expected in ["a", "b", "c", "a", "b", "c"] {
                let choice = pool.select(strategy, None).unwrap();
                assert_eq!(choice.backend_id(), expected);
                drop(choice);
            }
            assert!(pool.snapshot().unwrap().iter().all(|s| s.active == 0));
        }
    }
    #[test]
    fn least_connections_reserves_before_next_selection() {
        let pool = pool();
        let a = pool.select(Strategy::LeastConnections, None).unwrap();
        let b = pool.select(Strategy::LeastConnections, None).unwrap();
        let c = pool.select(Strategy::LeastConnections, None).unwrap();
        assert_eq!(
            (a.backend_id(), b.backend_id(), c.backend_id()),
            ("a", "b", "c")
        );
        drop(b);
        assert_eq!(
            pool.select(Strategy::LeastConnections, None)
                .unwrap()
                .backend_id(),
            "b"
        );
    }
    #[test]
    fn smooth_weighted_round_robin_matches_weights_and_skips_disabled() {
        let mut backends = vec![Backend::new("a"), Backend::new("b"), Backend::new("c")];
        backends[1].weight = 3;
        backends[2].weight = 6;
        let pool = Pool::new(backends).unwrap();
        for _ in 0..100 {
            drop(pool.select(Strategy::WeightedRoundRobin, None).unwrap());
        }
        let stats = pool.snapshot().unwrap();
        assert_eq!(
            stats.iter().map(|s| s.selected).collect::<Vec<_>>(),
            vec![10, 30, 60]
        );
        pool.set_enabled("c", false).unwrap();
        for _ in 0..40 {
            assert_ne!(
                pool.select(Strategy::WeightedRoundRobin, None)
                    .unwrap()
                    .backend_id(),
                "c"
            );
        }
        let stats = pool.snapshot().unwrap();
        assert_eq!(
            stats.iter().map(|s| s.selected).collect::<Vec<_>>(),
            vec![20, 60, 60]
        );
    }
    #[test]
    fn quota_priority_orders_and_overflows_to_last_eligible() {
        let mut backends = vec![Backend::new("a"), Backend::new("b"), Backend::new("c")];
        for b in &mut backends {
            b.quota = 1;
        }
        backends[0].priority = 2;
        backends[1].priority = 0;
        backends[2].priority = 1;
        let pool = Pool::new(backends).unwrap();
        let mut held = Vec::new();
        for (expected, overflow) in [("b", false), ("c", false), ("a", false), ("a", true)] {
            let choice = pool.select(Strategy::QuotaPriority, None).unwrap();
            assert_eq!(choice.backend_id(), expected);
            assert_eq!(choice.overflow(), overflow);
            held.push(choice);
        }
        pool.set_enabled("a", false).unwrap();
        let choice = pool.select(Strategy::QuotaPriority, None).unwrap();
        assert_eq!(choice.backend_id(), "c");
        assert!(choice.overflow());
        drop(choice);
        drop(held);
        assert!(pool.snapshot().unwrap().iter().all(|s| s.active == 0));
    }
    #[test]
    fn zero_quota_is_unlimited_and_ties_keep_config_order() {
        let pool = pool();
        let held: Vec<_> = (0..8)
            .map(|_| pool.select(Strategy::QuotaPriority, None).unwrap())
            .collect();
        assert!(held.iter().all(|s| s.backend_id() == "a" && !s.overflow()));
    }
    #[test]
    fn cloned_reservations_count_once_and_release_only_on_last_drop() {
        let pool = pool();
        let choice = pool.select(Strategy::RoundRobin, None).unwrap();
        let cloned = choice.clone();
        drop(choice);
        assert_eq!(pool.snapshot().unwrap()[0].active, 1);
        drop(cloned);
        assert_eq!(pool.snapshot().unwrap()[0].active, 0);
    }
    #[test]
    fn failure_cooldown_skips_backend_and_stale_success_cannot_clear_it() {
        let pool = pool();
        let stale = pool.select(Strategy::RoundRobin, Some("a")).unwrap();
        let bad = pool.select(Strategy::RoundRobin, Some("a")).unwrap();
        bad.failed(Duration::from_secs(30));
        bad.failed(Duration::from_secs(30));
        stale.succeeded();
        assert_eq!(pool.snapshot().unwrap()[0].failures, 1);
        assert!(!pool.snapshot().unwrap()[0].healthy);
        for strategy in [
            Strategy::RoundRobin,
            Strategy::LeastConnections,
            Strategy::WeightedRoundRobin,
            Strategy::QuotaPriority,
        ] {
            assert_ne!(pool.select(strategy, None).unwrap().backend_id(), "a");
            assert!(matches!(
                pool.select(strategy, Some("a")),
                Err(SelectError::PinnedUnavailable)
            ));
        }
    }
    #[test]
    fn pinned_route_never_moves_and_all_disabled_fails_closed() {
        let pool = pool();
        for strategy in [
            Strategy::RoundRobin,
            Strategy::LeastConnections,
            Strategy::WeightedRoundRobin,
            Strategy::QuotaPriority,
        ] {
            for _ in 0..4 {
                assert_eq!(pool.select(strategy, Some("b")).unwrap().backend_id(), "b");
            }
        }
        assert!(pool.select(Strategy::RoundRobin, Some("unknown")).is_err());
        for id in ["a", "b", "c"] {
            pool.set_enabled(id, false).unwrap();
        }
        assert!(matches!(
            pool.select(Strategy::RoundRobin, None),
            Err(SelectError::NoBackends)
        ));
    }
    #[test]
    fn concurrent_priority_selection_cannot_oversubscribe_preferred_quota() {
        let mut a = Backend::new("a");
        a.quota = 3;
        let pool = Pool::new(vec![a, Backend::new("b")]).unwrap();
        let barrier = Arc::new(std::sync::Barrier::new(17));
        let mut tasks = Vec::new();
        for _ in 0..16 {
            let pool = pool.clone();
            let barrier = barrier.clone();
            tasks.push(std::thread::spawn(move || {
                let choice = pool.select(Strategy::QuotaPriority, None).unwrap();
                barrier.wait();
                barrier.wait();
                choice.backend_id().to_owned()
            }));
        }
        barrier.wait();
        let stats = pool.snapshot().unwrap();
        assert_eq!(stats[0].active, 3);
        assert_eq!(stats[1].active, 13);
        barrier.wait();
        let ids: Vec<_> = tasks.into_iter().map(|t| t.join().unwrap()).collect();
        assert_eq!(ids.iter().filter(|id| *id == "a").count(), 3);
        assert!(pool.snapshot().unwrap().iter().all(|s| s.active == 0));
    }
    struct Message {
        deadline: Instant,
        selection: Option<Selection>,
        pinned: Option<String>,
    }
    impl Context for Message {
        fn deadline(&self) -> Instant {
            self.deadline
        }
    }
    impl RoutingContext for Message {
        fn pinned_backend(&self) -> Option<&str> {
            self.pinned.as_deref()
        }
        fn set_selection(&mut self, selection: Selection) {
            self.selection = Some(selection);
        }
    }
    #[tokio::test]
    async fn all_named_blocks_work_in_typed_native_pipelines_and_release_denials() {
        let pool = pool();
        let blocks: Vec<Arc<dyn Block<Message>>> = vec![
            Arc::new(RoundRobin::new("rr", pool.clone())),
            Arc::new(LeastConnections::new("lc", pool.clone())),
            Arc::new(WeightedRoundRobin::new("wrr", pool.clone())),
            Arc::new(QuotaPriority::new("qp", pool.clone())),
        ];
        for block in blocks {
            let pipeline = wayframe_runtime::Pipeline::compile(vec![block]).unwrap();
            let mut message = Message {
                deadline: Instant::now() + Duration::from_secs(1),
                selection: None,
                pinned: None,
            };
            assert_eq!(pipeline.evaluate(&mut message).await, Decision::Continue);
            assert!(message.selection.is_some());
            drop(message);
            let mut message = Message {
                deadline: Instant::now() + Duration::from_secs(1),
                selection: None,
                pinned: Some("unknown".into()),
            };
            assert!(matches!(
                pipeline.evaluate(&mut message).await,
                Decision::Deny { .. }
            ));
            assert!(message.selection.is_none());
        }
        assert!(pool.snapshot().unwrap().iter().all(|s| s.active == 0));
    }
    #[test]
    fn invalid_configuration_fails_instead_of_defaulting_strategy() {
        assert!(Pool::new(vec![]).is_err());
        let mut a = Backend::new("a");
        a.weight = 0;
        assert!(Pool::new(vec![a]).is_err());
        assert!(Pool::new(vec![Backend::new("a"), Backend::new("a")]).is_err());
        assert!(Pool::new(vec![Backend::new("untrusted/path")]).is_err());
        assert!(serde_json::from_str::<Strategy>("\"unknown\"").is_err());
    }
}
