//! Bounded typed DAG execution. Each node owns its output; joins never merge mutable contexts.
mod plans;
mod schedule;
use futures_util::{stream::FuturesUnordered, StreamExt};
pub use plans::{CacheStatistics, Output, Prepared};
use schedule::{accept, Arguments, NodeSet, NodeTimer, Schedule, Step};
use std::{
    any::{Any, TypeId},
    collections::HashSet,
    fmt,
    future::Future,
    marker::PhantomData,
    panic::AssertUnwindSafe,
    pin::Pin,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    sync::Semaphore,
    time::{timeout_at, Instant},
};
use tracing::Instrument;
use wayframe_core::{Context, Decision, Phase, Reason, Task};
type Value = Arc<dyn Any + Send + Sync>;
type Invoke = Arc<dyn Fn(&[Option<Value>]) -> Task<Value, Error> + Send + Sync>;
#[derive(Clone)]
struct Action {
    invoke: Invoke,
    immediate: bool,
}
impl Action {
    fn immediate(
        f: impl Fn(&[Option<Value>]) -> Task<Value, Error> + Send + Sync + 'static,
    ) -> Self {
        Self {
            invoke: Arc::new(f),
            immediate: true,
        }
    }
    fn deferred(
        f: impl Fn(&[Option<Value>]) -> Task<Value, Error> + Send + Sync + 'static,
    ) -> Self {
        Self {
            invoke: Arc::new(f),
            immediate: false,
        }
    }
}
type Completed = (usize, Duration, Duration, Result<Value, Error>);
type Running = Pin<Box<dyn Future<Output = Completed> + Send>>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Denied(Reason, &'static str),
    Failed(&'static str),
    Deadline,
    Overloaded,
    Panic,
    InvalidGraph(&'static str),
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "workflow failed: {self:?}")
    }
}
impl std::error::Error for Error {}
impl Error {
    pub fn decision(&self) -> Decision {
        match self {
            Self::Denied(reason, code) => Decision::deny(*reason, code),
            Self::Deadline => Decision::expired(),
            _ => Decision::unavailable(),
        }
    }
    fn outcome(&self) -> &'static str {
        match self {
            Self::Denied(..) => "denied",
            Self::Deadline => "timed_out",
            Self::Overloaded => "overloaded",
            Self::Panic => "panicked",
            _ => "failed",
        }
    }
}
pub struct Port<T> {
    scope: Arc<()>,
    index: usize,
    marker: PhantomData<fn() -> T>,
}
impl<T> Clone for Port<T> {
    fn clone(&self) -> Self {
        Self {
            scope: self.scope.clone(),
            index: self.index,
            marker: PhantomData,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum State {
    Pending,
    Running,
    Succeeded,
    Skipped,
    Cancelled,
    Failed,
}
#[derive(Debug)]
pub struct NodeReport {
    pub id: String,
    pub state: State,
    pub dependency_wait: Duration,
    pub scheduler_wait: Duration,
    /// Time executing after scheduler admission.
    pub elapsed: Duration,
    /// Why the node was removed from this invocation's executable graph.
    pub skip_reason: Option<SkipReason>,
    pub scheduling: Option<Scheduling>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Scheduling {
    Inline,
    Serial,
    Parallel,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SkipReason {
    BranchNotSelected,
    DependencySkipped,
    UnrequestedOutput,
}
pub struct Run {
    scope: Arc<()>,
    values: Vec<Option<Value>>,
    pub nodes: Vec<NodeReport>,
}
impl fmt::Debug for Run {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Run")
            .field("nodes", &self.nodes)
            .finish_non_exhaustive()
    }
}
impl Run {
    pub fn get<T: Send + Sync + 'static>(&self, port: &Port<T>) -> Option<&T> {
        if !Arc::ptr_eq(&self.scope, &port.scope) {
            return None;
        }
        self.values.get(port.index)?.as_ref()?.downcast_ref()
    }
}
pub struct Failure {
    pub error: Error,
    pub nodes: Vec<NodeReport>,
}
impl fmt::Debug for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Failure")
            .field("error", &self.error)
            .field("nodes", &self.nodes)
            .finish()
    }
}
impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.error.fmt(f)
    }
}
impl std::error::Error for Failure {}
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub max_parallel: usize,
    pub max_executions: usize,
    pub max_running_nodes: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            max_parallel: 16,
            max_executions: 128,
            max_running_nodes: 256,
        }
    }
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Work,
    Gate,
    Dispatch,
    Choice,
}
struct Node {
    id: String,
    phase: Phase,
    deps: Vec<usize>,
    input_types: Vec<TypeId>,
    output: TypeId,
    action: Action,
    dependencies: String,
    condition: Option<Condition>,
    kind: Kind,
    budget: Option<Duration>,
    discardable: bool,
}
struct Condition {
    dependency: usize,
    matches: Arc<dyn Fn(&Value) -> bool + Send + Sync>,
}
/// Immutable indexes prepared once per graph generation. No executable code is generated.
struct Topology {
    dependents: Vec<Vec<usize>>,
    guarded: Vec<Vec<usize>>,
    gates: Vec<usize>,
}
pub struct Builder<C> {
    scope: Arc<()>,
    nodes: Vec<Node>,
    limits: Limits,
    marker: PhantomData<fn() -> C>,
}
pub struct Flow<C> {
    scope: Arc<()>,
    nodes: Arc<Vec<Node>>,
    topology: Topology,
    limits: Limits,
    admission: Arc<Semaphore>,
    running: Arc<Semaphore>,
    plan_cache: Mutex<plans::Cache>,
    marker: PhantomData<fn() -> C>,
}
fn input<T: Send + Sync + 'static>(inputs: &[Option<Value>], i: usize) -> Arc<T> {
    inputs[i]
        .as_ref()
        .expect("required input")
        .clone()
        .downcast()
        .expect("compiled input type")
}
impl<C: Context + Sync> Builder<C> {
    pub fn new() -> Self {
        let root = Node {
            id: "input".into(),
            phase: Phase::Admit,
            deps: vec![],
            input_types: vec![],
            output: TypeId::of::<C>(),
            action: Action::immediate(|_| {
                Task::Immediate(Err(Error::Failed("input_not_executable")))
            }),
            dependencies: String::new(),
            condition: None,
            kind: Kind::Work,
            budget: None,
            discardable: false,
        };
        Self {
            scope: Arc::new(()),
            nodes: vec![root],
            limits: Limits::default(),
            marker: PhantomData,
        }
    }
    pub fn input(&self) -> Port<C> {
        Port {
            scope: self.scope.clone(),
            index: 0,
            marker: PhantomData,
        }
    }
    pub fn limits(mut self, limits: Limits) -> Self {
        self.limits = limits;
        self
    }
    fn check<T>(&self, p: &Port<T>) -> Result<usize, Error> {
        if !Arc::ptr_eq(&self.scope, &p.scope) || p.index >= self.nodes.len() {
            return Err(Error::InvalidGraph("foreign_port"));
        }
        Ok(p.index)
    }
    fn add<O: Send + Sync + 'static>(
        &mut self,
        id: impl Into<String>,
        phase: Phase,
        deps: Vec<usize>,
        types: Vec<TypeId>,
        action: Action,
        kind: Kind,
    ) -> Result<Port<O>, Error> {
        if self.nodes.len() >= 257 {
            return Err(Error::InvalidGraph("node_limit"));
        }
        let index = self.nodes.len();
        self.nodes.push(Node {
            id: id.into(),
            phase,
            deps,
            input_types: types,
            output: TypeId::of::<O>(),
            action,
            dependencies: String::new(),
            condition: None,
            kind,
            budget: None,
            discardable: false,
        });
        Ok(Port {
            scope: self.scope.clone(),
            index,
            marker: PhantomData,
        })
    }
    pub fn sync<I, O, F>(
        &mut self,
        id: impl Into<String>,
        phase: Phase,
        port: &Port<I>,
        f: F,
    ) -> Result<Port<O>, Error>
    where
        I: Send + Sync + 'static,
        O: Send + Sync + 'static,
        F: Fn(Arc<I>) -> Result<O, Error> + Send + Sync + 'static,
    {
        let dep = self.check(port)?;
        self.add(
            id,
            phase,
            vec![dep],
            vec![TypeId::of::<I>()],
            Action::immediate(move |args| {
                Task::Immediate(f(input(args, 0)).map(|v| Arc::new(v) as Value))
            }),
            Kind::Work,
        )
    }
    pub fn asynchronous<I, O, F, Fut>(
        &mut self,
        id: impl Into<String>,
        phase: Phase,
        port: &Port<I>,
        f: F,
    ) -> Result<Port<O>, Error>
    where
        I: Send + Sync + 'static,
        O: Send + Sync + 'static,
        F: Fn(Arc<I>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<O, Error>> + Send + 'static,
    {
        let dep = self.check(port)?;
        self.add(
            id,
            phase,
            vec![dep],
            vec![TypeId::of::<I>()],
            Action::deferred(move |args| {
                let future = f(input(args, 0));
                Task::Deferred(Box::pin(async move {
                    future.await.map(|v| Arc::new(v) as Value)
                }))
            }),
            Kind::Work,
        )
    }
    pub fn join<A, B, O, F>(
        &mut self,
        id: impl Into<String>,
        phase: Phase,
        a: &Port<A>,
        b: &Port<B>,
        f: F,
    ) -> Result<Port<O>, Error>
    where
        A: Send + Sync + 'static,
        B: Send + Sync + 'static,
        O: Send + Sync + 'static,
        F: Fn(Arc<A>, Arc<B>) -> Result<O, Error> + Send + Sync + 'static,
    {
        let deps = vec![self.check(a)?, self.check(b)?];
        self.add(
            id,
            phase,
            deps,
            vec![TypeId::of::<A>(), TypeId::of::<B>()],
            Action::immediate(move |args| {
                Task::Immediate(f(input(args, 0), input(args, 1)).map(|v| Arc::new(v) as Value))
            }),
            Kind::Work,
        )
    }
    /// Exactly one active branch must supply a value. A skipped branch is not a success.
    pub fn choice<T: Send + Sync + 'static>(
        &mut self,
        id: impl Into<String>,
        phase: Phase,
        a: &Port<T>,
        b: &Port<T>,
    ) -> Result<Port<T>, Error> {
        self.choice_many(id, phase, &[a.clone(), b.clone()])
    }
    /// Join any number of alternatives. Exactly one must produce a value.
    pub fn choice_many<T: Send + Sync + 'static>(
        &mut self,
        id: impl Into<String>,
        phase: Phase,
        alternatives: &[Port<T>],
    ) -> Result<Port<T>, Error> {
        if alternatives.len() < 2 || alternatives.len() > 256 {
            return Err(Error::InvalidGraph("invalid_choice_width"));
        }
        let deps = alternatives
            .iter()
            .map(|port| self.check(port))
            .collect::<Result<Vec<_>, _>>()?;
        if deps.iter().copied().collect::<HashSet<_>>().len() != deps.len() {
            return Err(Error::InvalidGraph("duplicate_choice_input"));
        }
        let count = deps.len();
        self.add(
            id,
            phase,
            deps,
            vec![TypeId::of::<T>(); count],
            Action::immediate(move |args| {
                let mut active = args.iter().take(count).flatten().cloned();
                let first = active.next();
                Task::Immediate(match (first, active.next()) {
                    (Some(value), None) => Ok(value),
                    _ => Err(Error::Failed("ambiguous_or_empty_choice")),
                })
            }),
            Kind::Choice,
        )
    }
    /// Mandatory gate. Every dispatch must depend on every gate in this graph.
    pub fn gate<I, F>(
        &mut self,
        id: impl Into<String>,
        phase: Phase,
        port: &Port<I>,
        f: F,
    ) -> Result<Port<()>, Error>
    where
        I: Send + Sync + 'static,
        F: Fn(&I) -> Decision + Send + Sync + 'static,
    {
        let dep = self.check(port)?;
        self.add(
            id,
            phase,
            vec![dep],
            vec![TypeId::of::<I>()],
            Action::immediate(move |args| {
                Task::Immediate(match f(&*input(args, 0)) {
                    Decision::Continue => Ok(Arc::new(()) as Value),
                    Decision::Deny { reason, code } => Err(Error::Denied(reason, code)),
                })
            }),
            Kind::Gate,
        )
    }
    pub fn dispatch<I, O, F, Fut>(
        &mut self,
        id: impl Into<String>,
        port: &Port<I>,
        f: F,
    ) -> Result<Port<O>, Error>
    where
        I: Send + Sync + 'static,
        O: Send + Sync + 'static,
        F: Fn(Arc<I>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<O, Error>> + Send + 'static,
    {
        let result = self.asynchronous(id, Phase::Dispatch, port, f)?;
        self.nodes[result.index].kind = Kind::Dispatch;
        Ok(result)
    }
    pub fn gate_async<I, F, Fut>(
        &mut self,
        id: impl Into<String>,
        phase: Phase,
        port: &Port<I>,
        f: F,
    ) -> Result<Port<()>, Error>
    where
        I: Send + Sync + 'static,
        F: Fn(Arc<I>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Decision> + Send + 'static,
    {
        let result = self.asynchronous(id, phase, port, move |input| {
            let future = f(input);
            async move {
                match future.await {
                    Decision::Continue => Ok(()),
                    Decision::Deny { reason, code } => Err(Error::Denied(reason, code)),
                }
            }
        })?;
        self.nodes[result.index].kind = Kind::Gate;
        Ok(result)
    }
    pub fn depends_on<T, U>(&mut self, node: &Port<T>, dependency: &Port<U>) -> Result<(), Error> {
        let n = self.check(node)?;
        let d = self.check(dependency)?;
        if n == 0 || self.nodes[n].deps.contains(&d) {
            return Err(Error::InvalidGraph("duplicate_dependency"));
        }
        self.nodes[n].deps.push(d);
        Ok(())
    }
    pub fn when<T>(
        &mut self,
        node: &Port<T>,
        condition: &Port<bool>,
        expected: bool,
    ) -> Result<(), Error> {
        self.when_value(node, condition, expected)
    }
    /// Select a branch from a typed fact (for example an enum). The fact is read afresh each run.
    /// Guard every optional producer; an unguarded dependency is still required work.
    pub fn when_value<T, K>(
        &mut self,
        node: &Port<T>,
        condition: &Port<K>,
        expected: K,
    ) -> Result<(), Error>
    where
        K: Eq + Send + Sync + 'static,
    {
        let n = self.check(node)?;
        let d = self.check(condition)?;
        if n == 0 || self.nodes[n].kind == Kind::Gate || self.nodes[n].condition.is_some() {
            return Err(Error::InvalidGraph("conditional_gate_or_duplicate"));
        }
        self.nodes[n].condition = Some(Condition {
            dependency: d,
            matches: Arc::new(move |value| {
                value
                    .downcast_ref::<K>()
                    .is_some_and(|value| *value == expected)
            }),
        });
        if !self.nodes[n].deps.contains(&d) {
            self.nodes[n].deps.push(d);
        }
        Ok(())
    }
    pub fn budget<T>(&mut self, node: &Port<T>, duration: Duration) -> Result<(), Error> {
        let n = self.check(node)?;
        if n == 0 || duration.is_zero() || duration > Duration::from_secs(120) {
            return Err(Error::InvalidGraph("invalid_node_budget"));
        }
        self.nodes[n].budget = Some(duration);
        Ok(())
    }
    /// Opt in to omitting ordinary work when no requested output or retained control needs it.
    /// Its action, errors and side effects must be safe to omit. Gates and dispatch cannot opt in.
    pub fn discardable<T>(&mut self, node: &Port<T>) -> Result<(), Error> {
        let n = self.check(node)?;
        if n == 0 || matches!(self.nodes[n].kind, Kind::Gate | Kind::Dispatch) {
            return Err(Error::InvalidGraph("discardable_control"));
        }
        self.nodes[n].discardable = true;
        Ok(())
    }
    pub fn compile(mut self) -> Result<Flow<C>, Error> {
        if !(1..=256).contains(&self.limits.max_parallel)
            || !(1..=100000).contains(&self.limits.max_executions)
            || !(1..=100000).contains(&self.limits.max_running_nodes)
        {
            return Err(Error::InvalidGraph("invalid_limits"));
        }
        let mut ids = HashSet::new();
        let mut edges = 0;
        for (i, node) in self.nodes.iter().enumerate() {
            if node.id.is_empty()
                || node.id.len() > 64
                || !node
                    .id
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
                || !ids.insert(&node.id)
            {
                return Err(Error::InvalidGraph("invalid_or_duplicate_id"));
            }
            edges += node.deps.len();
            for (p, d) in node.deps.iter().enumerate() {
                if *d >= self.nodes.len() || *d == i || self.nodes[*d].phase > node.phase {
                    return Err(Error::InvalidGraph("invalid_dependency_phase"));
                }
                if p < node.input_types.len() && node.input_types[p] != self.nodes[*d].output {
                    return Err(Error::InvalidGraph("input_type_mismatch"));
                }
            }
        }
        if edges > 2048 {
            return Err(Error::InvalidGraph("edge_limit"));
        }
        fn ancestors(
            nodes: &[Node],
            i: usize,
            visiting: &mut HashSet<usize>,
            seen: &mut HashSet<usize>,
        ) -> Result<(), Error> {
            if !visiting.insert(i) {
                return Err(Error::InvalidGraph("cycle"));
            }
            for d in &nodes[i].deps {
                if !seen.contains(d) {
                    ancestors(nodes, *d, visiting, seen)?;
                    seen.insert(*d);
                }
            }
            visiting.remove(&i);
            Ok(())
        }
        for (i, node) in self.nodes.iter().enumerate() {
            let mut seen = HashSet::new();
            ancestors(&self.nodes, i, &mut HashSet::new(), &mut seen)?;
            if node.kind == Kind::Dispatch
                && self
                    .nodes
                    .iter()
                    .enumerate()
                    .any(|(g, n)| n.kind == Kind::Gate && !seen.contains(&g))
            {
                return Err(Error::InvalidGraph("dispatch_bypasses_mandatory_gate"));
            }
        }
        let dependencies: Vec<_> = self
            .nodes
            .iter()
            .map(|node| {
                node.deps
                    .iter()
                    .map(|d| self.nodes[*d].id.as_str())
                    .collect::<Vec<_>>()
                    .join(",")
            })
            .collect();
        for (node, dependencies) in self.nodes.iter_mut().zip(dependencies) {
            node.dependencies = dependencies;
        }
        let mut topology = Topology {
            dependents: vec![vec![]; self.nodes.len()],
            guarded: vec![vec![]; self.nodes.len()],
            gates: vec![],
        };
        for (i, node) in self.nodes.iter().enumerate() {
            for dependency in &node.deps {
                topology.dependents[*dependency].push(i);
            }
            if let Some(condition) = &node.condition {
                topology.guarded[condition.dependency].push(i);
            }
            if node.kind == Kind::Gate {
                topology.gates.push(i);
            }
        }
        Ok(Flow {
            scope: self.scope,
            nodes: Arc::new(self.nodes),
            topology,
            limits: self.limits,
            admission: Arc::new(Semaphore::new(self.limits.max_executions)),
            running: Arc::new(Semaphore::new(self.limits.max_running_nodes)),
            plan_cache: Mutex::new(plans::Cache::default()),
            marker: PhantomData,
        })
    }
}
impl<C: Context + Sync> Default for Builder<C> {
    fn default() -> Self {
        Self::new()
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Completion {
    Succeeded,
    Failed,
    Cancelled,
}
impl<C: Context + Sync> Flow<C> {
    /// A synchronous cleanup callback runs on success, failure and cancellation. It cannot replace a denial.
    pub async fn run_finally<F>(&self, context: Arc<C>, finally: F) -> Result<Run, Failure>
    where
        F: FnOnce(Completion) + Send,
    {
        self.run_finally_selected(context, None, finally).await
    }
    pub async fn run_plan_finally<F>(
        &self,
        context: Arc<C>,
        plan: &Prepared,
        finally: F,
    ) -> Result<Run, Failure>
    where
        F: FnOnce(Completion) + Send,
    {
        self.run_finally_selected(context, Some(plan), finally)
            .await
    }
    async fn run_finally_selected<F>(
        &self,
        context: Arc<C>,
        plan: Option<&Prepared>,
        finally: F,
    ) -> Result<Run, Failure>
    where
        F: FnOnce(Completion) + Send,
    {
        struct Finally<F: FnOnce(Completion)> {
            callback: Option<F>,
            outcome: Completion,
        }
        impl<F: FnOnce(Completion)> Drop for Finally<F> {
            fn drop(&mut self) {
                if let Some(f) = self.callback.take() {
                    let _ = std::panic::catch_unwind(AssertUnwindSafe(|| f(self.outcome)));
                }
            }
        }
        let mut guard = Finally {
            callback: Some(finally),
            outcome: Completion::Cancelled,
        };
        let result = self.run_selected(context, plan).await;
        guard.outcome = if result.is_ok() {
            Completion::Succeeded
        } else {
            Completion::Failed
        };
        result
    }

    pub async fn run(&self, context: Arc<C>) -> Result<Run, Failure> {
        self.run_selected(context, None).await
    }
    pub async fn run_plan(&self, context: Arc<C>, plan: &Prepared) -> Result<Run, Failure> {
        self.run_selected(context, Some(plan)).await
    }
    async fn run_selected(&self, context: Arc<C>, plan: Option<&Prepared>) -> Result<Run, Failure> {
        let deadline: Instant = context.deadline().into();
        let start = Instant::now();
        let mut reports: Vec<_> = self
            .nodes
            .iter()
            .map(|n| NodeReport {
                id: n.id.clone(),
                state: State::Pending,
                dependency_wait: Duration::ZERO,
                scheduler_wait: Duration::ZERO,
                elapsed: Duration::ZERO,
                skip_reason: None,
                scheduling: None,
            })
            .collect();
        if plan.is_some_and(|plan| !plan.belongs_to(self)) {
            return Err(Failure {
                error: Error::InvalidGraph("foreign_plan"),
                nodes: reports,
            });
        }
        let _admission = match self.admission.clone().try_acquire_owned() {
            Ok(p) => p,
            Err(_) => {
                return Err(Failure {
                    error: Error::Overloaded,
                    nodes: reports,
                })
            }
        };
        let mut values = vec![None; self.nodes.len()];
        values[0] = Some(context as Value);
        reports[0].state = State::Succeeded;
        let mut tasks: FuturesUnordered<Running> = FuturesUnordered::new();
        let mut schedule = Schedule::new(&self.nodes);
        let span = tracing::info_span!(
            "workflow.execution",
            workflow.nodes = self.nodes.len() - 1,
            workflow.branch_checks = tracing::field::Empty,
            workflow.pruned_nodes = tracing::field::Empty,
            workflow.prepared = plan.is_some(),
            workflow.outcome = tracing::field::Empty
        );
        struct ExecutionGuard(Option<tracing::Span>);
        impl Drop for ExecutionGuard {
            fn drop(&mut self) {
                if let Some(span) = &self.0 {
                    span.record("workflow.outcome", "cancelled");
                }
            }
        }
        let mut guard = ExecutionGuard(Some(span.clone()));
        let execution = async {
            if let Some(plan) = plan {
                for i in 1..self.nodes.len() {
                    if !plan.active.contains(i) {
                        schedule.skip(i, SkipReason::UnrequestedOutput, &mut reports, start);
                    }
                }
            }
            schedule.complete(0, &self.nodes, &self.topology, &values, &mut reports, start)?;
            loop {
                if Instant::now() >= deadline {
                    return Err(Error::Deadline);
                }
                while tasks.len() < self.limits.max_parallel {
                    let Some(i) = schedule.ready.pop_first() else {
                        break;
                    };
                    let node = &self.nodes[i];
                    let ready = schedule.ready_at[i].expect("ready node");
                    reports[i].dependency_wait = ready.duration_since(start);
                    // Choices cannot bypass a mandatory gate through a skipped branch.
                    if node.kind == Kind::Dispatch
                        && self
                            .topology
                            .gates
                            .iter()
                            .any(|g| reports[*g].state != State::Succeeded)
                    {
                        return Err(Error::Denied(
                            Reason::Forbidden,
                            "mandatory_gate_not_satisfied",
                        ));
                    }
                    reports[i].state = State::Running;
                    schedule.pending -= 1;
                    let inline_permit = if node.action.immediate {
                        self.running.clone().try_acquire_owned().ok()
                    } else {
                        None
                    };
                    let scheduling = if inline_permit.is_some() {
                        Scheduling::Inline
                    } else if tasks.is_empty() && schedule.ready.is_empty() {
                        Scheduling::Serial
                    } else {
                        Scheduling::Parallel
                    };
                    reports[i].scheduling = Some(scheduling);
                    let node_span = tracing::info_span!(
                        "workflow.node", node.id = %node.id, node.phase = ?node.phase,
                        node.dependencies = %node.dependencies, node.scheduling = ?scheduling,
                        node.outcome = tracing::field::Empty,
                        node.scheduler_wait_ns = tracing::field::Empty,
                        node.dependency_wait_ns = ready.duration_since(start).as_nanos().min(u64::MAX as u128) as u64,
                        node.execution_ns = tracing::field::Empty
                    );
                    let step = Step {
                        index: i,
                        ready,
                        end: node.budget.map_or(deadline, |d| deadline.min(ready + d)),
                        args: Arguments::new(node, &values),
                        action: node.action.clone(),
                        semaphore: self.running.clone(),
                        timer: NodeTimer {
                            span: node_span.clone(),
                            done: false,
                        },
                    };
                    let completed = if let Some(permit) = inline_permit {
                        node_span.in_scope(|| step.inline(permit))
                    } else if scheduling == Scheduling::Serial {
                        step.run().instrument(node_span).await
                    } else {
                        tasks.push(Box::pin(step.run().instrument(node_span)));
                        continue;
                    };
                    accept(
                        completed,
                        &mut schedule,
                        &self.nodes,
                        &self.topology,
                        &mut values,
                        &mut reports,
                        start,
                    )?;
                }
                if schedule.pending == 0 && tasks.is_empty() {
                    return Ok(());
                }
                if tasks.is_empty() {
                    return Err(Error::InvalidGraph("unreachable_nodes"));
                }
                // A ready node's budget must expire even while a sibling occupies the last slot.
                let waiting_deadline = schedule
                    .ready
                    .iter()
                    .filter_map(|i| Some(schedule.ready_at[i]? + self.nodes[i].budget?))
                    .min()
                    .unwrap_or(deadline)
                    .min(deadline);
                match timeout_at(waiting_deadline, tasks.next()).await {
                    Ok(Some(completed)) => {
                        accept(
                            completed,
                            &mut schedule,
                            &self.nodes,
                            &self.topology,
                            &mut values,
                            &mut reports,
                            start,
                        )?;
                    }
                    Ok(None) => {}
                    Err(_) => {
                        for (i, node) in self.nodes.iter().enumerate() {
                            if reports[i].state == State::Pending
                                && schedule.ready_at[i].is_some_and(|ready| {
                                    node.budget
                                        .is_some_and(|budget| ready + budget <= Instant::now())
                                })
                            {
                                reports[i].state = State::Failed;
                                reports[i].scheduler_wait =
                                    schedule.ready_at[i].expect("ready").elapsed();
                                tracing::warn!(node.id = %node.id, node.outcome = "timed_out",
                                    node.scheduler_wait_ns = reports[i].scheduler_wait.as_nanos().min(u64::MAX as u128) as u64,
                                    "workflow scheduler budget expired");
                            }
                        }
                        return Err(Error::Deadline);
                    }
                }
            }
        };
        let result = execution.instrument(span.clone()).await;
        // Dropping sibling futures closes scopes and releases cooperative resources; no detached tasks.
        drop(tasks);
        guard.0 = None;
        span.record("workflow.branch_checks", schedule.branch_checks as u64);
        span.record("workflow.pruned_nodes", schedule.pruned as u64);
        match result {
            Ok(()) => {
                span.record("workflow.outcome", "succeeded");
                Ok(Run {
                    scope: self.scope.clone(),
                    values,
                    nodes: reports,
                })
            }
            Err(error) => {
                span.record("workflow.outcome", error.outcome());
                for n in &mut reports {
                    if matches!(n.state, State::Pending | State::Running) {
                        n.state = State::Cancelled;
                    }
                }
                Err(Failure {
                    error,
                    nodes: reports,
                })
            }
        }
    }
}
/// Bounded non-preemptible work: a running closure retains capacity even after caller timeout.
#[derive(Clone)]
pub struct CpuPool(Arc<Semaphore>);
impl CpuPool {
    pub fn new(capacity: usize) -> Result<Self, Error> {
        if !(1..=256).contains(&capacity) {
            return Err(Error::InvalidGraph("invalid_cpu_capacity"));
        }
        Ok(Self(Arc::new(Semaphore::new(capacity))))
    }
    pub async fn run<T, F>(&self, deadline: Instant, f: F) -> Result<T, Error>
    where
        T: Send + 'static,
        F: FnOnce() -> Result<T, Error> + Send + 'static,
    {
        if Instant::now() >= deadline {
            return Err(Error::Deadline);
        }
        let permit = timeout_at(deadline, self.0.clone().acquire_owned())
            .await
            .map_err(|_| Error::Deadline)?
            .map_err(|_| Error::Overloaded)?;
        if Instant::now() >= deadline {
            return Err(Error::Deadline);
        }
        let task = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            if Instant::now() >= deadline {
                return Err(Error::Deadline);
            }
            f()
        });
        struct CancelQueued(tokio::task::AbortHandle);
        impl Drop for CancelQueued {
            fn drop(&mut self) {
                self.0.abort();
            }
        }
        // Tokio can cancel a queued blocking closure; running work still owns its permit.
        let _cancel = CancelQueued(task.abort_handle());
        timeout_at(deadline, task)
            .await
            .map_err(|_| Error::Deadline)?
            .map_err(|_| Error::Panic)?
    }
}
/// Race/quorum work groups cancel unfinished cooperative futures when enough results arrive.
/// These are task helpers; they do not change mandatory graph gates.
pub type FutureTask<T> = Pin<Box<dyn Future<Output = Result<T, Error>> + Send>>;
pub async fn quorum<T>(
    tasks: Vec<FutureTask<T>>,
    required: usize,
    parallel: usize,
) -> Result<Vec<T>, Error> {
    if tasks.is_empty()
        || tasks.len() > 256
        || required == 0
        || required > tasks.len()
        || parallel == 0
        || parallel > 256
    {
        return Err(Error::InvalidGraph("invalid_quorum"));
    }
    let mut remaining = tasks.len();
    let mut running = futures_util::stream::iter(tasks).buffer_unordered(parallel);
    let mut results = Vec::new();
    while let Some(result) = running.next().await {
        remaining -= 1;
        if let Ok(value) = result {
            results.push(value);
        }
        if results.len() == required {
            return Ok(results);
        }
        if results.len() + remaining < required {
            return Err(Error::Failed("quorum_unavailable"));
        }
    }
    Err(Error::Failed("quorum_unavailable"))
}
/// A stream/handle has one consumer even when its graph value has multiple references.
pub struct Exclusive<T>(std::sync::Mutex<Option<T>>);
impl<T> Exclusive<T> {
    pub fn new(value: T) -> Self {
        Self(std::sync::Mutex::new(Some(value)))
    }
    pub fn take(&self) -> Result<T, Error> {
        self.0
            .lock()
            .map_err(|_| Error::Failed("resource_poisoned"))?
            .take()
            .ok_or(Error::Failed("resource_already_consumed"))
    }
}
