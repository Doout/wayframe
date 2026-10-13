//! Bounded typed DAG execution. Each node owns its output; joins never merge mutable contexts.
use futures_util::{stream::FuturesUnordered, FutureExt, StreamExt};
use std::{
    any::{Any, TypeId},
    collections::HashSet,
    fmt,
    future::Future,
    marker::PhantomData,
    panic::AssertUnwindSafe,
    pin::Pin,
    sync::Arc,
    time::Duration,
};
use tokio::{
    sync::Semaphore,
    time::{timeout_at, Instant},
};
use tracing::Instrument;
use wayframe_core::{Context, Decision, Phase, Reason, Task};
type Value = Arc<dyn Any + Send + Sync>;
type Action = Arc<dyn Fn(Vec<Option<Value>>) -> Task<Value, Error> + Send + Sync>;
type Running =
    Pin<Box<dyn Future<Output = (usize, Duration, Duration, Result<Value, Error>)> + Send>>;

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
    condition: Option<(usize, bool)>,
    kind: Kind,
    budget: Option<Duration>,
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
    limits: Limits,
    admission: Arc<Semaphore>,
    running: Arc<Semaphore>,
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
            action: Arc::new(|_| Task::Immediate(Err(Error::Failed("input_not_executable")))),
            condition: None,
            kind: Kind::Work,
            budget: None,
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
            condition: None,
            kind,
            budget: None,
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
            Arc::new(move |args| Task::Immediate(f(input(&args, 0)).map(|v| Arc::new(v) as Value))),
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
            Arc::new(move |args| {
                let future = f(input(&args, 0));
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
            Arc::new(move |args| {
                Task::Immediate(f(input(&args, 0), input(&args, 1)).map(|v| Arc::new(v) as Value))
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
        let deps = vec![self.check(a)?, self.check(b)?];
        self.add(
            id,
            phase,
            deps,
            vec![TypeId::of::<T>(); 2],
            Arc::new(|args| {
                let mut active = args.into_iter().take(2).flatten();
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
            Arc::new(move |args| {
                Task::Immediate(match f(&*input(&args, 0)) {
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
        let n = self.check(node)?;
        let d = self.check(condition)?;
        if n == 0 || self.nodes[n].kind == Kind::Gate || self.nodes[n].condition.is_some() {
            return Err(Error::InvalidGraph("conditional_gate_or_duplicate"));
        }
        self.nodes[n].condition = Some((d, expected));
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
    pub fn compile(self) -> Result<Flow<C>, Error> {
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
        Ok(Flow {
            scope: self.scope,
            nodes: Arc::new(self.nodes),
            limits: self.limits,
            admission: Arc::new(Semaphore::new(self.limits.max_executions)),
            running: Arc::new(Semaphore::new(self.limits.max_running_nodes)),
            marker: PhantomData,
        })
    }
}
impl<C: Context + Sync> Default for Builder<C> {
    fn default() -> Self {
        Self::new()
    }
}
struct NodeTimer {
    span: tracing::Span,
    done: bool,
}
impl Drop for NodeTimer {
    fn drop(&mut self) {
        if !self.done {
            self.span.record("node.outcome", "cancelled");
        }
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
        let result = self.run(context).await;
        guard.outcome = if result.is_ok() {
            Completion::Succeeded
        } else {
            Completion::Failed
        };
        result
    }

    pub async fn run(&self, context: Arc<C>) -> Result<Run, Failure> {
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
            })
            .collect();
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
        let mut pending = self.nodes.len() - 1;
        let mut tasks: FuturesUnordered<Running> = FuturesUnordered::new();
        let mut ready_at: Vec<Option<Instant>> = vec![None; self.nodes.len()];
        let span = tracing::info_span!(
            "workflow.execution",
            workflow.nodes = self.nodes.len() - 1,
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
            loop {
                if Instant::now() >= deadline {
                    return Err(Error::Deadline);
                }
                let mut changed = false;
                for i in 1..self.nodes.len() {
                    let node = &self.nodes[i];
                    if reports[i].state != State::Pending
                        || node
                            .deps
                            .iter()
                            .any(|d| matches!(reports[*d].state, State::Pending | State::Running))
                    {
                        continue;
                    }
                    let ready = *ready_at[i].get_or_insert_with(Instant::now);
                    reports[i].dependency_wait = ready.duration_since(start);
                    let skipped = node
                        .deps
                        .iter()
                        .any(|d| reports[*d].state == State::Skipped)
                        && node.kind != Kind::Choice;
                    let condition = node.condition.is_none_or(|(d, expected)| {
                        values[d]
                            .as_ref()
                            .and_then(|v| v.downcast_ref::<bool>())
                            .is_some_and(|v| *v == expected)
                    });
                    if skipped || !condition {
                        reports[i].state = State::Skipped;
                        pending -= 1;
                        changed = true;
                        continue;
                    }
                    if tasks.len() >= self.limits.max_parallel {
                        continue;
                    }
                    // Choices cannot bypass a mandatory gate through a skipped branch.
                    if node.kind == Kind::Dispatch
                        && self.nodes.iter().enumerate().any(|(g, n)| {
                            n.kind == Kind::Gate && reports[g].state != State::Succeeded
                        })
                    {
                        return Err(Error::Denied(
                            Reason::Forbidden,
                            "mandatory_gate_not_satisfied",
                        ));
                    }
                    reports[i].state = State::Running;
                    pending -= 1;
                    changed = true;
                    let args = node.deps.iter().map(|d| values[*d].clone()).collect();
                    let action = node.action.clone();
                    let semaphore = self.running.clone();
                    let end = node.budget.map_or(deadline, |d| deadline.min(ready + d));
                    let dependencies = node
                        .deps
                        .iter()
                        .map(|d| self.nodes[*d].id.as_str())
                        .collect::<Vec<_>>()
                        .join(",");
                    let node_span = tracing::info_span!(
                        "workflow.node", node.id = %node.id, node.phase = ?node.phase,
                        node.dependencies = %dependencies, node.outcome = tracing::field::Empty,
                        node.scheduler_wait_ns = tracing::field::Empty,
                        node.dependency_wait_ns = ready.duration_since(start).as_nanos().min(u64::MAX as u128) as u64,
                        node.execution_ns = tracing::field::Empty
                    );
                    let instrumentation = node_span.clone();
                    let timer = NodeTimer {
                        span: node_span.clone(),
                        done: false,
                    };
                    tasks.push(Box::pin(
                        async move {
                            let mut timer = timer;
                            let mut execution_start = None;
                            let execute = async {
                                let _permit = semaphore
                                    .acquire_owned()
                                    .await
                                    .map_err(|_| Error::Overloaded)?;
                                // Budgets also include waiting for this flow's parallel slots.
                                if Instant::now() >= end {
                                    return Err(Error::Deadline);
                                }
                                execution_start = Some(Instant::now());
                                action(args).resolve().await
                            };
                            let value =
                                match timeout_at(end, AssertUnwindSafe(execute).catch_unwind())
                                    .await
                                {
                                    Ok(Ok(result)) => result,
                                    Ok(Err(_)) => Err(Error::Panic),
                                    Err(_) => Err(Error::Deadline),
                                };
                            let scheduler_wait = execution_start
                                .map_or_else(|| ready.elapsed(), |s| s.duration_since(ready));
                            let elapsed = execution_start.map_or(Duration::ZERO, |s| s.elapsed());
                            node_span.record(
                                "node.scheduler_wait_ns",
                                scheduler_wait.as_nanos().min(u64::MAX as u128) as u64,
                            );
                            node_span.record(
                                "node.execution_ns",
                                elapsed.as_nanos().min(u64::MAX as u128) as u64,
                            );
                            node_span.record(
                                "node.outcome",
                                value.as_ref().map_or_else(|e| e.outcome(), |_| "succeeded"),
                            );
                            timer.done = true;
                            (i, scheduler_wait, elapsed, value)
                        }
                        .instrument(instrumentation),
                    ));
                }
                if pending == 0 && tasks.is_empty() {
                    return Ok(());
                }
                if tasks.is_empty() && !changed {
                    return Err(Error::InvalidGraph("unreachable_nodes"));
                }
                // A ready node's budget must expire even while a sibling occupies the last slot.
                let waiting_deadline = self
                    .nodes
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| reports[*i].state == State::Pending)
                    .filter_map(|(i, n)| Some(ready_at[i]? + n.budget?))
                    .min()
                    .unwrap_or(deadline)
                    .min(deadline);
                match timeout_at(waiting_deadline, tasks.next()).await {
                    Ok(Some((i, scheduler_wait, elapsed, value))) => {
                        reports[i].scheduler_wait = scheduler_wait;
                        reports[i].elapsed = elapsed;
                        match value {
                            Ok(value) => {
                                values[i] = Some(value);
                                reports[i].state = State::Succeeded;
                            }
                            Err(error) => {
                                reports[i].state = State::Failed;
                                return Err(error);
                            }
                        }
                    }
                    Ok(None) => {}
                    Err(_) => {
                        for (i, node) in self.nodes.iter().enumerate() {
                            if reports[i].state == State::Pending
                                && ready_at[i].is_some_and(|ready| {
                                    node.budget
                                        .is_some_and(|budget| ready + budget <= Instant::now())
                                })
                            {
                                reports[i].state = State::Failed;
                                reports[i].scheduler_wait = ready_at[i].expect("ready").elapsed();
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
        let permit = timeout_at(deadline, self.0.clone().acquire_owned())
            .await
            .map_err(|_| Error::Deadline)?
            .map_err(|_| Error::Overloaded)?;
        let task = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            f()
        });
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
