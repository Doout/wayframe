use super::{
    Action, Completed, Error, Kind, Node, NodeReport, Priority, SkipReason, State, Topology, Value,
};
use futures_util::FutureExt;
use std::{collections::VecDeque, panic::AssertUnwindSafe, sync::Arc, time::Duration};
use tokio::{
    sync::Semaphore,
    time::{timeout_at, Instant},
};
use wayframe_core::{Reason, Task};
pub(super) struct NodeTimer {
    pub(super) span: tracing::Span,
    pub(super) done: bool,
    pub(super) ready: Instant,
    pub(super) started: Option<Instant>,
}
/// The graph has at most 257 nodes, including input. Lowest-index scheduling remains deterministic.
#[derive(Clone, Copy, Default, PartialEq, Eq, Hash)]
pub(super) struct NodeSet([u64; 5]);
impl NodeSet {
    pub(super) fn contains(&self, i: usize) -> bool {
        self.0[i / 64] & (1 << (i % 64)) != 0
    }
    pub(super) fn insert(&mut self, i: usize) {
        self.0[i / 64] |= 1 << (i % 64);
    }
    pub(super) fn remove(&mut self, i: usize) {
        self.0[i / 64] &= !(1 << (i % 64));
    }
    pub(super) fn pop_first(&mut self) -> Option<usize> {
        for (word, bits) in self.0.iter_mut().enumerate() {
            if *bits != 0 {
                let bit = bits.trailing_zeros() as usize;
                *bits &= *bits - 1;
                return Some(word * 64 + bit);
            }
        }
        None
    }
    pub(super) fn is_empty(&self) -> bool {
        self.0.iter().all(|word| *word == 0)
    }
    pub(super) fn iter(&self) -> impl Iterator<Item = usize> + '_ {
        self.0.iter().enumerate().flat_map(|(word, bits)| {
            let mut bits = *bits;
            std::iter::from_fn(move || {
                if bits == 0 {
                    return None;
                }
                let bit = bits.trailing_zeros() as usize;
                bits &= bits - 1;
                Some(word * 64 + bit)
            })
        })
    }
}
/// Ordinary one/two-input nodes avoid heap allocation. Futures still own their immutable inputs.
pub(super) enum Arguments {
    Small([Option<Value>; 2], usize),
    Many(Vec<Option<Value>>),
}
impl Arguments {
    pub(super) fn new(node: &Node, values: &[Option<Value>]) -> Self {
        let inputs = &node.deps[..node.input_types.len()];
        if inputs.len() <= 2 {
            let mut args = [None, None];
            for (slot, index) in args.iter_mut().zip(inputs) {
                *slot = values[*index].clone();
            }
            Self::Small(args, inputs.len())
        } else {
            Self::Many(inputs.iter().map(|index| values[*index].clone()).collect())
        }
    }
    pub(super) fn as_slice(&self) -> &[Option<Value>] {
        match self {
            Self::Small(args, count) => &args[..*count],
            Self::Many(args) => args,
        }
    }
}
/// Per-invocation partial evaluation. Resolved facts remove inactive paths before scheduling them.
pub(super) struct Schedule {
    pub(super) remaining: Vec<usize>,
    pub(super) ready: NodeSet,
    pub(super) ready_at: Vec<Option<Instant>>,
    pub(super) settled: VecDeque<usize>,
    pub(super) pending: usize,
    pub(super) branch_checks: usize,
    pub(super) pruned: usize,
    measured: bool,
}
impl Schedule {
    pub(super) fn new(nodes: &[Node], measured: bool) -> Self {
        Self {
            remaining: nodes.iter().map(|node| node.deps.len()).collect(),
            ready: NodeSet::default(),
            ready_at: vec![None; nodes.len()],
            settled: VecDeque::with_capacity(nodes.len()),
            pending: nodes.len() - 1,
            branch_checks: 0,
            pruned: 0,
            measured,
        }
    }
    pub(super) fn next(
        &mut self,
        priority: Priority,
        scores: &[u64],
        nodes: &[Node],
        deadline: Instant,
    ) -> Option<usize> {
        if priority == Priority::Stable {
            return self.ready.pop_first();
        }
        let due = |i: usize| {
            nodes[i].budget.map_or(deadline, |budget| {
                deadline.min(self.ready_at[i].expect("ready") + budget)
            })
        };
        let selected = self.ready.iter().min_by(|a, b| {
            (nodes[*b].kind == Kind::Gate)
                .cmp(&(nodes[*a].kind == Kind::Gate))
                .then_with(|| due(*a).cmp(&due(*b)))
                .then_with(|| scores[*b].cmp(&scores[*a]))
                .then_with(|| a.cmp(b))
        })?;
        self.ready.remove(selected);
        Some(selected)
    }
    pub(super) fn skip(
        &mut self,
        i: usize,
        reason: SkipReason,
        reports: &mut [NodeReport],
        start: Instant,
    ) {
        self.ready.remove(i);
        self.ready_at[i] = None;
        self.pending -= 1;
        self.pruned += 1;
        reports[i].state = State::Skipped;
        reports[i].dependency_wait = if reason == SkipReason::UnrequestedOutput {
            Duration::ZERO
        } else {
            start.elapsed()
        };
        reports[i].skip_reason = Some(reason);
        // Pruning is observable but never takes a node execution permit or invokes its action.
        let span = tracing::info_span!("workflow.node", node.id = %reports[i].id,
            node.outcome = "skipped", node.skip_reason = ?reason,
            node.dependency_wait_ns = reports[i].dependency_wait.as_nanos().min(u64::MAX as u128) as u64,
            node.scheduler_wait_ns = 0u64, node.execution_ns = 0u64);
        drop(span);
    }
    pub(super) fn complete(
        &mut self,
        completed: usize,
        nodes: &[Node],
        topology: &Topology,
        values: &[Option<Value>],
        reports: &mut [NodeReport],
        start: Instant,
    ) -> Result<(), Error> {
        self.settled.push_back(completed);
        while let Some(dependency) = self.settled.pop_front() {
            // Resolve guards first, even when their other dependencies are still running.
            for &i in &topology.guarded[dependency] {
                if reports[i].state != State::Pending {
                    continue;
                }
                let condition = nodes[i].condition.as_ref().expect("compiled guard");
                let span = tracing::info_span!("workflow.branch", node.id = %nodes[i].id,
                    branch.selector = %nodes[dependency].id, branch.selected = tracing::field::Empty);
                self.branch_checks += 1;
                let selected = span
                    .in_scope(|| {
                        std::panic::catch_unwind(AssertUnwindSafe(|| {
                            values[dependency]
                                .as_ref()
                                .is_some_and(|value| (condition.matches)(value))
                        }))
                    })
                    .map_err(|_| Error::Panic)?;
                span.record("branch.selected", selected);
                drop(span);
                if !selected {
                    self.skip(i, SkipReason::BranchNotSelected, reports, start);
                    self.settled.push_back(i);
                }
            }
            for &i in &topology.dependents[dependency] {
                if reports[i].state != State::Pending {
                    continue;
                }
                if reports[dependency].state == State::Skipped && nodes[i].kind != Kind::Choice {
                    self.skip(i, SkipReason::DependencySkipped, reports, start);
                    // A graph may be embedded before dispatch in another pipeline. Its mandatory
                    // gates must fail closed here, even when it has no internal dispatch node.
                    if nodes[i].kind == Kind::Gate {
                        return Err(Error::Denied(
                            Reason::Forbidden,
                            "mandatory_gate_not_satisfied",
                        ));
                    }
                    self.settled.push_back(i);
                } else {
                    self.remaining[i] -= 1;
                    if self.remaining[i] == 0 {
                        self.ready.insert(i);
                        self.ready_at[i] = Some(Instant::now());
                    }
                }
            }
        }
        Ok(())
    }
}
impl Drop for NodeTimer {
    fn drop(&mut self) {
        if !self.done {
            self.span.record("node.outcome", "cancelled");
            self.span.record(
                "node.scheduler_wait_ns",
                self.started
                    .map_or_else(
                        || self.ready.elapsed(),
                        |started| started.duration_since(self.ready),
                    )
                    .as_nanos()
                    .min(u64::MAX as u128) as u64,
            );
            self.span.record(
                "node.execution_ns",
                self.started
                    .map_or(Duration::ZERO, |started| started.elapsed())
                    .as_nanos()
                    .min(u64::MAX as u128) as u64,
            );
        }
    }
}
pub(super) struct Step {
    pub(super) index: usize,
    pub(super) ready: Instant,
    pub(super) end: Instant,
    pub(super) args: Arguments,
    pub(super) action: Action,
    pub(super) semaphore: Arc<Semaphore>,
    pub(super) timer: NodeTimer,
}
impl Step {
    pub(super) fn finish(
        mut self,
        started: Option<Instant>,
        value: Result<Value, Error>,
    ) -> Completed {
        // A synchronous native function cannot be preempted, but its late result cannot continue.
        let value = if Instant::now() >= self.end {
            Err(Error::Deadline)
        } else {
            value
        };
        let scheduler_wait =
            started.map_or_else(|| self.ready.elapsed(), |s| s.duration_since(self.ready));
        let elapsed = started.map_or(Duration::ZERO, |s| s.elapsed());
        self.timer.span.record(
            "node.scheduler_wait_ns",
            scheduler_wait.as_nanos().min(u64::MAX as u128) as u64,
        );
        self.timer.span.record(
            "node.execution_ns",
            elapsed.as_nanos().min(u64::MAX as u128) as u64,
        );
        self.timer.span.record(
            "node.outcome",
            value.as_ref().map_or_else(|e| e.outcome(), |_| "succeeded"),
        );
        self.timer.done = true;
        (self.index, scheduler_wait, elapsed, value)
    }
    pub(super) fn inline(mut self, _permit: tokio::sync::OwnedSemaphorePermit) -> Completed {
        let mut started = None;
        let value = if Instant::now() >= self.end {
            Err(Error::Deadline)
        } else {
            started = Some(Instant::now());
            self.timer.started = started;
            std::panic::catch_unwind(AssertUnwindSafe(|| {
                match (self.action.invoke)(self.args.as_slice()) {
                    Task::Immediate(value) => value,
                    Task::Deferred(_) => Err(Error::InvalidGraph("invalid_immediate_action")),
                }
            }))
            .unwrap_or(Err(Error::Panic))
        };
        self.finish(started, value)
    }
    pub(super) async fn run(mut self) -> Completed {
        let mut started = None;
        let execute = async {
            let _permit = self
                .semaphore
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| Error::Overloaded)?;
            if Instant::now() >= self.end {
                return Err(Error::Deadline);
            }
            started = Some(Instant::now());
            self.timer.started = started;
            (self.action.invoke)(self.args.as_slice()).resolve().await
        };
        let value = match timeout_at(self.end, AssertUnwindSafe(execute).catch_unwind()).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(Error::Panic),
            Err(_) => Err(Error::Deadline),
        };
        self.finish(started, value)
    }
}
pub(super) fn accept(
    completed: Completed,
    schedule: &mut Schedule,
    nodes: &[Node],
    topology: &Topology,
    values: &mut [Option<Value>],
    reports: &mut [NodeReport],
    start: Instant,
) -> Result<(), Error> {
    let (i, scheduler_wait, elapsed, value) = completed;
    reports[i].scheduler_wait = scheduler_wait;
    reports[i].elapsed = elapsed;
    match value {
        Ok(value) => {
            if schedule.measured {
                nodes[i].observed.record(elapsed);
            }
            values[i] = Some(value);
            reports[i].state = State::Succeeded;
            schedule.complete(i, nodes, topology, values, reports, start)
        }
        Err(error) => {
            reports[i].state = State::Failed;
            Err(error)
        }
    }
}
