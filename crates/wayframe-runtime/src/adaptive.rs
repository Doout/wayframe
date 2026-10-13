//! Per-invocation execution selection at a declared phase boundary. No partial work is replayed.
use crate::Pipeline;
use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Instant,
};
use wayframe_core::{Block, Context, Decision, Execution, Phase, PlanError};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Path {
    Linear,
    Graph,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SelectionReason {
    SmallWorkload,
    ParallelWork,
    ExplicitRule,
    AdmissionPressure,
}
#[derive(Clone, Copy, Debug)]
pub struct Selection {
    pub path: Path,
    pub reason: SelectionReason,
}
impl Selection {
    /// Use an operator-chosen width threshold, rather than a latency guess or client override.
    pub fn for_width(independent_steps: usize, threshold: usize) -> Self {
        if independent_steps >= threshold.max(2) {
            Self {
                path: Path::Graph,
                reason: SelectionReason::ParallelWork,
            }
        } else {
            Self {
                path: Path::Linear,
                reason: SelectionReason::SmallWorkload,
            }
        }
    }
}
#[derive(Clone, Copy, Debug)]
pub struct Statistics {
    pub linear: u64,
    pub graph: u64,
}
type Selector<C> = Arc<dyn Fn(&C) -> Selection + Send + Sync>;

/// Both paths must implement the same required controls. Shared gates belong outside this block.
/// Selection happens when execution reaches this boundary, using the context produced so far.
pub struct Adaptive<C: Context> {
    id: String,
    phase: Phase,
    linear: Pipeline<C>,
    graph: Arc<dyn Block<C>>,
    select: Selector<C>,
    linear_count: AtomicU64,
    graph_count: AtomicU64,
}
impl<C: Context> Adaptive<C> {
    pub fn new(
        id: impl Into<String>,
        phase: Phase,
        linear: Pipeline<C>,
        graph: Arc<dyn Block<C>>,
        select: impl Fn(&C) -> Selection + Send + Sync + 'static,
    ) -> Result<Self, PlanError> {
        let id = id.into();
        if id.is_empty()
            || id.len() > 64
            || !id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
        {
            return Err(PlanError::InvalidId(id));
        }
        if graph.phase() != phase || linear.plan().blocks().iter().any(|b| b.phase() != phase) {
            return Err(PlanError::InvalidOrder(id));
        }
        Ok(Self {
            id,
            phase,
            linear,
            graph,
            select: Arc::new(select),
            linear_count: AtomicU64::new(0),
            graph_count: AtomicU64::new(0),
        })
    }
    pub fn statistics(&self) -> Statistics {
        Statistics {
            linear: self.linear_count.load(Ordering::Relaxed),
            graph: self.graph_count.load(Ordering::Relaxed),
        }
    }
}
impl<C: Context> Block<C> for Adaptive<C> {
    fn id(&self) -> &str {
        &self.id
    }
    fn phase(&self) -> Phase {
        self.phase
    }
    fn execute<'a>(&'a self, context: &'a mut C) -> Execution<'a> {
        let deadline = context.deadline();
        if Instant::now() >= deadline {
            return Execution::Immediate(Decision::expired());
        }
        #[cfg(feature = "tracing")]
        let selection_span = tracing::info_span!("workflow.select", block.id = %self.id, execution.path = tracing::field::Empty, selection.reason = tracing::field::Empty);
        #[cfg(feature = "tracing")]
        let selection = selection_span.in_scope(|| (self.select)(context));
        #[cfg(not(feature = "tracing"))]
        let selection = (self.select)(context);
        #[cfg(feature = "tracing")]
        {
            selection_span.record(
                "execution.path",
                match selection.path {
                    Path::Linear => "linear",
                    Path::Graph => "graph",
                },
            );
            selection_span.record("selection.reason", tracing::field::debug(selection.reason));
            drop(selection_span);
        }
        if Instant::now() >= deadline {
            return Execution::Immediate(Decision::expired());
        }
        match selection.path {
            Path::Linear => {
                self.linear_count.fetch_add(1, Ordering::Relaxed);
            }
            Path::Graph => {
                self.graph_count.fetch_add(1, Ordering::Relaxed);
            }
        }
        Execution::Deferred(Box::pin(async move {
            let work = async {
                if Instant::now() >= deadline {
                    return Decision::expired();
                }
                match selection.path {
                    Path::Linear => self.linear.evaluate(context).await,
                    Path::Graph => self.graph.execute(context).resolve().await,
                }
            };
            #[cfg(feature = "tracing")]
            let work = {
                use tracing::Instrument;
                work.instrument(tracing::info_span!("workflow.path", execution.path = match selection.path { Path::Linear => "linear", Path::Graph => "graph" }, selection.reason = ?selection.reason))
            };
            // A selected path cannot extend the shared request deadline, or trigger fallback/replay.
            match tokio::time::timeout_at(deadline.into(), work).await {
                Ok(decision) if Instant::now() < deadline => decision,
                _ => Decision::expired(),
            }
        }))
    }
}
