//! Cooperative execution and atomic deployment snapshots, independent of protocols.
use arc_swap::ArcSwap;
use std::{
    fmt,
    sync::{Arc, Mutex},
    time::Instant,
};
use wayframe_core::{Block, Context, Decision, Execution, Plan, PlanError};
pub mod adaptive;

pub struct Pipeline<C: Context> {
    plan: Plan<C>,
}
impl<C: Context> Pipeline<C> {
    pub fn compile(blocks: Vec<Arc<dyn Block<C>>>) -> Result<Self, PlanError> {
        Ok(Self {
            plan: Plan::compile(blocks)?,
        })
    }
    pub fn from_plan(plan: Plan<C>) -> Self {
        Self { plan }
    }
    pub fn len(&self) -> usize {
        self.plan.len()
    }
    pub fn is_empty(&self) -> bool {
        self.plan.is_empty()
    }
    pub fn plan(&self) -> &Plan<C> {
        &self.plan
    }
    pub async fn evaluate(&self, context: &mut C) -> Decision {
        // Capture once: a block cannot silently extend this pipeline's deadline.
        let deadline = context.deadline();
        if Instant::now() >= deadline {
            return Decision::expired();
        }
        for block in self.plan.blocks() {
            let run = async {
                let result = match block.execute(context) {
                    Execution::Immediate(result) => result,
                    Execution::Deferred(future) => {
                        match tokio::time::timeout_at(deadline.into(), future).await {
                            Ok(result) => result,
                            Err(_) => Decision::expired(),
                        }
                    }
                };
                #[cfg(feature = "tracing")]
                match &result {
                    Decision::Continue => {
                        tracing::Span::current().record("decision", "continue");
                    }
                    Decision::Deny { code, .. } => {
                        tracing::Span::current().record("decision", "deny");
                        tracing::Span::current().record("denial.code", code);
                    }
                }
                result
            };
            #[cfg(feature = "tracing")]
            let result = {
                use tracing::Instrument;
                run.instrument(tracing::info_span!("policy.step", block.id = block.id(), block.phase = ?block.phase(), decision = tracing::field::Empty, denial.code = tracing::field::Empty, lb.strategy = tracing::field::Empty, lb.backend.id = tracing::field::Empty, lb.overflow = tracing::field::Empty)).await
            };
            #[cfg(not(feature = "tracing"))]
            let result = run.await;
            if result != Decision::Continue {
                return result;
            }
            if Instant::now() >= deadline {
                return Decision::expired();
            }
        }
        Decision::Continue
    }
}

/// Holds the complete application-specific deployment (pipelines, routes, pools, etc.).
pub struct Generation<T> {
    pub id: u64,
    pub value: T,
}
/// Readers take an Arc snapshot; publication never waits for old requests to drain.
/// Preparation must complete BEFORE calling publish. Keep the snapshot until a stream ends.
pub struct Live<T> {
    current: ArcSwap<Generation<T>>,
    publication: Mutex<()>,
}
#[derive(Debug, PartialEq, Eq)]
pub enum PublishError {
    StaleGeneration,
    GenerationExhausted,
    Poisoned,
}
impl fmt::Display for PublishError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "deployment publication failed: {self:?}")
    }
}
impl std::error::Error for PublishError {}
impl<T> Live<T> {
    pub fn new(value: T) -> Self {
        Self {
            current: ArcSwap::from_pointee(Generation { id: 1, value }),
            publication: Mutex::new(()),
        }
    }
    pub fn snapshot(&self) -> Arc<Generation<T>> {
        self.current.load_full()
    }
    /// Compare-and-publish prevents a slow preparation from overwriting a newer deployment.
    pub fn publish(&self, expected: u64, prepared: T) -> Result<u64, PublishError> {
        let _lock = self
            .publication
            .lock()
            .map_err(|_| PublishError::Poisoned)?;
        let old = self.current.load();
        if old.id != expected {
            return Err(PublishError::StaleGeneration);
        }
        let id = old
            .id
            .checked_add(1)
            .ok_or(PublishError::GenerationExhausted)?;
        self.current.store(Arc::new(Generation {
            id,
            value: prepared,
        }));
        Ok(id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };
    use wayframe_core::{Phase, Reason};
    struct Work {
        deadline: Instant,
        value: usize,
    }
    impl Context for Work {
        fn deadline(&self) -> Instant {
            self.deadline
        }
    }
    struct Step {
        id: &'static str,
        deny: bool,
        calls: Arc<AtomicUsize>,
    }
    impl Block<Work> for Step {
        fn id(&self) -> &str {
            self.id
        }
        fn phase(&self) -> Phase {
            Phase::Authorize
        }
        fn execute<'a>(&'a self, c: &'a mut Work) -> Execution<'a> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            c.value += 1;
            Execution::Immediate(if self.deny {
                Decision::unavailable()
            } else {
                Decision::Continue
            })
        }
    }
    fn work() -> Work {
        Work {
            deadline: Instant::now() + Duration::from_secs(1),
            value: 0,
        }
    }
    #[tokio::test]
    async fn denial_is_terminal() {
        let calls = Arc::new(AtomicUsize::new(0));
        let p = Pipeline::compile(vec![
            Arc::new(Step {
                id: "deny",
                deny: true,
                calls: calls.clone(),
            }) as Arc<dyn Block<Work>>,
            Arc::new(Step {
                id: "later",
                deny: false,
                calls: calls.clone(),
            }),
        ])
        .unwrap();
        assert_eq!(p.evaluate(&mut work()).await, Decision::unavailable());
        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }
    #[tokio::test]
    async fn even_an_empty_pipeline_respects_deadline() {
        let p = Pipeline::<Work>::compile(vec![]).unwrap();
        let mut c = work();
        c.deadline = Instant::now();
        assert_eq!(p.evaluate(&mut c).await, Decision::expired());
    }
    struct Sleep;
    impl Block<Work> for Sleep {
        fn id(&self) -> &str {
            "sleep"
        }
        fn phase(&self) -> Phase {
            Phase::Authorize
        }
        fn execute<'a>(&'a self, _: &'a mut Work) -> Execution<'a> {
            Execution::Deferred(Box::pin(async {
                tokio::time::sleep(Duration::from_secs(1)).await;
                Decision::Continue
            }))
        }
    }
    #[tokio::test]
    async fn deferred_work_is_timed_out() {
        let p = Pipeline::compile(vec![Arc::new(Sleep) as Arc<dyn Block<Work>>]).unwrap();
        let mut c = work();
        c.deadline = Instant::now() + Duration::from_millis(5);
        assert!(matches!(
            p.evaluate(&mut c).await,
            Decision::Deny {
                reason: Reason::DeadlineExceeded,
                ..
            }
        ));
    }
    #[test]
    fn old_snapshot_survives_reload_and_stale_publish_fails() {
        let live = Live::new("old");
        let old = live.snapshot();
        assert_eq!(live.publish(old.id, "new"), Ok(2));
        assert_eq!(old.value, "old");
        assert_eq!(live.snapshot().value, "new");
        assert_eq!(
            live.publish(old.id, "stale"),
            Err(PublishError::StaleGeneration)
        );
        assert_eq!(live.snapshot().value, "new");
    }
}
