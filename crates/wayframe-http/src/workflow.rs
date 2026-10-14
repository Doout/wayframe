//! Embed a typed graph at an explicit HTTP policy phase. Native projection/merge code is trusted.
use crate::RequestContext;
use std::sync::Arc;
use wayframe_core::{Block, Context, Decision, Execution, Phase};
use wayframe_flow::{Error, Flow, Prepared, Run};
type Project<C> = Arc<dyn Fn(&RequestContext) -> C + Send + Sync>;
type Merge = Arc<dyn Fn(&mut RequestContext, Run) -> Decision + Send + Sync>;
pub struct FlowBlock<C: Context + Sync> {
    id: String,
    phase: Phase,
    flow: Arc<Flow<C>>,
    project: Project<C>,
    merge: Merge,
    plan: Option<Prepared>,
}
impl<C: Context + Sync> FlowBlock<C> {
    pub fn new(
        id: impl Into<String>,
        phase: Phase,
        flow: Flow<C>,
        project: impl Fn(&RequestContext) -> C + Send + Sync + 'static,
        merge: impl Fn(&mut RequestContext, Run) -> Decision + Send + Sync + 'static,
    ) -> Self {
        Self {
            id: id.into(),
            phase,
            flow: Arc::new(flow),
            project: Arc::new(project),
            merge: Arc::new(merge),
            plan: None,
        }
    }
    pub fn with_plan(mut self, plan: Prepared) -> Result<Self, Error> {
        if !plan.belongs_to(&self.flow) {
            return Err(Error::InvalidGraph("foreign_plan"));
        }
        self.plan = Some(plan);
        Ok(self)
    }
}
impl<C: Context + Sync> Block<RequestContext> for FlowBlock<C> {
    fn id(&self) -> &str {
        &self.id
    }
    fn phase(&self) -> Phase {
        self.phase
    }
    fn execute<'a>(&'a self, ctx: &'a mut RequestContext) -> Execution<'a> {
        Execution::Deferred(Box::pin(async move {
            let work = Arc::new((self.project)(ctx));
            let result = match &self.plan {
                Some(plan) => self.flow.run_plan(work, plan).await,
                None => self.flow.run(work).await,
            };
            match result {
                Ok(run) => (self.merge)(ctx, run),
                Err(failure) => failure.error.decision(),
            }
        }))
    }
}
