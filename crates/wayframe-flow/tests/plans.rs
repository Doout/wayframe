use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use wayframe_core::{Context, Decision, Phase, Reason};
use wayframe_flow::{Builder, Completion, Error, SkipReason, State};
struct Work {
    deadline: Instant,
    value: u64,
    allowed: bool,
}
impl Context for Work {
    fn deadline(&self) -> Instant {
        self.deadline
    }
}
fn work(value: u64, allowed: bool) -> Arc<Work> {
    Arc::new(Work {
        deadline: Instant::now() + Duration::from_secs(2),
        value,
        allowed,
    })
}

#[tokio::test]
async fn prepared_outputs_omit_discardable_work_and_keep_current_controls_and_effects() {
    let mut b = Builder::<Work>::new();
    let root = b.input();
    let selected = b
        .sync("selected", Phase::Inspect, &root, |ctx| Ok(ctx.value * 2))
        .unwrap();
    b.discardable(&selected).unwrap();
    let unused = b
        .sync::<_, (), _>("unused", Phase::Inspect, &root, |_| {
            panic!("unused action invoked")
        })
        .unwrap();
    b.discardable(&unused).unwrap();
    let effect_count = Arc::new(AtomicUsize::new(0));
    let effects = effect_count.clone();
    b.sync("effect", Phase::Inspect, &root, move |_| {
        effects.fetch_add(1, Ordering::SeqCst);
        Ok(())
    })
    .unwrap();
    let gate_count = Arc::new(AtomicUsize::new(0));
    let counted = gate_count.clone();
    let gate = b
        .gate("authorize", Phase::Authorize, &root, move |ctx| {
            counted.fetch_add(1, Ordering::SeqCst);
            if ctx.allowed {
                Decision::Continue
            } else {
                Decision::deny(Reason::Forbidden, "revoked")
            }
        })
        .unwrap();
    let writes = Arc::new(AtomicUsize::new(0));
    let written = writes.clone();
    let dispatch = b
        .dispatch("dispatch", &selected, move |value| {
            written.fetch_add(1, Ordering::SeqCst);
            async move { Ok(*value) }
        })
        .unwrap();
    b.depends_on(&dispatch, &gate).unwrap();
    let flow = b.compile().unwrap();
    let plan = flow.prepare(&[selected.output()]).unwrap();
    assert_eq!(plan.omitted_nodes(), 1);
    let run = flow.run_plan(work(5, true), &plan).await.unwrap();
    assert_eq!(run.get(&selected), Some(&10));
    assert_eq!(run.get(&dispatch), Some(&10));
    assert_eq!(run.get(&unused), None);
    assert_eq!(
        run.nodes[2].skip_reason,
        Some(SkipReason::UnrequestedOutput)
    );
    let cached = flow
        .prepare(&[selected.output(), selected.output()])
        .unwrap();
    assert_eq!(flow.plan_statistics().hits, 1);
    let failure = flow.run_plan(work(100, false), &cached).await.unwrap_err();
    assert_eq!(failure.error, Error::Denied(Reason::Forbidden, "revoked"));
    assert_eq!(writes.load(Ordering::SeqCst), 1);
    assert_eq!(gate_count.load(Ordering::SeqCst), 2);
    assert_eq!(effect_count.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn a_cached_plan_still_resolves_new_branch_facts_and_values() {
    let mut b = Builder::<Work>::new();
    let root = b.input();
    let selector = b
        .sync("selector", Phase::Inspect, &root, |ctx| Ok(ctx.allowed))
        .unwrap();
    b.discardable(&selector).unwrap();
    let a = b
        .sync("a", Phase::Inspect, &root, |ctx| Ok(ctx.value))
        .unwrap();
    let z = b
        .sync("z", Phase::Inspect, &root, |ctx| Ok(ctx.value + 100))
        .unwrap();
    b.discardable(&a).unwrap();
    b.discardable(&z).unwrap();
    b.when(&a, &selector, true).unwrap();
    b.when(&z, &selector, false).unwrap();
    let chosen = b.choice("choice", Phase::Inspect, &a, &z).unwrap();
    b.discardable(&chosen).unwrap();
    let flow = b.compile().unwrap();
    for (value, allowed, expected) in [(1, true, 1), (2, false, 102), (3, true, 3)] {
        let plan = flow.prepare(&[chosen.output()]).unwrap();
        let run = flow.run_plan(work(value, allowed), &plan).await.unwrap();
        assert_eq!(run.get(&chosen), Some(&expected));
    }
    assert_eq!(flow.plan_statistics().misses, 1);
    assert_eq!(flow.plan_statistics().hits, 2);
}

#[tokio::test]
async fn bounded_plan_cache_evicts_structure_without_invalidating_owned_plans() {
    let mut b = Builder::<Work>::new();
    let root = b.input();
    let mut outputs = vec![];
    for i in 0..40 {
        let port = b
            .sync(format!("node-{i}"), Phase::Inspect, &root, move |ctx| {
                Ok(ctx.value + i)
            })
            .unwrap();
        b.discardable(&port).unwrap();
        outputs.push(port);
    }
    let flow = b.compile().unwrap();
    let old = flow.prepare(&[outputs[0].output()]).unwrap();
    for output in &outputs {
        flow.prepare(&[output.output()]).unwrap();
    }
    let stats = flow.plan_statistics();
    assert_eq!(stats.entries, stats.capacity);
    assert!(stats.evictions > 0);
    let run = flow.run_plan(work(7, true), &old).await.unwrap();
    assert_eq!(run.get(&outputs[0]), Some(&7));
    assert_eq!(
        run.nodes
            .iter()
            .filter(|n| n.state == State::Succeeded)
            .count(),
        2
    );
}

#[tokio::test]
async fn foreign_plans_and_outputs_are_rejected_and_controls_cannot_be_discardable() {
    let mut a = Builder::<Work>::new();
    let root = a.input();
    let gate = a
        .gate("gate", Phase::Authorize, &root, |_| Decision::Continue)
        .unwrap();
    let dispatch = a.dispatch("dispatch", &root, |_| async { Ok(()) }).unwrap();
    a.depends_on(&dispatch, &gate).unwrap();
    for error in [
        a.discardable(&root),
        a.discardable(&gate),
        a.discardable(&dispatch),
    ] {
        assert_eq!(error, Err(Error::InvalidGraph("discardable_control")));
    }
    let other = Builder::<Work>::new();
    let foreign = other.input();
    let other = other.compile().unwrap();
    let flow = a.compile().unwrap();
    assert!(flow.prepare(&[foreign.output()]).is_err());
    let plan = other.prepare(&[]).unwrap();
    assert!(!plan.belongs_to(&flow));
    let seen = Arc::new(std::sync::Mutex::new(vec![]));
    let saved = seen.clone();
    let failure = flow
        .run_plan_finally(work(1, true), &plan, move |outcome| {
            saved.lock().unwrap().push(outcome)
        })
        .await
        .unwrap_err();
    assert_eq!(failure.error, Error::InvalidGraph("foreign_plan"));
    assert!(failure.nodes.iter().all(|n| n.state == State::Pending));
    assert_eq!(*seen.lock().unwrap(), [Completion::Failed]);
}

#[tokio::test]
async fn unannotated_errors_remain_required_even_without_requested_outputs() {
    let mut b = Builder::<Work>::new();
    b.sync::<_, (), _>("required", Phase::Inspect, &b.input(), |_| {
        Err(Error::Failed("required"))
    })
    .unwrap();
    let flow = b.compile().unwrap();
    let plan = flow.prepare(&[]).unwrap();
    assert_eq!(plan.omitted_nodes(), 0);
    assert_eq!(
        flow.run_plan(work(1, true), &plan).await.unwrap_err().error,
        Error::Failed("required")
    );
}

#[tokio::test]
async fn prepared_plan_finalizer_runs_once_when_serial_work_is_cancelled() {
    let mut b = Builder::<Work>::new();
    let entered = Arc::new(tokio::sync::Notify::new());
    let signal = entered.clone();
    let waiting = b
        .asynchronous("waiting", Phase::Inspect, &b.input(), move |_| {
            let signal = signal.clone();
            async move {
                signal.notify_one();
                std::future::pending::<Result<(), Error>>().await
            }
        })
        .unwrap();
    b.discardable(&waiting).unwrap();
    let flow = b.compile().unwrap();
    let plan = flow.prepare(&[waiting.output()]).unwrap();
    let seen = Arc::new(std::sync::Mutex::new(vec![]));
    let saved = seen.clone();
    let task = tokio::spawn(async move {
        flow.run_plan_finally(work(1, true), &plan, move |outcome| {
            saved.lock().unwrap().push(outcome)
        })
        .await
    });
    entered.notified().await;
    task.abort();
    let _ = task.await;
    assert_eq!(*seen.lock().unwrap(), [Completion::Cancelled]);
}
