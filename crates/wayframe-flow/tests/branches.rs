use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use wayframe_core::{Context, Decision, Phase, Reason};
use wayframe_flow::{Builder, Error, SkipReason, State};

#[derive(PartialEq, Eq, Clone, Copy)]
enum Route {
    Read,
    Write,
    Admin,
}
struct Work {
    deadline: Instant,
    route: Route,
    cached: bool,
}
impl Context for Work {
    fn deadline(&self) -> Instant {
        self.deadline
    }
}
fn work(route: Route, cached: bool) -> Arc<Work> {
    Arc::new(Work {
        deadline: Instant::now() + Duration::from_secs(2),
        route,
        cached,
    })
}

#[tokio::test]
async fn typed_routes_and_later_facts_prune_branches_and_rejoin_sync_work() {
    let mut b = Builder::<Work>::new();
    let root = b.input();
    let selector = b
        .sync("route", Phase::Inspect, &root, |ctx| Ok(ctx.route))
        .unwrap();
    let read = b
        .sync("read", Phase::Inspect, &root, |_| Ok("read"))
        .unwrap();
    b.when_value(&read, &selector, Route::Read).unwrap();
    let admin = b
        .sync("admin", Phase::Inspect, &root, |_| Ok("admin"))
        .unwrap();
    b.when_value(&admin, &selector, Route::Admin).unwrap();
    // This fact is unavailable until the write branch is selected.
    let cache = b
        .asynchronous("write-cache", Phase::Inspect, &root, |ctx| async move {
            Ok(ctx.cached)
        })
        .unwrap();
    b.when_value(&cache, &selector, Route::Write).unwrap();
    let hit = b
        .sync("write-hit", Phase::Inspect, &cache, |_| Ok("hit"))
        .unwrap();
    b.when(&hit, &cache, true).unwrap();
    let miss = b
        .sync("write-miss", Phase::Inspect, &cache, |_| Ok("miss"))
        .unwrap();
    b.when(&miss, &cache, false).unwrap();
    let write = b
        .choice("write-result", Phase::Inspect, &hit, &miss)
        .unwrap();
    // A choice inside an inactive parent path must itself be guarded.
    b.when_value(&write, &selector, Route::Write).unwrap();
    let chosen = b
        .choice_many("route-result", Phase::Inspect, &[read, write, admin])
        .unwrap();
    let output = b
        .sync("after", Phase::Authorize, &chosen, |result| {
            Ok(format!("after:{result}"))
        })
        .unwrap();
    let flow = b.compile().unwrap();
    // Reuse one compiled graph while request facts change; no old decision or value is reused.
    for (route, cached, expected) in [
        (Route::Read, false, "after:read"),
        (Route::Write, false, "after:miss"),
        (Route::Write, true, "after:hit"),
        (Route::Admin, true, "after:admin"),
    ] {
        let run = flow.run(work(route, cached)).await.unwrap();
        assert_eq!(run.get(&output).unwrap(), expected);
        assert!(run
            .nodes
            .iter()
            .any(|n| n.skip_reason == Some(SkipReason::BranchNotSelected)));
        assert!(run
            .nodes
            .iter()
            .filter(|n| n.state == State::Skipped)
            .all(|n| n.elapsed == Duration::ZERO && n.scheduler_wait == Duration::ZERO));
    }
}

#[tokio::test]
async fn inactive_branch_is_pruned_before_an_unrelated_dependency_finishes() {
    let mut b = Builder::<Work>::new();
    let root = b.input();
    let blocked = b
        .asynchronous("blocked", Phase::Inspect, &root, |_| {
            std::future::pending::<Result<(), Error>>()
        })
        .unwrap();
    let selector = b
        .sync("selector", Phase::Inspect, &root, |_| Ok(false))
        .unwrap();
    let inactive = b
        .sync("inactive", Phase::Inspect, &blocked, |_| {
            panic!("inactive action executed");
            #[allow(unreachable_code)]
            Ok(())
        })
        .unwrap();
    b.when(&inactive, &selector, true).unwrap();
    b.sync("inactive-child", Phase::Inspect, &inactive, |_| Ok(()))
        .unwrap();
    b.gate("deny", Phase::Authorize, &selector, |_| {
        Decision::deny(Reason::Forbidden, "stop")
    })
    .unwrap();
    let failure = b
        .compile()
        .unwrap()
        .run(work(Route::Read, false))
        .await
        .unwrap_err();
    assert_eq!(failure.error, Error::Denied(Reason::Forbidden, "stop"));
    assert_eq!(failure.nodes[1].state, State::Cancelled);
    assert_eq!(failure.nodes[3].state, State::Skipped);
    assert_eq!(
        failure.nodes[3].skip_reason,
        Some(SkipReason::BranchNotSelected)
    );
    assert_eq!(
        failure.nodes[4].skip_reason,
        Some(SkipReason::DependencySkipped)
    );
}

#[tokio::test]
async fn choice_rejects_zero_or_multiple_selected_values() {
    for enabled in [false, true] {
        let mut b = Builder::<Work>::new();
        let root = b.input();
        let selector = b
            .sync("selector", Phase::Inspect, &root, move |_| Ok(enabled))
            .unwrap();
        let mut alternatives = vec![];
        for i in 0..3 {
            let branch = b
                .sync(format!("branch-{i}"), Phase::Inspect, &root, |_| Ok(()))
                .unwrap();
            b.when(&branch, &selector, true).unwrap();
            alternatives.push(branch);
        }
        b.choice_many("choice", Phase::Inspect, &alternatives)
            .unwrap();
        assert_eq!(
            b.compile()
                .unwrap()
                .run(work(Route::Read, false))
                .await
                .unwrap_err()
                .error,
            Error::Failed("ambiguous_or_empty_choice")
        );
    }
}

#[tokio::test]
async fn pruning_cannot_satisfy_a_skipped_mandatory_gate() {
    let mut b = Builder::<Work>::new();
    let root = b.input();
    let selector = b
        .sync("selector", Phase::Inspect, &root, |_| Ok(false))
        .unwrap();
    let left = b.sync("left", Phase::Inspect, &root, |_| Ok(())).unwrap();
    b.when(&left, &selector, true).unwrap();
    let right = b.sync("right", Phase::Inspect, &root, |_| Ok(())).unwrap();
    b.when(&right, &selector, false).unwrap();
    let gate = b
        .gate("required", Phase::Authorize, &left, |_| Decision::Continue)
        .unwrap();
    let chosen = b.choice("choice", Phase::Authorize, &left, &right).unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let counted = calls.clone();
    let dispatch = b
        .dispatch("dispatch", &chosen, move |_| {
            counted.fetch_add(1, Ordering::SeqCst);
            async { Ok(()) }
        })
        .unwrap();
    b.depends_on(&dispatch, &gate).unwrap();
    let failure = b
        .compile()
        .unwrap()
        .run(work(Route::Read, false))
        .await
        .unwrap_err();
    assert_eq!(
        failure.error,
        Error::Denied(Reason::Forbidden, "mandatory_gate_not_satisfied")
    );
    assert_eq!(failure.nodes.last().unwrap().state, State::Cancelled);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[test]
fn multiway_choices_reject_duplicate_foreign_and_missing_inputs() {
    let mut b = Builder::<Work>::new();
    let root = b.input();
    assert!(b
        .choice_many("empty", Phase::Inspect, &[] as &[wayframe_flow::Port<()>])
        .is_err());
    assert!(b
        .choice_many("duplicate", Phase::Inspect, &[root.clone(), root.clone()])
        .is_err());
    let other = Builder::<Work>::new();
    assert!(b
        .choice_many("foreign", Phase::Inspect, &[root, other.input()])
        .is_err());
}

#[tokio::test]
async fn a_choice_cannot_hide_a_pruned_mandatory_gate() {
    let mut b = Builder::<Work>::new();
    let root = b.input();
    let selector = b
        .sync("selector", Phase::Inspect, &root, |_| Ok(false))
        .unwrap();
    let inactive = b
        .sync("inactive", Phase::Inspect, &root, |_| Ok(()))
        .unwrap();
    b.when(&inactive, &selector, true).unwrap();
    let gate = b
        .gate("required", Phase::Authorize, &inactive, |_| {
            Decision::Continue
        })
        .unwrap();
    let fallback = b
        .sync("fallback", Phase::Authorize, &root, |_| Ok(()))
        .unwrap();
    let selected = b
        .choice("choice", Phase::Authorize, &gate, &fallback)
        .unwrap();
    b.dispatch("dispatch", &selected, |_| async {
        panic!("dispatch bypassed a gate");
        #[allow(unreachable_code)]
        Ok(())
    })
    .unwrap();
    assert_eq!(
        b.compile()
            .unwrap()
            .run(work(Route::Read, false))
            .await
            .unwrap_err()
            .error,
        Error::Denied(Reason::Forbidden, "mandatory_gate_not_satisfied")
    );
}

#[tokio::test]
async fn an_embedded_graph_cannot_succeed_with_a_skipped_gate() {
    let mut b = Builder::<Work>::new();
    let root = b.input();
    let selector = b
        .sync("selector", Phase::Inspect, &root, |_| Ok(false))
        .unwrap();
    let optional = b
        .sync("optional", Phase::Inspect, &root, |_| Ok(()))
        .unwrap();
    b.when(&optional, &selector, true).unwrap();
    b.gate("required", Phase::Authorize, &optional, |_| {
        Decision::Continue
    })
    .unwrap();
    // A FlowBlock may dispatch later in its parent pipeline; there is no dispatch node here.
    let failure = b
        .compile()
        .unwrap()
        .run(work(Route::Read, false))
        .await
        .unwrap_err();
    assert_eq!(
        failure.error,
        Error::Denied(Reason::Forbidden, "mandatory_gate_not_satisfied")
    );
    assert_eq!(failure.nodes.last().unwrap().state, State::Skipped);
}

#[tokio::test]
async fn panicking_typed_comparison_is_a_terminal_failure() {
    struct BadEq;
    impl PartialEq for BadEq {
        fn eq(&self, _: &Self) -> bool {
            panic!("selector failure")
        }
    }
    impl Eq for BadEq {}
    let mut b = Builder::<Work>::new();
    let root = b.input();
    let selector = b
        .sync("selector", Phase::Inspect, &root, |_| Ok(BadEq))
        .unwrap();
    let branch = b.sync("branch", Phase::Inspect, &root, |_| Ok(())).unwrap();
    b.when_value(&branch, &selector, BadEq).unwrap();
    let failure = b
        .compile()
        .unwrap()
        .run(work(Route::Read, false))
        .await
        .unwrap_err();
    assert_eq!(failure.error, Error::Panic);
    assert_eq!(failure.nodes[2].state, State::Cancelled);
}
