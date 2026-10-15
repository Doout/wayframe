use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use wayframe_core::{Block, Context, Decision, Execution, Phase, Reason};
use wayframe_runtime::{
    adaptive::{Adaptive, Path, Selection},
    Pipeline,
};
struct Work {
    deadline: Instant,
    width: usize,
}

struct Timed {
    id: &'static str,
    delay: Duration,
    calls: Arc<AtomicUsize>,
    denied: Arc<std::sync::atomic::AtomicBool>,
}
impl Block<Work> for Timed {
    fn id(&self) -> &str {
        self.id
    }
    fn phase(&self) -> Phase {
        Phase::Authorize
    }
    fn execute<'a>(&'a self, _: &'a mut Work) -> Execution<'a> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.denied.load(Ordering::SeqCst) {
            return Execution::Immediate(Decision::deny(Reason::Forbidden, "current_rule"));
        }
        Execution::Deferred(Box::pin(async move {
            tokio::time::sleep(self.delay).await;
            Decision::Continue
        }))
    }
}
#[tokio::test]
async fn learned_path_selection_keeps_current_denials_and_never_replays() {
    use wayframe_runtime::adaptive::{ProfileSettings, Workload};
    let linear = Arc::new(AtomicUsize::new(0));
    let graph = Arc::new(AtomicUsize::new(0));
    let denied = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let selected = Adaptive::new(
        "measured",
        Phase::Authorize,
        Pipeline::compile(vec![Arc::new(Timed {
            id: "linear",
            delay: Duration::from_millis(12),
            calls: linear.clone(),
            denied: denied.clone(),
        })])
        .unwrap(),
        Arc::new(Timed {
            id: "graph",
            delay: Duration::from_millis(1),
            calls: graph.clone(),
            denied: denied.clone(),
        }),
        |c: &Work| Selection::for_width(c.width, 2),
    )
    .unwrap()
    .with_profile(
        |c: &Work| Workload {
            key: c.width as u64,
            width: c.width,
        },
        ProfileSettings {
            min_samples: 2,
            ..ProfileSettings::default()
        },
    )
    .unwrap();
    for _ in 0..4 {
        assert_eq!(
            selected.execute(&mut work(2)).resolve().await,
            Decision::Continue
        );
    }
    assert_eq!(linear.load(Ordering::SeqCst), 2);
    assert_eq!(graph.load(Ordering::SeqCst), 2);
    assert_eq!(
        selected.execute(&mut work(2)).resolve().await,
        Decision::Continue
    );
    assert_eq!(linear.load(Ordering::SeqCst), 2);
    assert_eq!(graph.load(Ordering::SeqCst), 3);
    denied.store(true, Ordering::SeqCst);
    assert_eq!(
        selected.execute(&mut work(2)).resolve().await,
        Decision::deny(Reason::Forbidden, "current_rule")
    );
    assert_eq!(linear.load(Ordering::SeqCst), 2);
    assert_eq!(graph.load(Ordering::SeqCst), 4);
    let stats = selected.profile_statistics().unwrap();
    assert_eq!(stats.successes, 5);
    assert_eq!(stats.failures, 1);
}
#[tokio::test]
async fn a_panicking_selector_or_classifier_fails_closed_before_either_path() {
    use wayframe_runtime::adaptive::{ProfileSettings, Workload};
    let calls = Arc::new(AtomicUsize::new(0));
    for classifier in [false, true] {
        let selected = Adaptive::new(
            "panic",
            Phase::Authorize,
            Pipeline::compile(vec![step("linear", Phase::Authorize, calls.clone(), false)])
                .unwrap(),
            step("graph", Phase::Authorize, calls.clone(), false),
            move |_: &Work| {
                if !classifier {
                    panic!("selector");
                }
                Selection::for_width(2, 2)
            },
        )
        .unwrap()
        .with_profile(
            move |_: &Work| -> Workload {
                panic!("classifier");
            },
            ProfileSettings::default(),
        )
        .unwrap();
        assert_eq!(
            selected.execute(&mut work(2)).resolve().await,
            Decision::deny(Reason::Internal, "workflow_selection_failed")
        );
    }
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}
impl Context for Work {
    fn deadline(&self) -> Instant {
        self.deadline
    }
}
struct Step {
    id: &'static str,
    phase: Phase,
    calls: Arc<AtomicUsize>,
    deny: bool,
    reveal: bool,
}
impl Block<Work> for Step {
    fn id(&self) -> &str {
        self.id
    }
    fn phase(&self) -> Phase {
        self.phase
    }
    fn execute<'a>(&'a self, c: &'a mut Work) -> Execution<'a> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.reveal {
            c.width = 2;
        }
        Execution::Immediate(if self.deny {
            Decision::deny(Reason::Forbidden, "required_gate")
        } else {
            Decision::Continue
        })
    }
}
fn step(
    id: &'static str,
    phase: Phase,
    calls: Arc<AtomicUsize>,
    deny: bool,
) -> Arc<dyn Block<Work>> {
    Arc::new(Step {
        id,
        phase,
        calls,
        deny,
        reveal: false,
    })
}
fn work(width: usize) -> Work {
    Work {
        deadline: Instant::now() + Duration::from_secs(1),
        width,
    }
}
#[tokio::test]
async fn selection_uses_new_information_at_a_boundary_and_returns_to_sync_work() {
    let linear = Arc::new(AtomicUsize::new(0));
    let graph = Arc::new(AtomicUsize::new(0));
    let after = Arc::new(AtomicUsize::new(0));
    let selected = Arc::new(
        Adaptive::new(
            "adaptive",
            Phase::Authorize,
            Pipeline::compile(vec![step(
                "linear",
                Phase::Authorize,
                linear.clone(),
                false,
            )])
            .unwrap(),
            step("graph", Phase::Authorize, graph.clone(), false),
            |c: &Work| Selection::for_width(c.width, 2),
        )
        .unwrap(),
    );
    let pipeline = Pipeline::compile(vec![
        selected.clone() as Arc<dyn Block<Work>>,
        step("after", Phase::Limit, after.clone(), false),
    ])
    .unwrap();
    assert_eq!(pipeline.evaluate(&mut work(1)).await, Decision::Continue);
    let switched = Pipeline::compile(vec![
        Arc::new(Step {
            id: "inspect",
            phase: Phase::Inspect,
            calls: Arc::new(AtomicUsize::new(0)),
            deny: false,
            reveal: true,
        }) as Arc<dyn Block<Work>>,
        selected.clone(),
        step("after", Phase::Limit, after.clone(), false),
    ])
    .unwrap();
    assert_eq!(switched.evaluate(&mut work(1)).await, Decision::Continue);
    assert_eq!(linear.load(Ordering::SeqCst), 1);
    assert_eq!(graph.load(Ordering::SeqCst), 1);
    assert_eq!(after.load(Ordering::SeqCst), 2);
    assert_eq!(selected.statistics().linear, 1);
    assert_eq!(selected.statistics().graph, 1);
}
#[tokio::test]
async fn either_path_denies_without_fallback_or_followup_execution() {
    let after = Arc::new(AtomicUsize::new(0));
    let linear = Arc::new(AtomicUsize::new(0));
    let graph = Arc::new(AtomicUsize::new(0));
    let selected = Arc::new(
        Adaptive::new(
            "adaptive",
            Phase::Authorize,
            Pipeline::compile(vec![step("linear", Phase::Authorize, linear.clone(), true)])
                .unwrap(),
            step("graph", Phase::Authorize, graph.clone(), true),
            |c: &Work| Selection::for_width(c.width, 2),
        )
        .unwrap(),
    );
    let pipeline = Pipeline::compile(vec![
        selected as Arc<dyn Block<Work>>,
        step("after", Phase::Limit, after.clone(), false),
    ])
    .unwrap();
    for width in [1, 2] {
        assert_eq!(
            pipeline.evaluate(&mut work(width)).await,
            Decision::deny(Reason::Forbidden, "required_gate")
        );
    }
    assert_eq!(linear.load(Ordering::SeqCst), 1);
    assert_eq!(graph.load(Ordering::SeqCst), 1);
    assert_eq!(after.load(Ordering::SeqCst), 0);
}
struct Waiting(Arc<AtomicUsize>);
impl Block<Work> for Waiting {
    fn id(&self) -> &str {
        "waiting-graph"
    }
    fn phase(&self) -> Phase {
        Phase::Authorize
    }
    fn execute<'a>(&'a self, _: &'a mut Work) -> Execution<'a> {
        Execution::Deferred(Box::pin(async move {
            struct Lease(Arc<AtomicUsize>);
            impl Drop for Lease {
                fn drop(&mut self) {
                    self.0.fetch_sub(1, Ordering::SeqCst);
                }
            }
            self.0.fetch_add(1, Ordering::SeqCst);
            let _lease = Lease(self.0.clone());
            std::future::pending().await
        }))
    }
}
#[tokio::test]
async fn selected_work_keeps_the_deadline_and_releases_on_timeout() {
    let alive = Arc::new(AtomicUsize::new(0));
    let linear = Arc::new(AtomicUsize::new(0));
    let selected = Adaptive::new(
        "adaptive",
        Phase::Authorize,
        Pipeline::compile(vec![step(
            "linear",
            Phase::Authorize,
            linear.clone(),
            false,
        )])
        .unwrap(),
        Arc::new(Waiting(alive.clone())),
        |c: &Work| Selection::for_width(c.width, 2),
    )
    .unwrap();
    let mut c = Work {
        deadline: Instant::now() + Duration::from_millis(5),
        width: 2,
    };
    assert_eq!(
        selected.execute(&mut c).resolve().await,
        Decision::expired()
    );
    assert_eq!(alive.load(Ordering::SeqCst), 0);
    assert_eq!(linear.load(Ordering::SeqCst), 0);
    c.deadline = Instant::now() - Duration::from_secs(1);
    assert_eq!(
        selected.execute(&mut c).resolve().await,
        Decision::expired()
    );
    assert_eq!(selected.statistics().graph, 1);
}
#[test]
fn incompatible_path_phases_fail_preparation() {
    assert!(Adaptive::new(
        "adaptive",
        Phase::Authorize,
        Pipeline::compile(vec![]).unwrap(),
        step(
            "wrong",
            Phase::Dispatch,
            Arc::new(AtomicUsize::new(0)),
            false
        ),
        |_: &Work| Selection::for_width(2, 2)
    )
    .is_err());
    assert_eq!(Selection::for_width(1, 0).path, Path::Linear);
}
