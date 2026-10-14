use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use wayframe_core::{Context, Decision, Phase, Reason};
use wayframe_flow::{Builder, Error, Flow, Limits, Priority, State};
struct Work {
    deadline: std::time::Instant,
    allowed: bool,
}
impl Context for Work {
    fn deadline(&self) -> std::time::Instant {
        self.deadline
    }
}
fn work(ms: u64, allowed: bool) -> Arc<Work> {
    Arc::new(Work {
        deadline: (tokio::time::Instant::now() + Duration::from_millis(ms)).into_std(),
        allowed,
    })
}
fn delayed(
    priority: Priority,
    hints: bool,
    starts: Arc<Mutex<Vec<String>>>,
    writes: Arc<AtomicUsize>,
) -> Flow<Work> {
    let mut b = Builder::<Work>::new().priority(priority).limits(Limits {
        max_parallel: 2,
        max_executions: 4,
        max_running_nodes: 2,
    });
    let root = b.input();
    let gate = b
        .gate("authorize", Phase::Authenticate, &root, |ctx| {
            if ctx.allowed {
                Decision::Continue
            } else {
                Decision::deny(Reason::Forbidden, "no")
            }
        })
        .unwrap();
    let mut jobs = vec![];
    for (id, ms) in [("fast-a", 10), ("fast-b", 10), ("slow", 80)] {
        let starts = starts.clone();
        let job = b
            .asynchronous(id, Phase::Inspect, &root, move |_| {
                let starts = starts.clone();
                async move {
                    starts.lock().unwrap().push(id.into());
                    tokio::time::sleep(Duration::from_millis(ms)).await;
                    Ok(ms)
                }
            })
            .unwrap();
        if hints {
            b.cost_hint(&job, Duration::from_millis(ms)).unwrap();
        }
        jobs.push(job);
    }
    let dispatch = b
        .dispatch("dispatch", &jobs[2], move |_| {
            writes.fetch_add(1, Ordering::SeqCst);
            async { Ok(()) }
        })
        .unwrap();
    b.depends_on(&dispatch, &gate).unwrap();
    b.depends_on(&dispatch, &jobs[0]).unwrap();
    b.depends_on(&dispatch, &jobs[1]).unwrap();
    b.compile().unwrap()
}

#[tokio::test(start_paused = true)]
async fn longest_ready_path_meets_a_deadline_that_creation_order_misses() {
    for (priority, expected_ms) in [(Priority::Stable, 85), (Priority::CriticalPath, 80)] {
        let starts = Arc::new(Mutex::new(vec![]));
        let writes = Arc::new(AtomicUsize::new(0));
        let flow = delayed(priority, true, starts.clone(), writes.clone());
        let started = tokio::time::Instant::now();
        let result = flow.run(work(85, true)).await;
        assert_eq!(started.elapsed(), Duration::from_millis(expected_ms));
        if priority == Priority::Stable {
            assert_eq!(result.unwrap_err().error, Error::Deadline);
            assert_eq!(writes.load(Ordering::SeqCst), 0);
            assert_eq!(&starts.lock().unwrap()[..2], &["fast-a", "fast-b"]);
        } else {
            let run = result.unwrap();
            assert!(run.nodes.iter().all(|n| n.state == State::Succeeded));
            assert_eq!(writes.load(Ordering::SeqCst), 1);
            assert!(starts.lock().unwrap()[..2].contains(&"slow".into()));
        }
    }
}

#[tokio::test(start_paused = true)]
async fn measured_node_costs_reorder_later_invocations_without_hints() {
    let starts = Arc::new(Mutex::new(vec![]));
    let writes = Arc::new(AtomicUsize::new(0));
    let flow = delayed(Priority::Measured, false, starts.clone(), writes.clone());
    for _ in 0..3 {
        flow.run(work(200, true)).await.unwrap();
    }
    let costs = flow.node_costs();
    let slow = costs.iter().find(|n| n.id == "slow").unwrap();
    assert_eq!(slow.samples, 3);
    assert_eq!(slow.elapsed, Duration::from_millis(80));
    starts.lock().unwrap().clear();
    let begin = tokio::time::Instant::now();
    flow.run(work(85, true)).await.unwrap();
    assert_eq!(begin.elapsed(), Duration::from_millis(80));
    assert!(starts.lock().unwrap()[..2].contains(&"slow".into()));
    assert_eq!(writes.load(Ordering::SeqCst), 4);
}

#[tokio::test(start_paused = true)]
async fn ready_control_denial_prevents_reordering_into_expensive_work() {
    let starts = Arc::new(Mutex::new(vec![]));
    let writes = Arc::new(AtomicUsize::new(0));
    let flow = delayed(Priority::CriticalPath, true, starts.clone(), writes.clone());
    assert_eq!(
        flow.run(work(85, false)).await.unwrap_err().error,
        Error::Denied(Reason::Forbidden, "no")
    );
    assert!(starts.lock().unwrap().is_empty());
    assert_eq!(writes.load(Ordering::SeqCst), 0);
}

#[tokio::test(start_paused = true)]
async fn short_ready_node_budget_precedes_a_longer_cost_hint() {
    let mut b = Builder::<Work>::new()
        .priority(Priority::CriticalPath)
        .limits(Limits {
            max_parallel: 1,
            max_executions: 1,
            max_running_nodes: 1,
        });
    let root = b.input();
    let slow = b
        .asynchronous("slow", Phase::Inspect, &root, |_| async {
            tokio::time::sleep(Duration::from_millis(40)).await;
            Ok(())
        })
        .unwrap();
    b.cost_hint(&slow, Duration::from_millis(40)).unwrap();
    let urgent = b
        .asynchronous("urgent", Phase::Inspect, &root, |_| async {
            tokio::time::sleep(Duration::from_millis(5)).await;
            Ok(())
        })
        .unwrap();
    b.budget(&urgent, Duration::from_millis(10)).unwrap();
    let flow = b.compile().unwrap();
    let run = flow.run(work(100, true)).await.unwrap();
    assert_eq!(run.nodes[2].scheduler_wait, Duration::ZERO);
    assert_eq!(run.nodes[1].scheduler_wait, Duration::from_millis(5));
}

#[tokio::test(start_paused = true)]
async fn an_unneeded_slow_node_is_omitted_but_an_impossible_required_path_still_expires() {
    let mut b = Builder::<Work>::new().priority(Priority::CriticalPath);
    let root = b.input();
    let calls = Arc::new(AtomicUsize::new(0));
    let counted = calls.clone();
    let optional = b
        .asynchronous("optional-slow", Phase::Inspect, &root, move |_| {
            counted.fetch_add(1, Ordering::SeqCst);
            async {
                tokio::time::sleep(Duration::from_millis(80)).await;
                Ok(())
            }
        })
        .unwrap();
    b.discardable(&optional).unwrap();
    b.cost_hint(&optional, Duration::from_millis(80)).unwrap();
    let needed = b
        .asynchronous("needed", Phase::Inspect, &root, |_| async {
            tokio::time::sleep(Duration::from_millis(10)).await;
            Ok(())
        })
        .unwrap();
    let flow = b.compile().unwrap();
    let plan = flow.prepare(&[needed.output()]).unwrap();
    flow.run_plan(work(20, true), &plan).await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        flow.run_plan(work(5, true), &plan).await.unwrap_err().error,
        Error::Deadline
    );
}
