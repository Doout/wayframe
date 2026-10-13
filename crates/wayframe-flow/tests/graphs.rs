use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use wayframe_core::{Context, Decision, Phase, Reason};
use wayframe_flow::{Builder, CpuPool, Error, Limits, State};
struct Work {
    deadline: Instant,
}
impl Context for Work {
    fn deadline(&self) -> Instant {
        self.deadline
    }
}
fn work() -> Arc<Work> {
    Arc::new(Work {
        deadline: Instant::now() + Duration::from_secs(2),
    })
}
#[tokio::test]
async fn parallel_async_branches_join_before_a_sync_followup() {
    let mut b = Builder::<Work>::new();
    let root = b.input();
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let a_barrier = barrier.clone();
    let a = b
        .asynchronous("a", Phase::Inspect, &root, move |_| {
            let barrier = a_barrier.clone();
            async move {
                barrier.wait().await;
                Ok(20u64)
            }
        })
        .unwrap();
    let z = b
        .asynchronous("b", Phase::Inspect, &root, move |_| {
            let barrier = barrier.clone();
            async move {
                barrier.wait().await;
                Ok(22u64)
            }
        })
        .unwrap();
    let joined = b
        .join("join", Phase::Inspect, &a, &z, |a, z| Ok(*a + *z))
        .unwrap();
    let output = b
        .sync("after", Phase::Inspect, &joined, |v| {
            Ok(format!("answer={v}"))
        })
        .unwrap();
    let run = b.compile().unwrap().run(work()).await.unwrap();
    assert_eq!(run.get(&output).unwrap(), "answer=42");
    assert!(run.nodes.iter().all(|n| n.state == State::Succeeded));
}
#[tokio::test]
async fn conditional_branches_merge_exactly_one_output() {
    let mut b = Builder::<Work>::new();
    let root = b.input();
    let condition = b
        .sync("condition", Phase::Inspect, &root, |_| Ok(false))
        .unwrap();
    let a = b.sync("true", Phase::Inspect, &root, |_| Ok(1u64)).unwrap();
    b.when(&a, &condition, true).unwrap();
    let z = b
        .sync("false", Phase::Inspect, &root, |_| Ok(2u64))
        .unwrap();
    b.when(&z, &condition, false).unwrap();
    let output = b.choice("merge", Phase::Inspect, &a, &z).unwrap();
    let run = b.compile().unwrap().run(work()).await.unwrap();
    assert_eq!(run.get(&output), Some(&2));
    assert_eq!(run.nodes[2].state, State::Skipped);
}
#[tokio::test]
async fn mandatory_denial_cancels_siblings_and_prevents_dispatch() {
    let mut b = Builder::<Work>::new();
    let root = b.input();
    let alive = Arc::new(AtomicUsize::new(0));
    struct Lease(Arc<AtomicUsize>);
    impl Drop for Lease {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let parallel = barrier.clone();
    let count = alive.clone();
    let waiting = b
        .asynchronous("waiting", Phase::Authorize, &root, move |_| {
            let barrier = parallel.clone();
            let count = count.clone();
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                let _lease = Lease(count);
                barrier.wait().await;
                std::future::pending::<()>().await;
                Ok(())
            }
        })
        .unwrap();
    let deny = b
        .gate_async("deny", Phase::Authorize, &root, move |_| {
            let barrier = barrier.clone();
            async move {
                barrier.wait().await;
                Decision::deny(Reason::Forbidden, "no")
            }
        })
        .unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = calls.clone();
    let dispatch = b
        .dispatch("dispatch", &root, move |_| {
            let calls = counter.clone();
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        })
        .unwrap();
    b.depends_on(&dispatch, &deny).unwrap();
    b.depends_on(&dispatch, &waiting).unwrap();
    let failure = b.compile().unwrap().run(work()).await.unwrap_err();
    assert_eq!(failure.error, Error::Denied(Reason::Forbidden, "no"));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(alive.load(Ordering::SeqCst), 0);
}
#[test]
fn graph_validation_rejects_cycles_foreign_ports_and_dispatch_bypasses() {
    let mut a = Builder::<Work>::new();
    let root = a.input();
    let gate = a
        .gate("auth", Phase::Authenticate, &root, |_| Decision::Continue)
        .unwrap();
    a.dispatch("unsafe", &root, |_| async { Ok(()) }).unwrap();
    assert!(matches!(
        a.compile(),
        Err(Error::InvalidGraph("dispatch_bypasses_mandatory_gate"))
    ));
    let mut a = Builder::<Work>::new();
    assert!(a.sync("bad", Phase::Inspect, &gate, |_| Ok(())).is_err());
    let root = a.input();
    let x = a.sync("x", Phase::Inspect, &root, |_| Ok(())).unwrap();
    let y = a.sync("y", Phase::Inspect, &x, |_| Ok(())).unwrap();
    a.depends_on(&x, &y).unwrap();
    assert!(matches!(a.compile(), Err(Error::InvalidGraph("cycle"))));
}
#[tokio::test]
async fn graph_deadline_and_panics_are_failures_without_dispatch() {
    let mut b = Builder::<Work>::new();
    let root = b.input();
    let node = b
        .asynchronous("slow", Phase::Inspect, &root, |_| {
            std::future::pending::<Result<(), Error>>()
        })
        .unwrap();
    b.budget(&node, Duration::from_millis(5)).unwrap();
    assert_eq!(
        b.compile().unwrap().run(work()).await.unwrap_err().error,
        Error::Deadline
    );
    let mut b = Builder::<Work>::new();
    let root = b.input();
    b.sync::<_, (), _>("panic", Phase::Inspect, &root, |_| {
        panic!("trusted task panic")
    })
    .unwrap();
    assert_eq!(
        b.compile().unwrap().run(work()).await.unwrap_err().error,
        Error::Panic
    );
}
#[tokio::test]
async fn scheduler_obeys_parallel_limit_and_execution_admission() {
    let mut b = Builder::<Work>::new().limits(Limits {
        max_parallel: 1,
        max_executions: 1,
        max_running_nodes: 1,
    });
    let root = b.input();
    let release = Arc::new(tokio::sync::Notify::new());
    let entered = Arc::new(tokio::sync::Notify::new());
    let r = release.clone();
    let e = entered.clone();
    b.asynchronous("one", Phase::Inspect, &root, move |_| {
        let r = r.clone();
        let e = e.clone();
        async move {
            e.notify_one();
            r.notified().await;
            Ok(())
        }
    })
    .unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let c = calls.clone();
    b.sync("two", Phase::Inspect, &root, move |_| {
        c.fetch_add(1, Ordering::SeqCst);
        Ok(())
    })
    .unwrap();
    let flow = Arc::new(b.compile().unwrap());
    let f = flow.clone();
    let task = tokio::spawn(async move { f.run(work()).await });
    entered.notified().await;
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(flow.run(work()).await.unwrap_err().error, Error::Overloaded);
    release.notify_one();
    task.await.unwrap().unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}
#[tokio::test(start_paused = true)]
async fn ready_node_budget_expires_while_waiting_for_a_parallel_slot() {
    let mut builder = Builder::<Work>::new().limits(Limits {
        max_parallel: 1,
        max_executions: 1,
        max_running_nodes: 1,
    });
    let root = builder.input();
    builder
        .asynchronous("occupied", Phase::Inspect, &root, |_| {
            std::future::pending::<Result<(), Error>>()
        })
        .unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let check = calls.clone();
    let waiting = builder
        .sync("waiting", Phase::Inspect, &root, move |_| {
            check.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
        .unwrap();
    builder.budget(&waiting, Duration::from_millis(40)).unwrap();
    let failure = builder.compile().unwrap().run(work()).await.unwrap_err();
    assert_eq!(failure.error, Error::Deadline);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(failure.nodes[1].state, State::Cancelled);
    assert_eq!(failure.nodes[2].state, State::Failed);
    assert_eq!(failure.nodes[2].scheduler_wait, Duration::from_millis(40));
    assert_eq!(failure.nodes[2].elapsed, Duration::ZERO);
}

#[tokio::test(start_paused = true)]
async fn node_reports_separate_parallel_slot_wait_from_execution() {
    let mut builder = Builder::<Work>::new().limits(Limits {
        max_parallel: 1,
        max_executions: 1,
        max_running_nodes: 1,
    });
    let root = builder.input();
    builder
        .asynchronous("first", Phase::Inspect, &root, |_| async {
            tokio::time::sleep(Duration::from_millis(50)).await;
            Ok(())
        })
        .unwrap();
    builder
        .asynchronous("second", Phase::Inspect, &root, |_| async {
            tokio::time::sleep(Duration::from_millis(10)).await;
            Ok(())
        })
        .unwrap();
    let run = builder.compile().unwrap().run(work()).await.unwrap();
    assert_eq!(run.nodes[2].dependency_wait, Duration::ZERO);
    assert_eq!(run.nodes[2].scheduler_wait, Duration::from_millis(50));
    assert_eq!(run.nodes[2].elapsed, Duration::from_millis(10));
}

#[tokio::test]
async fn cancelling_the_caller_drops_parallel_work() {
    let mut b = Builder::<Work>::new();
    let root = b.input();
    let entered = Arc::new(tokio::sync::Notify::new());
    let seen = entered.clone();
    let gone = Arc::new(AtomicUsize::new(0));
    let check = gone.clone();
    struct DropSeen(Arc<AtomicUsize>);
    impl Drop for DropSeen {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    b.asynchronous("wait", Phase::Inspect, &root, move |_| {
        let seen = seen.clone();
        let check = check.clone();
        async move {
            let _guard = DropSeen(check);
            seen.notify_one();
            std::future::pending::<Result<(), Error>>().await
        }
    })
    .unwrap();
    let flow = b.compile().unwrap();
    let task = tokio::spawn(async move { flow.run(work()).await });
    entered.notified().await;
    task.abort();
    let _ = task.await;
    assert_eq!(gone.load(Ordering::SeqCst), 1);
}
#[tokio::test]
async fn cpu_timeout_keeps_running_work_accounted_and_quorum_cancels_losers() {
    let pool = CpuPool::new(1).unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    assert_eq!(
        pool.run(
            tokio::time::Instant::now() + Duration::from_millis(5),
            move || {
                rx.recv().unwrap();
                Ok(())
            }
        )
        .await,
        Err(Error::Deadline)
    );
    assert_eq!(
        pool.run(
            tokio::time::Instant::now() + Duration::from_millis(5),
            || Ok(())
        )
        .await,
        Err(Error::Deadline)
    );
    tx.send(()).unwrap();
    let tasks: Vec<wayframe_flow::FutureTask<u64>> =
        vec![Box::pin(async { Ok(42) }), Box::pin(std::future::pending())];
    assert_eq!(wayframe_flow::quorum(tasks, 1, 2).await.unwrap(), vec![42]);
}
#[test]
fn streams_have_explicit_single_consumer_ownership() {
    let value = wayframe_flow::Exclusive::new(String::from("stream"));
    assert_eq!(value.take().unwrap(), "stream");
    assert!(value.take().is_err());
}
#[tokio::test]
async fn finalizer_runs_once_on_cancellation_and_cannot_replace_failure() {
    use wayframe_flow::Completion;
    let mut builder = Builder::<Work>::new();
    let entered = Arc::new(tokio::sync::Notify::new());
    let signal = entered.clone();
    builder
        .asynchronous("work", Phase::Authorize, &builder.input(), move |_| {
            let signal = signal.clone();
            async move {
                signal.notify_one();
                std::future::pending::<Result<(), Error>>().await
            }
        })
        .unwrap();
    let flow = Arc::new(builder.compile().unwrap());
    let outcomes = Arc::new(std::sync::Mutex::new(vec![]));
    let saved = outcomes.clone();
    let task = tokio::spawn(async move {
        flow.run_finally(work(), move |outcome| saved.lock().unwrap().push(outcome))
            .await
    });
    entered.notified().await;
    task.abort();
    let _ = task.await;
    assert_eq!(*outcomes.lock().unwrap(), [Completion::Cancelled]);
    let mut builder = Builder::<Work>::new();
    builder
        .gate("deny", Phase::Authorize, &builder.input(), |_| {
            Decision::deny(Reason::Forbidden, "denied")
        })
        .unwrap();
    let flow = builder.compile().unwrap();
    let saved = outcomes.clone();
    assert!(matches!(
        flow.run_finally(work(), move |outcome| saved.lock().unwrap().push(outcome))
            .await
            .unwrap_err()
            .error,
        Error::Denied(..)
    ));
    assert_eq!(
        *outcomes.lock().unwrap(),
        [Completion::Cancelled, Completion::Failed]
    );
}
