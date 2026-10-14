use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use wayframe_core::{Context, Phase};
use wayframe_flow::{Builder, Error, Limits, Scheduling, State};
struct Work {
    deadline: Instant,
    waiting: bool,
}
impl Context for Work {
    fn deadline(&self) -> Instant {
        self.deadline
    }
}
fn work(waiting: bool) -> Arc<Work> {
    Arc::new(Work {
        deadline: Instant::now() + Duration::from_secs(2),
        waiting,
    })
}

#[tokio::test]
async fn graph_switches_inline_parallel_and_serial_as_its_frontier_changes() {
    let mut b = Builder::<Work>::new();
    let before = b
        .sync("before", Phase::Inspect, &b.input(), |_| Ok(10u64))
        .unwrap();
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let mut branches = vec![];
    for i in 0..2 {
        let barrier = barrier.clone();
        branches.push(
            b.asynchronous(format!("parallel-{i}"), Phase::Inspect, &before, move |v| {
                let barrier = barrier.clone();
                async move {
                    barrier.wait().await;
                    Ok(*v + 1)
                }
            })
            .unwrap(),
        );
    }
    let join = b
        .join(
            "join",
            Phase::Inspect,
            &branches[0],
            &branches[1],
            |a, b| Ok(*a + *b),
        )
        .unwrap();
    let serial = b
        .asynchronous("serial", Phase::Inspect, &join, |v| async move {
            tokio::task::yield_now().await;
            Ok(*v + 1)
        })
        .unwrap();
    let after = b
        .sync("after", Phase::Inspect, &serial, |v| Ok(*v + 1))
        .unwrap();
    let run = b.compile().unwrap().run(work(false)).await.unwrap();
    assert_eq!(run.get(&after), Some(&24));
    let modes: Vec<_> = run
        .nodes
        .iter()
        .skip(1)
        .map(|n| n.scheduling.unwrap())
        .collect();
    assert_eq!(
        modes,
        [
            Scheduling::Inline,
            Scheduling::Parallel,
            Scheduling::Parallel,
            Scheduling::Inline,
            Scheduling::Serial,
            Scheduling::Inline
        ]
    );
}

#[tokio::test]
async fn ready_queue_executes_the_last_node_at_the_full_graph_bound() {
    let mut b = Builder::<Work>::new();
    let root = b.input();
    let calls = Arc::new(AtomicUsize::new(0));
    let mut last = None;
    for i in 0..256 {
        let calls = calls.clone();
        last = Some(
            b.sync(format!("node-{i}"), Phase::Inspect, &root, move |_| {
                Ok(calls.fetch_add(1, Ordering::SeqCst))
            })
            .unwrap(),
        );
    }
    let run = b.compile().unwrap().run(work(false)).await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 256);
    assert_eq!(run.get(&last.unwrap()), Some(&255));
    assert!(run
        .nodes
        .iter()
        .skip(1)
        .all(|n| n.scheduling == Some(Scheduling::Inline)));
}

#[tokio::test]
async fn late_sync_output_is_rejected_before_dispatch() {
    let mut b = Builder::<Work>::new();
    let root = b.input();
    let slow = b
        .sync("slow", Phase::Inspect, &root, |_| {
            std::thread::sleep(Duration::from_millis(40));
            Ok(())
        })
        .unwrap();
    b.budget(&slow, Duration::from_millis(20)).unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let counted = calls.clone();
    b.dispatch("dispatch", &slow, move |_| {
        counted.fetch_add(1, Ordering::SeqCst);
        async { Ok(()) }
    })
    .unwrap();
    let failure = b.compile().unwrap().run(work(false)).await.unwrap_err();
    assert_eq!(failure.error, Error::Deadline);
    assert_eq!(failure.nodes[1].state, State::Failed);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn immediate_work_waits_for_global_capacity_without_invoking_its_action() {
    let mut b = Builder::<Work>::new().limits(Limits {
        max_parallel: 2,
        max_executions: 2,
        max_running_nodes: 1,
    });
    let root = b.input();
    let selector = b
        .sync("selector", Phase::Inspect, &root, |ctx| Ok(ctx.waiting))
        .unwrap();
    b.budget(&selector, Duration::from_millis(20)).unwrap();
    let entered = Arc::new(tokio::sync::Notify::new());
    let signal = entered.clone();
    let waiting = b
        .asynchronous("waiting", Phase::Inspect, &root, move |_| {
            let signal = signal.clone();
            async move {
                signal.notify_one();
                std::future::pending::<Result<(), Error>>().await
            }
        })
        .unwrap();
    b.when(&waiting, &selector, true).unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let counted = calls.clone();
    let immediate = b
        .sync("immediate", Phase::Inspect, &root, move |_| {
            counted.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
        .unwrap();
    b.when(&immediate, &selector, false).unwrap();
    let flow = Arc::new(b.compile().unwrap());
    let running = flow.clone();
    let task = tokio::spawn(async move { running.run(work(true)).await });
    entered.notified().await;
    let failure = flow.run(work(false)).await.unwrap_err();
    assert_eq!(failure.error, Error::Deadline);
    assert_eq!(failure.nodes[1].scheduling, Some(Scheduling::Serial));
    assert_eq!(failure.nodes[1].elapsed, Duration::ZERO);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    task.abort();
    let _ = task.await;
    // Cancellation releases the shared node slot for another invocation.
    flow.run(work(false)).await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}
