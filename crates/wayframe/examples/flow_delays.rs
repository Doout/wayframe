//! Real-timer delayed DAG workloads. These simulate independent I/O, not network/provider latency.
use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use wayframe::{
    flow::{Builder, Flow, Limits, Prepared, Priority},
    prelude::*,
};
struct Work {
    deadline: Instant,
}
impl Context for Work {
    fn deadline(&self) -> Instant {
        self.deadline
    }
}
struct Case {
    name: &'static str,
    flow: Flow<Work>,
    plan: Option<Prepared>,
    starts: Arc<Mutex<Vec<(String, u128)>>>,
}
fn build(
    name: &'static str,
    delays: &[u64],
    parallel: usize,
    priority: Priority,
    prepared: bool,
) -> Result<Case, Box<dyn std::error::Error>> {
    let mut b = Builder::<Work>::new().priority(priority).limits(Limits {
        max_parallel: parallel,
        max_executions: 16,
        max_running_nodes: 16,
    });
    let root = b.input();
    b.gate("authorize", Phase::Authenticate, &root, |_| {
        Decision::Continue
    })?;
    let starts = Arc::new(Mutex::new(vec![]));
    let mut jobs = vec![];
    for (i, ms) in delays.iter().copied().enumerate() {
        let starts = starts.clone();
        let job = b.asynchronous(format!("job-{i}"), Phase::Inspect, &root, move |ctx| {
            let starts = starts.clone();
            async move {
                // Relative to the request deadline origin; no request payload is retained.
                starts.lock().unwrap().push((
                    format!("job-{i}"),
                    (Instant::now() - (ctx.deadline - Duration::from_secs(3))).as_nanos(),
                ));
                tokio::time::sleep(Duration::from_millis(ms)).await;
                Ok(ms)
            }
        })?;
        if priority == Priority::CriticalPath {
            b.cost_hint(&job, Duration::from_millis(ms))?;
        }
        if prepared && i + 1 == delays.len() {
            b.discardable(&job)?;
        }
        jobs.push(job);
    }
    let flow = b.compile()?;
    let plan = if prepared {
        Some(flow.prepare(&[jobs[0].output()])?)
    } else {
        None
    };
    Ok(Case {
        name,
        flow,
        plan,
        starts,
    })
}
async fn run(case: &Case) -> Result<(f64, serde_json::Value), Box<dyn std::error::Error>> {
    case.starts.lock().unwrap().clear();
    let ctx = Arc::new(Work {
        deadline: Instant::now() + Duration::from_secs(3),
    });
    let begin = Instant::now();
    let result = if let Some(plan) = &case.plan {
        case.flow.run_plan(ctx, plan).await
    } else {
        case.flow.run(ctx).await
    };
    let result = result?;
    let elapsed = begin.elapsed().as_secs_f64() * 1000.0;
    let report = serde_json::json!({
        "nodes":result.nodes.iter().map(|node|serde_json::json!({"id":node.id,"state":format!("{:?}",node.state),
            "scheduling":node.scheduling.map(|mode|format!("{mode:?}")),"skip_reason":node.skip_reason.map(|reason|format!("{reason:?}")),
            "dependency_wait_ns":node.dependency_wait.as_nanos(),"scheduler_wait_ns":node.scheduler_wait.as_nanos(),"execution_ns":node.elapsed.as_nanos()})).collect::<Vec<_>>(),
        "starts":case.starts.lock().unwrap().clone()
    });
    Ok((elapsed, report))
}
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let count = std::env::var("WAYFRAME_DELAY_RUNS")
        .ok()
        .map(|s| s.parse())
        .transpose()?
        .unwrap_or(10usize);
    if !(3..=100).contains(&count) {
        return Err("delay runs must be 3..100".into());
    }
    let mut reports = vec![];
    for (name, delays, prepared) in [
        ("one-slow", vec![10, 10, 80], false),
        ("several-slow", vec![20, 10, 80, 50], false),
        ("unused-slow", vec![10, 80], true),
    ] {
        for parallel in [1, 2, 4] {
            for priority in [Priority::Stable, Priority::CriticalPath, Priority::Measured] {
                let case = build(name, &delays, parallel, priority, prepared)?;
                let (cold, cold_report) = run(&case).await?;
                // At least three successful observations before measuring learned ordering.
                run(&case).await?;
                run(&case).await?;
                let mut samples = vec![];
                let mut last = serde_json::Value::Null;
                for _ in 0..count {
                    let (elapsed, report) = run(&case).await?;
                    samples.push(elapsed);
                    last = report;
                }
                let mut ordered = samples.clone();
                ordered.sort_by(f64::total_cmp);
                reports.push(serde_json::json!({"scenario":case.name,"delays_ms":delays,"max_parallel":parallel,
                    "priority":format!("{priority:?}"),"prepared":prepared,"cold_ms":cold,"cold_report":cold_report,
                    "warm_median_ms":ordered[ordered.len()/2],"warm_samples_ms":samples,"last_report":last,
                    "observed_costs":case.flow.node_costs().iter().map(|cost|serde_json::json!({"id":cost.id,"samples":cost.samples,"elapsed_ns":cost.elapsed.as_nanos()})).collect::<Vec<_>>() }));
            }
        }
    }
    println!(
        "{}",
        serde_json::json!({"iterations_per_case":count,"warmup":3,"cases":reports,
        "scope":"real monotonic timers, simulated I/O sleeps, no network/plugins, tracing disabled; no absolute wall-time deadline assertion"})
    );
    Ok(())
}
