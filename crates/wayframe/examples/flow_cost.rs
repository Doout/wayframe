//! Isolate graph scheduling overhead from network, TLS, storage and SDK costs.
use std::{
    hint::black_box,
    sync::Arc,
    time::{Duration, Instant},
};
use wayframe::{flow::Builder, prelude::*};
#[derive(Clone)]
struct Work {
    deadline: Instant,
    width: usize,
}
impl Context for Work {
    fn deadline(&self) -> Instant {
        self.deadline
    }
}
struct Graph(Arc<wayframe::flow::Flow<Work>>);
impl Block<Work> for Graph {
    fn id(&self) -> &str {
        "graph"
    }
    fn phase(&self) -> Phase {
        Phase::Authorize
    }
    fn execute<'a>(&'a self, c: &'a mut Work) -> Execution<'a> {
        let context = Arc::new(c.clone());
        Execution::Deferred(Box::pin(async move {
            self.0
                .run(context)
                .await
                .map_or_else(|failure| failure.error.decision(), |_| Decision::Continue)
        }))
    }
}
struct Noop(String);
impl Block<Work> for Noop {
    fn id(&self) -> &str {
        &self.0
    }
    fn phase(&self) -> Phase {
        Phase::Authorize
    }
    fn execute<'a>(&'a self, _: &'a mut Work) -> Execution<'a> {
        Execution::Immediate(Decision::Continue)
    }
}
async fn branching_cost(count: usize, nodes: usize, selected: bool) -> serde_json::Value {
    let mut builder = Builder::<Work>::new();
    let root = builder.input();
    let fact = builder
        .sync("fact", Phase::Authorize, &root, |ctx| Ok(ctx.width > 0))
        .unwrap();
    let mut tail = builder
        .sync("optional", Phase::Authorize, &root, |_| Ok(0u64))
        .unwrap();
    builder.when(&tail, &fact, true).unwrap();
    for i in 1..nodes {
        tail = builder
            .sync(format!("optional-{i}"), Phase::Authorize, &tail, |value| {
                Ok(*value + 1)
            })
            .unwrap();
    }
    let otherwise = builder
        .sync("otherwise", Phase::Authorize, &root, |_| Ok(0u64))
        .unwrap();
    builder.when(&otherwise, &fact, false).unwrap();
    let joined = builder
        .choice("choice", Phase::Authorize, &tail, &otherwise)
        .unwrap();
    let output = builder
        .sync("after", Phase::Authorize, &joined, |value| Ok(*value + 1))
        .unwrap();
    let flow = builder.compile().unwrap();
    let ctx = Arc::new(Work {
        deadline: Instant::now() + Duration::from_secs(300),
        width: usize::from(selected),
    });
    let mut batches = Vec::new();
    let mut pruned = 0;
    // Warm compilation, allocation and executor paths before timing.
    for _ in 0..100 {
        black_box(flow.run(ctx.clone()).await.unwrap());
    }
    for _ in 0..7 {
        let start = Instant::now();
        for _ in 0..count {
            let run = flow.run(ctx.clone()).await.unwrap();
            assert_eq!(
                run.get(&output),
                Some(&if selected { nodes as u64 } else { 1 })
            );
            black_box(run);
        }
        batches.push(start.elapsed().as_nanos() as f64 / count as f64);
    }
    let run = flow.run(ctx).await.unwrap();
    for node in &run.nodes {
        pruned += usize::from(node.state == wayframe::flow::State::Skipped);
    }
    let mut ordered = batches.clone();
    ordered.sort_by(f64::total_cmp);
    serde_json::json!({"optional_nodes":nodes,"optional_path_selected":selected,
        "median_ns":ordered[3],"batches_ns":batches,"pruned_nodes":pruned,
        "executed_nodes":run.nodes.iter().filter(|n| n.state == wayframe::flow::State::Succeeded).count()-1})
}
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let count = std::env::var("WAYFRAME_FLOW_RUNS")
        .ok()
        .map(|s| s.parse())
        .transpose()?
        .unwrap_or(10000usize);
    if !(1000..=100000).contains(&count) {
        return Err("invalid benchmark count".into());
    }
    let mut cases = Vec::new();
    for nodes in [0, 1, 4, 16, 64] {
        let pipeline = Pipeline::compile(
            (0..nodes)
                .map(|n| Arc::new(Noop(format!("n{n}"))) as Arc<dyn Block<Work>>)
                .collect(),
        )?;
        let mut builder = Builder::<Work>::new();
        let input = builder.input();
        for n in 0..nodes {
            builder.asynchronous(format!("n{n}"), Phase::Authorize, &input, |_| async {
                Ok(1u64)
            })?;
        }
        let flow = Arc::new(builder.compile()?);
        let selected = wayframe::adaptive::Adaptive::new(
            "adaptive",
            Phase::Authorize,
            Pipeline::compile(
                (0..nodes)
                    .map(|n| Arc::new(Noop(format!("n{n}"))) as Arc<dyn Block<Work>>)
                    .collect(),
            )?,
            Arc::new(Graph(flow.clone())),
            |c: &Work| wayframe::adaptive::Selection::for_width(c.width, 2),
        )?;
        let mut linear = Vec::new();
        let mut graph = Vec::new();
        let mut adaptive_linear = Vec::new();
        let mut adaptive_graph = Vec::new();
        for _ in 0..7 {
            let mut work = Work {
                deadline: Instant::now() + Duration::from_secs(300),
                width: 1,
            };
            let start = Instant::now();
            for _ in 0..count {
                black_box(pipeline.evaluate(&mut work).await);
            }
            linear.push(start.elapsed().as_nanos() as f64 / count as f64);
            let context = Arc::new(work.clone());
            let start = Instant::now();
            for _ in 0..count {
                black_box(flow.run(context.clone()).await.unwrap());
            }
            graph.push(start.elapsed().as_nanos() as f64 / count as f64);
            let start = Instant::now();
            for _ in 0..count {
                black_box(selected.execute(&mut work).resolve().await);
            }
            adaptive_linear.push(start.elapsed().as_nanos() as f64 / count as f64);
            work.width = 2;
            let start = Instant::now();
            for _ in 0..count {
                black_box(selected.execute(&mut work).resolve().await);
            }
            adaptive_graph.push(start.elapsed().as_nanos() as f64 / count as f64);
        }
        let mut l = linear.clone();
        let mut g = graph.clone();
        l.sort_by(f64::total_cmp);
        g.sort_by(f64::total_cmp);
        let mut al = adaptive_linear.clone();
        al.sort_by(f64::total_cmp);
        let mut ag = adaptive_graph.clone();
        ag.sort_by(f64::total_cmp);
        cases.push(serde_json::json!({"nodes":nodes,"linear_ns":l[3],"graph_ns":g[3],"incremental_ns":g[3]-l[3],"linear_batches_ns":linear,"graph_batches_ns":graph,"adaptive_linear_ns":al[3],"adaptive_graph_ns":ag[3],"adaptive_linear_batches_ns":adaptive_linear,"adaptive_graph_batches_ns":adaptive_graph}));
    }
    let mut branches = Vec::new();
    for nodes in [1, 16, 64, 128] {
        for selected in [false, true] {
            branches.push(branching_cost(count, nodes, selected).await);
        }
    }
    println!(
        "{}",
        serde_json::json!({"iterations_per_batch":count,"batches":7,"cases":cases,"branch_cases":branches,"scope":"warm graph scheduling and conditional specialization; no network or plugin execution; tracing disabled"})
    );
    Ok(())
}
