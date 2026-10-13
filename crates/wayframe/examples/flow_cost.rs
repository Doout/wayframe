//! Isolate graph scheduling overhead from network, TLS, storage and SDK costs.
use std::{
    hint::black_box,
    sync::Arc,
    time::{Duration, Instant},
};
use wayframe::{flow::Builder, prelude::*};
struct Work(Instant);
impl Context for Work {
    fn deadline(&self) -> Instant {
        self.0
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
        let flow = builder.compile()?;
        let mut linear = Vec::new();
        let mut graph = Vec::new();
        for _ in 0..7 {
            let mut work = Work(Instant::now() + Duration::from_secs(300));
            let start = Instant::now();
            for _ in 0..count {
                black_box(pipeline.evaluate(&mut work).await);
            }
            linear.push(start.elapsed().as_nanos() as f64 / count as f64);
            let context = Arc::new(Work(Instant::now() + Duration::from_secs(300)));
            let start = Instant::now();
            for _ in 0..count {
                black_box(flow.run(context.clone()).await.unwrap());
            }
            graph.push(start.elapsed().as_nanos() as f64 / count as f64);
        }
        let mut l = linear.clone();
        let mut g = graph.clone();
        l.sort_by(f64::total_cmp);
        g.sort_by(f64::total_cmp);
        cases.push(serde_json::json!({"nodes":nodes,"linear_ns":l[3],"graph_ns":g[3],"incremental_ns":g[3]-l[3],"linear_batches_ns":linear,"graph_batches_ns":graph}));
    }
    println!(
        "{}",
        serde_json::json!({"iterations_per_batch":count,"batches":7,"cases":cases,"scope":"warm graph scheduling; no network or plugin execution"})
    );
    Ok(())
}
