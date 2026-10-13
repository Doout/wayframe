//! Microbenchmark only. No network, plugin, TLS or production-throughput claims.
use std::{
    hint::black_box,
    sync::Arc,
    time::{Duration, Instant},
};
use wayframe::prelude::*;
struct Work {
    due: Instant,
    value: u64,
}
impl Context for Work {
    fn deadline(&self) -> Instant {
        self.due
    }
}
struct Increment(String);
impl Block<Work> for Increment {
    fn id(&self) -> &str {
        &self.0
    }
    fn phase(&self) -> Phase {
        Phase::Inspect
    }
    fn execute<'a>(&'a self, work: &'a mut Work) -> Execution<'a> {
        work.value = black_box(work.value.wrapping_add(1));
        Execution::Immediate(Decision::Continue)
    }
}
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runs: u64 = std::env::var("WAYFRAME_BENCH_RUNS")
        .unwrap_or_else(|_| "200000".into())
        .parse()?;
    if !(1000..=10_000_000).contains(&runs) {
        return Err("runs must be 1000..10000000".into());
    }
    println!("{{\"kind\":\"native_pipeline_microbenchmark\",\"runs\":{runs},\"results\":[");
    for (index, blocks) in [0, 1, 4, 16, 64].into_iter().enumerate() {
        let pipeline = Pipeline::compile(
            (0..blocks)
                .map(|n| Arc::new(Increment(format!("step-{n}"))) as Arc<dyn Block<Work>>)
                .collect(),
        )?;
        let mut work = Work {
            due: Instant::now() + Duration::from_secs(3600),
            value: 0,
        };
        for _ in 0..1000 {
            black_box(pipeline.evaluate(&mut work).await);
        }
        let start = Instant::now();
        for _ in 0..runs {
            black_box(pipeline.evaluate(black_box(&mut work)).await);
        }
        let ns = start.elapsed().as_secs_f64() * 1e9 / runs as f64;
        let comma = if index == 0 { "" } else { "," };
        println!(
            "{comma}{{\"blocks\":{blocks},\"mean_ns_per_evaluation\":{ns:.2},\"checksum\":{}}}",
            work.value
        );
    }
    println!("]}}");
    Ok(())
}
