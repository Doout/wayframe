//! Native pipeline batches avoid a timer call for each block transition.
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
    let runs = std::env::var("WAYFRAME_BENCH_RUNS")
        .unwrap_or_else(|_| "200000".into())
        .parse::<u64>()?;
    let rounds = std::env::var("WAYFRAME_BENCH_ROUNDS")
        .unwrap_or_else(|_| "7".into())
        .parse::<usize>()?;
    if !(1000..=10_000_000).contains(&runs) || !(3..=30).contains(&rounds) {
        return Err("invalid runs or rounds".into());
    }
    let mut results = Vec::new();
    let mut baseline = 0.0;
    for blocks in [0, 1, 4, 16, 64] {
        let pipeline = Pipeline::compile(
            (0..blocks)
                .map(|n| Arc::new(Increment(format!("step-{n}"))) as Arc<dyn Block<Work>>)
                .collect(),
        )?;
        let mut work = Work {
            due: Instant::now() + Duration::from_secs(3600),
            value: 0,
        };
        for _ in 0..10000 {
            black_box(pipeline.evaluate(&mut work).await);
        }
        let mut samples = Vec::new();
        for _ in 0..rounds {
            let start = Instant::now();
            for _ in 0..runs {
                assert_eq!(
                    black_box(pipeline.evaluate(black_box(&mut work)).await),
                    Decision::Continue
                );
            }
            samples.push(start.elapsed().as_secs_f64() * 1e9 / runs as f64);
        }
        let mut sorted = samples.clone();
        sorted.sort_by(f64::total_cmp);
        let median = sorted[rounds / 2];
        if blocks == 0 {
            baseline = median;
        }
        let expected = blocks as u64 * (10000 + runs * rounds as u64);
        if work.value != expected {
            return Err("benchmark checksum mismatch".into());
        }
        results.push(serde_json::json!({"blocks":blocks,"median_ns_per_evaluation":median,"added_ns_over_empty":median-baseline,"min_ns":sorted[0],"max_ns":sorted[rounds-1],"raw_round_means_ns":samples,"checksum":work.value}));
    }
    println!(
        "{}",
        serde_json::to_string_pretty(
            &serde_json::json!({"kind":"native_pipeline_microbenchmark","runs_per_round":runs,"rounds":rounds,"warmup_evaluations":10000,"results":results})
        )?
    );
    Ok(())
}
