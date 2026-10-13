//! Repeated native pipeline batches isolate routing/lease overhead from network and SDK work.
use serde_json::json;
use std::{
    hint::black_box,
    sync::Arc,
    time::{Duration, Instant},
};
use wayframe::{
    lb::{Backend, LoadBalance, Pool, RoutingContext, Selection, Strategy},
    prelude::*,
};
struct Work {
    deadline: Instant,
    selection: Option<Selection>,
}
impl Context for Work {
    fn deadline(&self) -> Instant {
        self.deadline
    }
}
impl RoutingContext for Work {
    fn set_selection(&mut self, selection: Selection) {
        self.selection = Some(selection);
    }
}
async fn batch(pipeline: &Pipeline<Work>, count: usize) -> f64 {
    let mut work = Work {
        deadline: Instant::now() + Duration::from_secs(300),
        selection: None,
    };
    let start = Instant::now();
    for _ in 0..count {
        assert_eq!(pipeline.evaluate(&mut work).await, Decision::Continue);
        if let Some(selection) = work.selection.take() {
            black_box(selection.backend_id());
            drop(selection);
        }
    }
    start.elapsed().as_nanos() as f64 / count as f64
}
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let count = std::env::var("WAYFRAME_LB_RUNS")
        .ok()
        .map(|s| s.parse())
        .transpose()?
        .unwrap_or(100000usize);
    if !(1000..=1000000).contains(&count) {
        return Err("invalid LB benchmark batch size".into());
    }
    let baseline = Pipeline::compile(vec![])?;
    batch(&baseline, 10000).await;
    let mut empty = Vec::new();
    for _ in 0..7 {
        empty.push(batch(&baseline, count).await);
    }
    let mut sorted = empty.clone();
    sorted.sort_by(f64::total_cmp);
    let baseline_ns = sorted[3];
    let mut cases = Vec::new();
    for backends in [2, 8, 64] {
        for strategy in [
            Strategy::RoundRobin,
            Strategy::LeastConnections,
            Strategy::WeightedRoundRobin,
            Strategy::QuotaPriority,
        ] {
            let pool = Pool::new(
                (0..backends)
                    .map(|i| {
                        let mut b = Backend::new(format!("b{i}"));
                        b.weight = (i + 1) as u32;
                        b.quota = 1;
                        b
                    })
                    .collect(),
            )?;
            let block: Arc<dyn Block<Work>> =
                Arc::new(LoadBalance::new("lb", pool.clone(), strategy));
            let pipeline = Pipeline::compile(vec![block])?;
            batch(&pipeline, 10000).await;
            let mut means = Vec::new();
            for _ in 0..7 {
                means.push(batch(&pipeline, count).await);
            }
            let mut sorted = means.clone();
            sorted.sort_by(f64::total_cmp);
            let stats = pool.snapshot()?;
            assert!(stats.iter().all(|s| s.active == 0));
            assert_eq!(
                stats.iter().map(|s| s.selected).sum::<u64>(),
                (count * 7 + 10000) as u64
            );
            cases.push(json!({"strategy":strategy,"backends":backends,"raw_batch_mean_ns":means,"median_ns":sorted[3],"min_ns":sorted[0],"max_ns":sorted[6],"added_ns_above_empty_pipeline":sorted[3]-baseline_ns,"selections":stats.iter().map(|s|s.selected).sum::<u64>(),"leaked_reservations":0}));
        }
    }
    println!(
        "{}",
        serde_json::to_string_pretty(
            &json!({"kind":"native_lb_pipeline","requests_per_batch":count,"batches":7,"warmup":10000,"scope":"single-thread typed native pipeline, selection and final lease release; no network, health probes, exporter, SDK or saturation workload","empty_pipeline_median_ns":baseline_ns,"empty_raw_batch_mean_ns":empty,"cases":cases})
        )?
    );
    Ok(())
}
