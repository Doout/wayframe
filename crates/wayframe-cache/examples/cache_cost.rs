//! Release-mode lookup cost. Same source runs against the pre-change cache.
use std::{
    collections::BTreeSet,
    hint::black_box,
    time::{Duration, Instant},
};
use wayframe_cache::{Cache, Key, Scope};
fn main() -> anyhow::Result<()> {
    let key = Key::new(
        Scope {
            tenant: "tenant",
            subject: "reader",
            permissions: &BTreeSet::new(),
            generation: 1,
            route: "route",
        },
        "read",
        "tool-v1",
        &serde_json::json!({"key":"a"}),
    )?;
    let cache = Cache::new(512, 16 * 1024 * 1024, 128)?;
    cache.insert(key.clone(), 42_u64, 8, Duration::from_secs(60))?;
    let mut trials = Vec::new();
    for _ in 0..30 {
        let started = Instant::now();
        for _ in 0..10000 {
            black_box(cache.get(black_box(&key))?.expect("retained"));
        }
        trials.push(started.elapsed().as_nanos() as f64 / 10000.0);
    }
    let mut ordered = trials.clone();
    ordered.sort_by(f64::total_cmp);
    println!(
        "{}",
        serde_json::json!({"case":"ttl_lookup", "batch_iterations":10000,"trials_ns":trials,"p50_ns":ordered[14],"p95_ns":ordered[28],"stats":cache.stats()?})
    );
    Ok(())
}
