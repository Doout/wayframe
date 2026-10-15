//! Exact evidence and a diamond invalidation; excludes remote validation I/O.
use std::{
    collections::{BTreeMap, BTreeSet},
    hint::black_box,
    time::{Duration, Instant},
};
use wayframe_cache::{Cache, Evidence, Key, Scope};
fn key(name: &str) -> Key {
    Key::new(
        Scope {
            tenant: "tenant",
            subject: "reader",
            permissions: &BTreeSet::new(),
            generation: 1,
            route: "route",
        },
        "read",
        name,
        &serde_json::Value::Null,
    )
    .unwrap()
}
fn main() -> anyhow::Result<()> {
    let cache = Cache::new(512, 16 * 1024 * 1024, 128)?;
    let ttl = Duration::from_secs(60);
    let evidence = Evidence::new(BTreeMap::from([("resource".into(), "epoch:1".into())]))?;
    let a = cache.retain(key("a"), 1, 8, ttl, &[], evidence.clone())?;
    let b = cache.retain(
        key("b"),
        2,
        8,
        ttl,
        std::slice::from_ref(&a.dependency),
        Evidence::default(),
    )?;
    let c = cache.retain(
        key("c"),
        3,
        8,
        ttl,
        std::slice::from_ref(&a.dependency),
        Evidence::default(),
    )?;
    let deps = vec![b.dependency, c.dependency];
    let join = key("join");
    cache.retain(join.clone(), 4, 8, ttl, &deps, Evidence::default())?;
    let mut trials = Vec::new();
    for _ in 0..30 {
        let started = Instant::now();
        for _ in 0..10000 {
            black_box(
                cache
                    .lookup(black_box(&join), &deps, &Evidence::default())?
                    .expect("retained"),
            );
        }
        trials.push(started.elapsed().as_nanos() as f64 / 10000.0);
    }
    let started = Instant::now();
    let removed = cache.invalidate(&key("a"))?;
    let invalidate_ns = started.elapsed().as_nanos();
    assert_eq!(removed, 4);
    let mut ordered = trials.clone();
    ordered.sort_by(f64::total_cmp);
    let root = cache.retain(key("root"), 1, 8, ttl, &[], Evidence::default())?;
    for index in 0..511 {
        cache.retain(
            key(&format!("child-{index}")),
            2,
            8,
            ttl,
            std::slice::from_ref(&root.dependency),
            Evidence::default(),
        )?;
    }
    let started = Instant::now();
    let wide_removed = cache.invalidate(&key("root"))?;
    let wide_invalidate_ns = started.elapsed().as_nanos();
    assert_eq!(wide_removed, 512);
    println!(
        "{}",
        serde_json::json!({"case":"diamond_lookup", "batch_iterations":10000,"trials_ns":trials,"p50_ns":ordered[14],"p95_ns":ordered[28],"invalidate_ns":invalidate_ns,"removed":removed,"wide_removed":wide_removed,"wide_invalidate_ns":wide_invalidate_ns})
    );
    Ok(())
}
