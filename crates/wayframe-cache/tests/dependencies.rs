use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};
use wayframe_cache::{Cache, Evidence, Key, Scope};
fn key(name: &str) -> Key {
    Key::new(
        Scope {
            tenant: "tenant",
            subject: "user",
            permissions: &BTreeSet::new(),
            generation: 1,
            route: "route",
        },
        "compute",
        name,
        &serde_json::Value::Null,
    )
    .unwrap()
}
fn evidence(version: &str) -> Evidence {
    Evidence::new(BTreeMap::from([("resource".into(), version.into())])).unwrap()
}
#[test]
fn presence_is_advisory_and_does_not_discard_stateful_entries() {
    let cache = Cache::new(8, 65536, 2).unwrap();
    assert!(!cache.may_contain(&key("read")).unwrap());
    cache
        .retain(
            key("read"),
            42,
            8,
            Duration::from_secs(60),
            &[],
            evidence("1"),
        )
        .unwrap();
    assert!(cache.may_contain(&key("read")).unwrap());
    let mut other = key("read");
    other.subject = "other-user".into();
    assert!(!cache.may_contain(&other).unwrap());
    assert_eq!(
        *cache
            .lookup(&key("read"), &[], &evidence("1"))
            .unwrap()
            .unwrap()
            .value,
        42
    );
    // A positive hint cannot authorize a result with changed remote evidence.
    assert!(cache
        .lookup(&key("read"), &[], &evidence("2"))
        .unwrap()
        .is_none());
    assert!(!cache.may_contain(&key("read")).unwrap());
}

#[tokio::test]
async fn presence_excludes_expired_entries_without_returning_or_pruning_them() {
    let cache = Cache::new(8, 65536, 2).unwrap();
    cache
        .retain(
            key("read"),
            42,
            8,
            Duration::from_millis(100),
            &[],
            evidence("1"),
        )
        .unwrap();
    assert!(cache.may_contain(&key("read")).unwrap());
    tokio::time::sleep(Duration::from_millis(120)).await;
    assert!(!cache.may_contain(&key("read")).unwrap());
    assert_eq!(cache.stats().unwrap()["entries"], 1);
    assert!(cache
        .lookup(&key("read"), &[], &evidence("1"))
        .unwrap()
        .is_none());
}

#[test]
fn changed_witness_invalidates_diamond_but_preserves_independent_branch() {
    let cache = Cache::new(32, 65536, 4).unwrap();
    let ttl = Duration::from_secs(60);
    let a = cache
        .retain(key("a"), 1, 8, ttl, &[], evidence("1"))
        .unwrap();
    let b = cache
        .retain(
            key("b"),
            2,
            8,
            ttl,
            std::slice::from_ref(&a.dependency),
            Evidence::default(),
        )
        .unwrap();
    let c = cache
        .retain(
            key("c"),
            3,
            8,
            ttl,
            std::slice::from_ref(&a.dependency),
            Evidence::default(),
        )
        .unwrap();
    cache
        .retain(
            key("join"),
            4,
            8,
            ttl,
            &[b.dependency, c.dependency],
            Evidence::default(),
        )
        .unwrap();
    cache.insert(key("independent"), 5, 8, ttl).unwrap();
    assert!(cache
        .lookup(&key("a"), &[], &evidence("2"))
        .unwrap()
        .is_none());
    assert_eq!(cache.stats().unwrap()["entries"], 1);
    assert_eq!(*cache.get(&key("independent")).unwrap().unwrap(), 5);
    assert_eq!(cache.stats().unwrap()["invalidated"], 4);
}
#[test]
fn stale_inflight_result_cannot_repopulate_a_branch() {
    let cache = Cache::new(8, 65536, 2).unwrap();
    let ttl = Duration::from_secs(60);
    let old = cache.insert(key("a"), 1, 8, ttl).unwrap();
    assert_eq!(*old, 1);
    let old = cache
        .lookup(&key("a"), &[], &Evidence::default())
        .unwrap()
        .unwrap()
        .dependency;
    cache.insert(key("a"), 1, 8, ttl).unwrap(); // Same output, different computation revision.
    assert!(cache
        .retain(key("child"), 2, 8, ttl, &[old], Evidence::default())
        .is_err());
    assert!(cache.get(&key("child")).unwrap().is_none());
}
#[tokio::test]
async fn expired_ancestor_prevents_transitive_reuse() {
    let cache = Cache::new(8, 65536, 2).unwrap();
    let a = cache
        .retain(
            key("a"),
            1,
            8,
            Duration::from_millis(100),
            &[],
            Evidence::default(),
        )
        .unwrap();
    let b = cache
        .retain(
            key("b"),
            2,
            8,
            Duration::from_secs(60),
            &[a.dependency],
            Evidence::default(),
        )
        .unwrap();
    let deps = vec![b.dependency];
    cache
        .retain(
            key("c"),
            3,
            8,
            Duration::from_secs(60),
            &deps,
            Evidence::default(),
        )
        .unwrap();
    tokio::time::sleep(Duration::from_millis(110)).await;
    assert!(cache
        .lookup(&key("c"), &deps, &Evidence::default())
        .unwrap()
        .is_none());
}
#[test]
fn foreign_scope_and_unchecked_evidence_are_rejected() {
    let cache = Cache::new(8, 65536, 2).unwrap();
    let a = cache
        .retain(key("a"), 1, 8, Duration::from_secs(60), &[], evidence("1"))
        .unwrap();
    let mut other = key("b");
    other.subject = "other".into();
    assert!(cache
        .retain(
            other,
            2,
            8,
            Duration::from_secs(60),
            &[a.dependency],
            Evidence::default()
        )
        .is_err());
    assert!(cache.get(&key("a")).unwrap().is_none());
    assert!(Evidence::new(BTreeMap::new()).is_err());
    assert!(Evidence::new(BTreeMap::from([("resource".into(), "x".repeat(257))])).is_err());
}
#[test]
fn eviction_removes_children_and_metadata_is_accounted() {
    let cache = Cache::new(2, 65536, 2).unwrap();
    let a = cache
        .retain(key("a"), 1, 8, Duration::from_secs(10), &[], evidence("1"))
        .unwrap();
    cache
        .retain(
            key("b"),
            2,
            8,
            Duration::from_secs(20),
            &[a.dependency],
            Evidence::default(),
        )
        .unwrap();
    assert!(cache.stats().unwrap()["bytes"].as_u64().unwrap() > 16);
    cache
        .insert(key("c"), 3, 8, Duration::from_secs(30))
        .unwrap();
    assert_eq!(cache.stats().unwrap()["entries"], 1);
    assert!(cache.get(&key("b")).unwrap().is_none());
}

#[test]
fn receipts_are_bound_to_the_cache_instance() {
    let a = Cache::new(8, 65536, 2).unwrap();
    let b = Cache::new(8, 65536, 2).unwrap();
    let ttl = Duration::from_secs(60);
    let foreign = a
        .retain(key("parent"), 1, 8, ttl, &[], Evidence::default())
        .unwrap();
    b.insert(key("parent"), 99, 8, ttl).unwrap();
    assert!(b
        .retain(
            key("child"),
            2,
            8,
            ttl,
            &[foreign.dependency],
            Evidence::default()
        )
        .is_err());
}

#[test]
fn wide_invalidation_cleans_reverse_edges_before_keys_are_reused() {
    let cache = Cache::new(512, 16 * 1024 * 1024, 2).unwrap();
    let ttl = Duration::from_secs(60);
    for _ in 0..2 {
        let root = cache
            .retain(key("root"), 1, 8, ttl, &[], Evidence::default())
            .unwrap();
        for index in 0..511 {
            cache
                .retain(
                    key(&format!("child-{index}")),
                    2,
                    8,
                    ttl,
                    std::slice::from_ref(&root.dependency),
                    Evidence::default(),
                )
                .unwrap();
        }
        assert_eq!(cache.invalidate(&key("root")).unwrap(), 512);
        assert_eq!(cache.stats().unwrap()["entries"], 0);
        assert_eq!(cache.stats().unwrap()["bytes"], 0);
        assert_eq!(cache.stats().unwrap()["dependency_edges"], 0);
        assert_eq!(cache.stats().unwrap()["dependency_parents"], 0);
    }
}
