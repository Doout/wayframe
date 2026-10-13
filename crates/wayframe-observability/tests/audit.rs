use wayframe_observability::{Audit, AuditEvent, Histogram};
#[test]
fn audit_is_exclusive_durable_bounded_and_detects_tampering() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("audit.jsonl");
    let audit = Audit::open(&path, 1024).unwrap();
    assert!(Audit::open(&path, 1024).is_err());
    let event = || AuditEvent {
        actor: "admin".into(),
        action: "PUT".into(),
        object: "rules".into(),
        revision: 2,
        outcome: "committed".into(),
    };
    audit.append(event()).unwrap();
    drop(audit);
    let audit = Audit::open(&path, 1024).unwrap();
    audit.append(event()).unwrap();
    let mut full = false;
    for _ in 0..10 {
        if audit.append(event()).is_err() {
            full = true;
            break;
        }
    }
    assert!(full);
    drop(audit);
    let data = std::fs::read_to_string(&path).unwrap();
    assert!(data.contains("timestamp_unix_ms"));
    std::fs::write(&path, data.replacen("committed", "corrupted", 1)).unwrap();
    assert!(Audit::open(&path, 1024).is_err());
}
#[test]
fn histogram_buckets_and_sum_are_exact() {
    let h = Histogram::new(vec![10, 20]).unwrap();
    for n in [0, 10, 11, 30] {
        h.observe(n);
    }
    assert_eq!(h.snapshot()["bucket_counts"], serde_json::json!([2, 1, 1]));
    assert_eq!(h.snapshot()["sum_ns"], 51);
}
