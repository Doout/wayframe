//! Bounded latency metrics and durable audit records, with optional OTLP export.
use anyhow::{ensure, Result};
#[cfg(feature = "otel")]
mod telemetry;
#[cfg(feature = "otel")]
pub use telemetry::{init, Telemetry};
/// A bounded histogram. Labels are configured plugin/route IDs, never user-controlled paths.
pub struct Histogram {
    bounds: Vec<u64>,
    buckets: Vec<std::sync::atomic::AtomicU64>,
    sum: std::sync::atomic::AtomicU64,
}
impl Histogram {
    pub fn new(bounds: Vec<u64>) -> Result<Self> {
        ensure!(
            !bounds.is_empty() && bounds.len() <= 64 && bounds.windows(2).all(|p| p[0] < p[1]),
            "invalid histogram bounds"
        );
        let buckets = (0..=bounds.len())
            .map(|_| std::sync::atomic::AtomicU64::new(0))
            .collect();
        Ok(Self {
            bounds,
            buckets,
            sum: std::sync::atomic::AtomicU64::new(0),
        })
    }
    pub fn observe(&self, nanos: u64) {
        use std::sync::atomic::Ordering;
        let i = self.bounds.partition_point(|bound| *bound < nanos);
        self.buckets[i].fetch_add(1, Ordering::Relaxed);
        let _ = self
            .sum
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                Some(n.saturating_add(nanos))
            });
    }
    pub fn snapshot(&self) -> serde_json::Value {
        use std::sync::atomic::Ordering;
        serde_json::json!({"bounds_ns":self.bounds,"bucket_counts":self.buckets.iter().map(|b|b.load(Ordering::Relaxed)).collect::<Vec<_>>(),"sum_ns":self.sum.load(Ordering::Relaxed)})
    }
}
/// Durable append-only audit entries; callers supply only reviewed/redacted fields.
pub struct Audit {
    file: std::sync::Mutex<std::fs::File>,
    previous: std::sync::Mutex<String>,
    maximum: u64,
}
#[derive(serde::Serialize)]
pub struct AuditEvent {
    pub actor: String,
    pub action: String,
    pub object: String,
    pub revision: u64,
    pub outcome: String,
}
impl Audit {
    pub fn open(path: &std::path::Path, maximum: u64) -> Result<Self> {
        use std::io::{BufRead, BufReader};
        ensure!(
            path.is_absolute() && (1024..=1024 * 1024 * 1024).contains(&maximum),
            "invalid audit file/budget"
        );
        if let Ok(meta) = path.symlink_metadata() {
            ensure!(
                meta.is_file() && !meta.file_type().is_symlink() && meta.len() <= maximum,
                "invalid audit file"
            );
        }
        let mut options = std::fs::OpenOptions::new();
        options.append(true).read(true).create(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let file = options.open(path)?;
        fs2::FileExt::try_lock_exclusive(&file)
            .map_err(|_| anyhow::anyhow!("audit file already owned"))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            ensure!(
                file.metadata()?.permissions().mode() & 0o077 == 0,
                "audit file must be private"
            );
        }
        let mut previous = String::new();
        for line in BufReader::new(file.try_clone()?).lines() {
            let line = line?;
            ensure!(line.len() <= 65536, "audit entry too large");
            let value: serde_json::Value = serde_json::from_str(&line)?;
            ensure!(
                value["previous"].as_str() == Some(previous.as_str()),
                "audit chain mismatch"
            );
            let record = serde_json::to_vec(&value["event"])?;
            use sha2::Digest;
            let digest = hex::encode(sha2::Sha256::digest(
                [previous.as_bytes(), record.as_slice()].concat(),
            ));
            ensure!(
                value["digest"].as_str() == Some(digest.as_str()),
                "audit digest mismatch"
            );
            previous = digest;
        }
        Ok(Self {
            file: std::sync::Mutex::new(file),
            previous: std::sync::Mutex::new(previous),
            maximum,
        })
    }
    pub fn append(&self, event: AuditEvent) -> Result<()> {
        use sha2::Digest;
        use std::io::Write;
        for s in [&event.actor, &event.action, &event.object, &event.outcome] {
            ensure!(
                !s.is_empty() && s.len() <= 256 && !s.chars().any(char::is_control),
                "invalid redacted audit field"
            );
        }
        let mut file = self
            .file
            .lock()
            .map_err(|_| anyhow::anyhow!("audit poisoned"))?;
        let mut previous = self
            .previous
            .lock()
            .map_err(|_| anyhow::anyhow!("audit poisoned"))?;
        let mut value = serde_json::to_value(event)?;
        value["timestamp_unix_ms"] = serde_json::json!(std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis());
        let record = serde_json::to_vec(&value)?;
        let digest = hex::encode(sha2::Sha256::digest(
            [previous.as_bytes(), record.as_slice()].concat(),
        ));
        let mut bytes = serde_json::to_vec(
            &serde_json::json!({"event":value,"previous":*previous,"digest":digest}),
        )?;
        bytes.push(b'\n');
        ensure!(
            file.metadata()?.len().saturating_add(bytes.len() as u64) <= self.maximum,
            "audit capacity reached"
        );
        file.write_all(&bytes)?;
        file.sync_data()?;
        *previous = digest;
        Ok(())
    }
}
