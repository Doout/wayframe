//! Explicit identity/generation-scoped caches with bounded storage and single-flight loading.
use anyhow::{ensure, Result};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    future::Future,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, Weak,
    },
    time::{Duration, Instant},
};
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct Key {
    pub tenant: String,
    pub subject: String,
    pub permissions: String,
    pub generation: u64,
    pub route: String,
    pub operation: String,
    pub target: String,
    pub arguments: String,
}
pub struct Scope<'a> {
    pub tenant: &'a str,
    pub subject: &'a str,
    pub permissions: &'a BTreeSet<String>,
    pub generation: u64,
    pub route: &'a str,
}
impl Key {
    pub fn new(
        scope: Scope<'_>,
        operation: &str,
        target: &str,
        arguments: &serde_json::Value,
    ) -> Result<Self> {
        let Scope {
            tenant,
            subject,
            permissions,
            generation,
            route,
        } = scope;
        for field in [tenant, subject, route, operation, target] {
            ensure!(
                field.len() <= 1024 && !field.chars().any(char::is_control),
                "invalid cache key"
            );
        }
        ensure!(
            generation > 0
                && permissions.len() <= 128
                && permissions
                    .iter()
                    .all(|s| s.len() <= 256 && !s.chars().any(char::is_control)),
            "invalid cache scope"
        );
        let arguments = serde_json::to_vec(arguments)?;
        ensure!(
            arguments.len() <= 1024 * 1024,
            "cache argument projection too large"
        );
        Ok(Self {
            tenant: tenant.into(),
            subject: subject.into(),
            permissions: hex::encode(Sha256::digest(serde_json::to_vec(permissions)?)),
            generation,
            route: route.into(),
            operation: operation.into(),
            target: target.into(),
            arguments: hex::encode(Sha256::digest(arguments)),
        })
    }
}
/// An opaque revision of a retained computation, never an authorization decision.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Dependency {
    cache_id: u64,
    key: Key,
    revision: u64,
}

/// Exact, application-supplied resource versions. Empty evidence preserves TTL-only reuse.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Evidence(BTreeMap<String, String>);
impl Evidence {
    pub fn new(resources: BTreeMap<String, String>) -> Result<Self> {
        ensure!(
            !resources.is_empty()
                && resources.len() <= 64
                && resources.iter().all(|(key, version)| [key, version]
                    .iter()
                    .all(|part| !part.is_empty()
                        && part.len() <= 256
                        && !part.chars().any(char::is_control))),
            "invalid resource evidence"
        );
        Ok(Self(resources))
    }
    pub fn resources(&self) -> &BTreeMap<String, String> {
        &self.0
    }
    fn bytes(&self) -> usize {
        self.0.iter().map(|(k, v)| k.len() + v.len()).sum()
    }
}

pub struct Retained<T> {
    pub value: Arc<T>,
    pub dependency: Dependency,
}

struct Entry<T> {
    value: Arc<T>,
    until: Instant,
    bytes: usize,
    evidence: Evidence,
    dependencies: Vec<Dependency>,
    revision: u64,
}
static NEXT_CACHE: AtomicU64 = AtomicU64::new(1);
struct State<T> {
    id: u64,
    entries: HashMap<Key, Entry<T>>,
    children: HashMap<Key, HashSet<Key>>,
    flights: HashMap<Key, Weak<tokio::sync::Mutex<()>>>,
    bytes: usize,
    revision: u64,
    invalidated: u64,
}
pub struct Cache<T> {
    state: Mutex<State<T>>,
    maximum: usize,
    maximum_bytes: usize,
    maximum_flights: usize,
}
impl<T: Send + Sync + 'static> Cache<T> {
    pub fn new(maximum: usize, maximum_bytes: usize, maximum_flights: usize) -> Result<Self> {
        ensure!(
            (1..=100000).contains(&maximum)
                && (1024..=1024 * 1024 * 1024).contains(&maximum_bytes)
                && (1..=10000).contains(&maximum_flights),
            "invalid cache bounds"
        );
        Ok(Self {
            state: Mutex::new(State {
                id: NEXT_CACHE
                    .try_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
                    .map_err(|_| anyhow::anyhow!("cache identity exhausted"))?,
                entries: HashMap::new(),
                children: HashMap::new(),
                flights: HashMap::new(),
                bytes: 0,
                revision: 0,
                invalidated: 0,
            }),
            maximum,
            maximum_bytes,
            maximum_flights,
        })
    }
    pub fn stats(&self) -> Result<serde_json::Value> {
        let s = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("cache poisoned"))?;
        Ok(
            serde_json::json!({"entries":s.entries.len(),"bytes":s.bytes,"invalidated":s.invalidated,"dependency_edges":s.children.values().map(HashSet::len).sum::<usize>(),"dependency_parents":s.children.len(),"flights":s.flights.values().filter(|v|v.strong_count()>0).count()}),
        )
    }
    /// Removes the computation and every retained descendant, including diamonds.
    fn remove_branch(s: &mut State<T>, key: &Key) -> usize {
        let mut pending = vec![key.clone()];
        let mut removed = 0;
        while let Some(key) = pending.pop() {
            if let Some(entry) = s.entries.remove(&key) {
                s.bytes -= entry.bytes;
                removed += 1;
                pending.extend(s.children.remove(&key).unwrap_or_default());
                for dependency in &entry.dependencies {
                    if let Some(children) = s.children.get_mut(&dependency.key) {
                        children.remove(&key);
                        if children.is_empty() {
                            s.children.remove(&dependency.key);
                        }
                    }
                }
            }
        }
        s.invalidated = s.invalidated.saturating_add(removed as u64);
        removed
    }
    fn prune(s: &mut State<T>) {
        let now = Instant::now();
        let expired: Vec<_> = s
            .entries
            .iter()
            .filter(|(_, e)| e.until <= now)
            .map(|(key, _)| key.clone())
            .collect();
        for key in expired {
            Self::remove_branch(s, &key);
        }
    }
    fn current(s: &State<T>, dependencies: &[Dependency]) -> bool {
        let mut pending: Vec<_> = dependencies.iter().collect();
        let mut seen = std::collections::HashSet::new();
        while let Some(dep) = pending.pop() {
            if dep.cache_id != s.id {
                return false;
            }
            let Some(entry) = s.entries.get(&dep.key) else {
                return false;
            };
            if entry.revision != dep.revision || entry.until <= Instant::now() {
                return false;
            }
            if seen.insert(&dep.key) {
                pending.extend(entry.dependencies.iter());
            }
        }
        true
    }
    pub fn invalidate(&self, key: &Key) -> Result<usize> {
        let mut s = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("cache poisoned"))?;
        Ok(Self::remove_branch(&mut s, key))
    }
    pub fn get(&self, key: &Key) -> Result<Option<Arc<T>>> {
        let mut s = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("cache poisoned"))?;
        if s.entries.get(key).is_some_and(|entry| {
            entry.until <= Instant::now()
                || !entry.evidence.0.is_empty()
                || !entry.dependencies.is_empty()
        }) {
            Self::remove_branch(&mut s, key);
        }
        Ok(s.entries.get(key).map(|entry| entry.value.clone()))
    }
    /// Caller must obtain fresh evidence under its own consistency and permission contract.
    /// This checks exact versions and parent revisions atomically with the cache lookup.
    pub fn lookup(
        &self,
        key: &Key,
        dependencies: &[Dependency],
        evidence: &Evidence,
    ) -> Result<Option<Retained<T>>> {
        let mut s = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("cache poisoned"))?;
        if s.entries
            .get(key)
            .is_some_and(|entry| entry.until <= Instant::now())
        {
            Self::remove_branch(&mut s, key);
        }
        if s.entries.get(key).is_some_and(|e| {
            e.evidence != *evidence
                || e.dependencies != dependencies
                || !Self::current(&s, dependencies)
        }) {
            Self::remove_branch(&mut s, key);
        }
        Ok(s.entries.get(key).map(|entry| Retained {
            value: entry.value.clone(),
            dependency: Dependency {
                cache_id: s.id,
                key: key.clone(),
                revision: entry.revision,
            },
        }))
    }
    pub fn insert(&self, key: Key, value: T, bytes: usize, ttl: Duration) -> Result<Arc<T>> {
        Ok(self
            .retain(key, value, bytes, ttl, &[], Evidence::default())?
            .value)
    }
    /// Rejects stale parent receipts, including a parent invalidated while work ran.
    pub fn retain(
        &self,
        key: Key,
        value: T,
        bytes: usize,
        ttl: Duration,
        dependencies: &[Dependency],
        evidence: Evidence,
    ) -> Result<Retained<T>> {
        ensure!(
            !ttl.is_zero() && ttl <= Duration::from_secs(3600) && dependencies.len() <= 64,
            "invalid cache entry"
        );
        ensure!(
            dependencies.iter().all(|dep| dep.key != key
                && dep.key.tenant == key.tenant
                && dep.key.subject == key.subject
                && dep.key.permissions == key.permissions
                && dep.key.generation == key.generation),
            "foreign or cyclic dependency"
        );
        let key_bytes = |key: &Key| {
            key.tenant.len()
                + key.subject.len()
                + key.permissions.len()
                + key.route.len()
                + key.operation.len()
                + key.target.len()
                + key.arguments.len()
                + 8
        };
        let bytes = bytes
            .saturating_add(key_bytes(&key).saturating_mul(2))
            .saturating_add(evidence.bytes())
            .saturating_add(
                dependencies
                    .iter()
                    .map(|d| 2 * key_bytes(&d.key) + key_bytes(&key) + 16)
                    .sum::<usize>(),
            );
        ensure!(bytes <= self.maximum_bytes, "cache byte capacity");
        let mut s = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("cache poisoned"))?;
        Self::prune(&mut s);
        ensure!(Self::current(&s, dependencies), "stale cache dependency");
        // Replacing a parent invalidates its previous descendants even if its value is equal.
        Self::remove_branch(&mut s, &key);
        while !s.entries.is_empty()
            && (s.entries.len() >= self.maximum
                || s.bytes.saturating_add(bytes) > self.maximum_bytes)
        {
            let oldest = s
                .entries
                .iter()
                .min_by_key(|(_, e)| e.until)
                .map(|(k, _)| k.clone())
                .expect("nonempty");
            Self::remove_branch(&mut s, &oldest);
        }
        ensure!(
            Self::current(&s, dependencies),
            "dependency evicted during admission"
        );
        s.revision = s
            .revision
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("cache revision exhausted"))?;
        let revision = s.revision;
        let value = Arc::new(value);
        s.bytes += bytes;
        for dependency in dependencies {
            s.children
                .entry(dependency.key.clone())
                .or_default()
                .insert(key.clone());
        }
        s.entries.insert(
            key.clone(),
            Entry {
                value: value.clone(),
                until: Instant::now() + ttl,
                bytes,
                evidence,
                dependencies: dependencies.to_vec(),
                revision,
            },
        );
        Ok(Retained {
            value,
            dependency: Dependency {
                cache_id: s.id,
                key,
                revision,
            },
        })
    }
    pub fn invalidate_generation(&self, generation: u64) -> Result<()> {
        let mut s = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("cache poisoned"))?;
        let keys: Vec<_> = s
            .entries
            .keys()
            .filter(|key| key.generation != generation)
            .cloned()
            .collect();
        for key in keys {
            Self::remove_branch(&mut s, &key);
        }
        Ok(())
    }
    pub async fn singleflight(
        &self,
        key: &Key,
        deadline: tokio::time::Instant,
    ) -> Result<tokio::sync::OwnedMutexGuard<()>> {
        let flight = {
            let mut s = self
                .state
                .lock()
                .map_err(|_| anyhow::anyhow!("cache poisoned"))?;
            s.flights.retain(|_, v| v.strong_count() > 0);
            if let Some(f) = s.flights.get(key).and_then(Weak::upgrade) {
                f
            } else {
                ensure!(
                    s.flights.len() < self.maximum_flights,
                    "cache flight capacity"
                );
                let f = Arc::new(tokio::sync::Mutex::new(()));
                s.flights.insert(key.clone(), Arc::downgrade(&f));
                f
            }
        };
        Ok(tokio::time::timeout_at(deadline, flight.lock_owned()).await?)
    }
    pub async fn get_or_load<F, Fut>(
        &self,
        key: Key,
        deadline: tokio::time::Instant,
        ttl: Duration,
        load: F,
    ) -> Result<Arc<T>>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<(T, usize)>>,
    {
        if let Some(value) = self.get(&key)? {
            return Ok(value);
        }
        let _permit = self.singleflight(&key, deadline).await?;
        if let Some(value) = self.get(&key)? {
            return Ok(value);
        }
        let (value, bytes) = tokio::time::timeout_at(deadline, load()).await??;
        self.insert(key, value, bytes, ttl)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn scope_isolation_and_singleflight() {
        let cache = Arc::new(Cache::new(8, 1024, 2).unwrap());
        let permissions = BTreeSet::from(["read".into()]);
        let key = Key::new(
            Scope {
                tenant: "a",
                subject: "u",
                permissions: &permissions,
                generation: 1,
                route: "route",
            },
            "tools/list",
            "",
            &serde_json::Value::Null,
        )
        .unwrap();
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let c = cache.clone();
        let k = key.clone();
        let e = entered.clone();
        let r = release.clone();
        let first = tokio::spawn(async move {
            c.get_or_load(
                k,
                tokio::time::Instant::now() + Duration::from_secs(2),
                Duration::from_secs(1),
                move || async move {
                    e.notify_one();
                    r.notified().await;
                    Ok((42u64, 8))
                },
            )
            .await
            .unwrap()
        });
        entered.notified().await;
        let c = cache.clone();
        let k = key.clone();
        let second = tokio::spawn(async move {
            c.get_or_load(
                k,
                tokio::time::Instant::now() + Duration::from_secs(2),
                Duration::from_secs(1),
                || async {
                    panic!("duplicate load");
                    #[allow(unreachable_code)]
                    Ok((0, 0))
                },
            )
            .await
            .unwrap()
        });
        release.notify_one();
        assert_eq!(*first.await.unwrap(), 42);
        assert_eq!(*second.await.unwrap(), 42);
        let mut other = key.clone();
        other.tenant = "b".into();
        assert!(cache.get(&other).unwrap().is_none());
        cache.invalidate_generation(2).unwrap();
        assert!(cache.get(&key).unwrap().is_none());
    }
}
