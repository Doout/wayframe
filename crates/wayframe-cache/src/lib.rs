//! Explicit identity/generation-scoped caches with bounded storage and single-flight loading.
use anyhow::{ensure, Result};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeSet, HashMap},
    future::Future,
    sync::{Arc, Mutex, Weak},
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
struct Entry<T> {
    value: Arc<T>,
    until: Instant,
    bytes: usize,
}
struct State<T> {
    entries: HashMap<Key, Entry<T>>,
    flights: HashMap<Key, Weak<tokio::sync::Mutex<()>>>,
    bytes: usize,
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
                entries: HashMap::new(),
                flights: HashMap::new(),
                bytes: 0,
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
            serde_json::json!({"entries":s.entries.len(),"bytes":s.bytes,"flights":s.flights.values().filter(|v|v.strong_count()>0).count()}),
        )
    }
    pub fn get(&self, key: &Key) -> Result<Option<Arc<T>>> {
        let mut s = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("cache poisoned"))?;
        if s.entries
            .get(key)
            .is_some_and(|e| e.until <= Instant::now())
        {
            if let Some(e) = s.entries.remove(key) {
                s.bytes -= e.bytes;
            }
        }
        Ok(s.entries.get(key).map(|e| e.value.clone()))
    }
    pub fn insert(&self, key: Key, value: T, bytes: usize, ttl: Duration) -> Result<Arc<T>> {
        ensure!(
            !ttl.is_zero() && ttl <= Duration::from_secs(3600) && bytes <= self.maximum_bytes,
            "invalid cache entry"
        );
        let mut s = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("cache poisoned"))?;
        let now = Instant::now();
        let mut removed = 0;
        s.entries.retain(|_, e| {
            if e.until <= now {
                removed += e.bytes;
                false
            } else {
                true
            }
        });
        s.bytes -= removed;
        if let Some(old) = s.entries.remove(&key) {
            s.bytes -= old.bytes;
        }
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
            if let Some(old) = s.entries.remove(&oldest) {
                s.bytes -= old.bytes;
            }
        }
        ensure!(
            s.bytes.saturating_add(bytes) <= self.maximum_bytes,
            "cache byte capacity"
        );
        let value = Arc::new(value);
        s.bytes += bytes;
        s.entries.insert(
            key,
            Entry {
                value: value.clone(),
                until: now + ttl,
                bytes,
            },
        );
        Ok(value)
    }
    pub fn invalidate_generation(&self, generation: u64) -> Result<()> {
        let mut s = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("cache poisoned"))?;
        let mut removed = 0;
        s.entries.retain(|key, e| {
            if key.generation != generation {
                removed += e.bytes;
                false
            } else {
                true
            }
        });
        s.bytes -= removed;
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
