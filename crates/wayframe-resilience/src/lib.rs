//! Circuit breakers, bounded keyed admission and active probes. Tool calls are never replayed.
use anyhow::{ensure, Result};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, Weak},
    time::{Duration, Instant},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    Success,
    UpstreamFailure,
    LocalBudget,
    Cancelled,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    Closed,
    Open,
    HalfOpen,
}
struct State {
    mode: Mode,
    until: Instant,
    failures: u32,
    active: usize,
    probe: bool,
}
struct Inner {
    state: Mutex<State>,
    threshold: u32,
    cooldown: Duration,
    maximum: usize,
}
#[derive(Clone)]
pub struct Breaker(Arc<Inner>);
impl Breaker {
    pub fn new(threshold: u32, cooldown: Duration, maximum: usize) -> Result<Self> {
        ensure!(
            threshold > 0
                && threshold <= 10000
                && !cooldown.is_zero()
                && cooldown <= Duration::from_secs(600)
                && (1..=100000).contains(&maximum),
            "invalid breaker bounds"
        );
        Ok(Self(Arc::new(Inner {
            state: Mutex::new(State {
                mode: Mode::Closed,
                until: Instant::now(),
                failures: 0,
                active: 0,
                probe: false,
            }),
            threshold,
            cooldown,
            maximum,
        })))
    }
    pub fn admit(&self) -> Result<Lease> {
        let mut s = self
            .0
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("breaker poisoned"))?;
        if s.mode == Mode::Open && s.until <= Instant::now() {
            s.mode = Mode::HalfOpen;
        }
        ensure!(
            s.mode != Mode::Open
                && s.active < self.0.maximum
                && !(s.mode == Mode::HalfOpen && s.probe),
            "circuit unavailable"
        );
        let probe = s.mode == Mode::HalfOpen;
        s.active += 1;
        if probe {
            s.probe = true;
        }
        Ok(Lease {
            inner: self.0.clone(),
            probe,
            outcome: Outcome::Cancelled,
        })
    }
    pub fn mode(&self) -> Result<Mode> {
        Ok(self
            .0
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("breaker poisoned"))?
            .mode)
    }
}
pub struct Lease {
    inner: Arc<Inner>,
    probe: bool,
    outcome: Outcome,
}
impl Lease {
    pub fn complete(mut self, outcome: Outcome) {
        self.outcome = outcome;
    }
}
impl Drop for Lease {
    fn drop(&mut self) {
        if let Ok(mut s) = self.inner.state.lock() {
            s.active -= 1;
            if self.probe {
                s.probe = false;
            }
            match self.outcome {
                Outcome::Success if self.probe || s.mode == Mode::Closed => {
                    s.mode = Mode::Closed;
                    s.failures = 0;
                }
                Outcome::UpstreamFailure => {
                    s.failures = s.failures.saturating_add(1);
                    if self.probe || s.failures >= self.inner.threshold {
                        s.mode = Mode::Open;
                        s.until = Instant::now() + self.inner.cooldown;
                    }
                }
                _ if self.probe => {
                    s.mode = Mode::Open;
                    s.until = Instant::now() + self.inner.cooldown;
                }
                _ => {}
            }
        }
    }
}
struct QuotaState {
    keys: HashMap<String, Weak<Semaphore>>,
}
#[derive(Clone)]
pub struct Quota {
    state: Arc<Mutex<QuotaState>>,
    capacity: usize,
    max_keys: usize,
    queue: Arc<Semaphore>,
}
impl Quota {
    pub fn new(capacity: usize, max_keys: usize, max_queued: usize) -> Result<Self> {
        ensure!(
            (1..=100000).contains(&capacity)
                && (1..=100000).contains(&max_keys)
                && max_queued <= 100000,
            "invalid quota bounds"
        );
        Ok(Self {
            state: Arc::new(Mutex::new(QuotaState {
                keys: HashMap::new(),
            })),
            capacity,
            max_keys,
            queue: Arc::new(Semaphore::new(max_queued)),
        })
    }
    fn key(&self, key: &str) -> Result<Arc<Semaphore>> {
        ensure!(!key.is_empty() && key.len() <= 1024, "invalid quota key");
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("quota poisoned"))?;
        state.keys.retain(|_, v| v.strong_count() > 0);
        if let Some(value) = state.keys.get(key).and_then(Weak::upgrade) {
            return Ok(value);
        }
        ensure!(state.keys.len() < self.max_keys, "quota key capacity");
        let value = Arc::new(Semaphore::new(self.capacity));
        state.keys.insert(key.into(), Arc::downgrade(&value));
        Ok(value)
    }
    pub fn try_admit(&self, key: &str) -> Result<OwnedSemaphorePermit> {
        Ok(self.key(key)?.try_acquire_owned()?)
    }
    pub async fn admit(
        &self,
        key: &str,
        deadline: tokio::time::Instant,
    ) -> Result<OwnedSemaphorePermit> {
        let semaphore = self.key(key)?;
        if let Ok(permit) = semaphore.clone().try_acquire_owned() {
            return Ok(permit);
        }
        let _queue = self.queue.clone().try_acquire_owned()?;
        Ok(tokio::time::timeout_at(deadline, semaphore.acquire_owned()).await??)
    }
}
pub struct Probe {
    task: tokio::task::JoinHandle<()>,
    stop: CancellationToken,
}
impl Drop for Probe {
    fn drop(&mut self) {
        self.stop.cancel();
        self.task.abort();
    }
}
impl Probe {
    pub fn start(
        pool: wayframe_lb::Pool,
        targets: Vec<(String, reqwest::Url, reqwest::Client)>,
        interval: Duration,
        timeout: Duration,
    ) -> Result<Self> {
        ensure!(
            !targets.is_empty()
                && targets.len() <= 128
                && !interval.is_zero()
                && interval <= Duration::from_secs(600)
                && !timeout.is_zero()
                && timeout <= Duration::from_secs(30),
            "invalid probes"
        );
        for (_, url, _) in &targets {
            ensure!(
                matches!(url.scheme(), "http" | "https")
                    && url.username().is_empty()
                    && url.password().is_none()
                    && url.fragment().is_none(),
                "invalid health target"
            );
        }
        for (id, _, _) in &targets {
            pool.set_available(id, false)?;
        }
        let stop = CancellationToken::new();
        let cancel = stop.clone();
        let task = tokio::spawn(async move {
            let mut tick = tokio::time::interval(interval);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {_=cancel.cancelled()=>return,_=tick.tick()=>{
                    use futures_util::StreamExt;
                    let checks=futures_util::stream::iter(targets.clone().into_iter().map(|(id,url,client)|async move {let response=tokio::time::timeout(timeout,client.get(url.clone()).send()).await;(id,matches!(response,Ok(Ok(response)) if response.status().is_success()))})).buffer_unordered(8);
                    tokio::pin!(checks);
                    loop {tokio::select! {_=cancel.cancelled()=>return,next=checks.next()=>match next {Some((id,healthy))=>{let _=pool.set_available(&id,healthy);},None=>break}}}
                }}
            }
        });
        Ok(Self { task, stop })
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn half_open_is_single_probe_and_local_failure_is_neutral() {
        let breaker = Breaker::new(1, Duration::from_millis(5), 2).unwrap();
        breaker.admit().unwrap().complete(Outcome::LocalBudget);
        assert_eq!(breaker.mode().unwrap(), Mode::Closed);
        breaker.admit().unwrap().complete(Outcome::UpstreamFailure);
        assert!(breaker.admit().is_err());
        tokio::time::sleep(Duration::from_millis(10)).await;
        let probe = breaker.admit().unwrap();
        assert!(breaker.admit().is_err());
        probe.complete(Outcome::Success);
        assert_eq!(breaker.mode().unwrap(), Mode::Closed);
    }
    #[tokio::test]
    async fn quota_bounds_keys_queue_and_preserves_accounting() {
        let quota = Quota::new(1, 2, 1).unwrap();
        let a = quota.try_admit("a").unwrap();
        assert!(quota.try_admit("a").is_err());
        let b = quota.try_admit("b").unwrap();
        assert!(quota.try_admit("c").is_err());
        assert!(quota
            .admit("a", tokio::time::Instant::now() + Duration::from_millis(5))
            .await
            .is_err());
        drop(a);
        assert!(quota.try_admit("a").is_ok());
        drop(b);
    }
}
