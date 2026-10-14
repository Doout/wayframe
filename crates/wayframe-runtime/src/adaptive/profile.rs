use super::{Path, Selection, SelectionReason};
use std::{
    collections::{HashMap, VecDeque},
    fmt,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

#[derive(Clone, Copy, Debug)]
pub struct Workload {
    pub key: u64,
    pub width: usize,
}
#[derive(Clone, Copy, Debug)]
pub struct ProfileSettings {
    pub capacity: usize,
    pub min_samples: u64,
    pub explore_every: u64,
    pub improvement_percent: u64,
    pub ttl: Duration,
    pub min_budget: Duration,
}
impl Default for ProfileSettings {
    fn default() -> Self {
        Self {
            capacity: 64,
            min_samples: 8,
            explore_every: 64,
            improvement_percent: 10,
            ttl: Duration::from_secs(60),
            min_budget: Duration::from_millis(10),
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProfileError(pub &'static str);
impl fmt::Display for ProfileError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid execution profile: {}", self.0)
    }
}
impl std::error::Error for ProfileError {}
impl ProfileSettings {
    pub fn validate(&self) -> Result<(), ProfileError> {
        if !(1..=1024).contains(&self.capacity)
            || !(2..=1024).contains(&self.min_samples)
            || !(8..=65536).contains(&self.explore_every)
            || !(1..=50).contains(&self.improvement_percent)
            || !(Duration::from_secs(1)..=Duration::from_secs(3600)).contains(&self.ttl)
            || self.min_budget > Duration::from_secs(60)
        {
            return Err(ProfileError("settings_out_of_bounds"));
        }
        Ok(())
    }
}
#[derive(Clone, Copy, Debug, Default)]
pub struct ProfileStatistics {
    pub classes: usize,
    pub capacity: usize,
    pub warmups: u64,
    pub explorations: u64,
    pub measured: u64,
    pub budget_skips: u64,
    pub evictions: u64,
    pub stale_observations: u64,
    pub successes: u64,
    pub failures: u64,
}
#[derive(Clone, Copy, Default)]
struct Cost {
    samples: u64,
    nanos: u64,
}
impl Cost {
    fn record(&mut self, elapsed: Duration) {
        let sample = elapsed.as_nanos().min(u64::MAX as u128) as u64;
        self.nanos = if self.samples == 0 {
            sample
        } else {
            ((self.nanos as u128 * 7 + sample as u128) / 8) as u64
        };
        self.samples = self.samples.saturating_add(1);
    }
}
struct Class {
    stamp: Arc<()>,
    width: usize,
    costs: [Cost; 2],
    attempts: [u64; 2],
    calls: u64,
    preferred: Path,
    updated: Instant,
}
#[derive(Default)]
struct State {
    classes: HashMap<u64, Class>,
    recent: VecDeque<u64>,
    statistics: ProfileStatistics,
}
pub(super) struct Profile {
    settings: ProfileSettings,
    state: Mutex<State>,
}
pub(super) struct Ticket {
    key: u64,
    stamp: Arc<()>,
    path: Path,
}
fn index(path: Path) -> usize {
    if path == Path::Linear {
        0
    } else {
        1
    }
}
fn other(path: Path) -> Path {
    if path == Path::Linear {
        Path::Graph
    } else {
        Path::Linear
    }
}
impl Profile {
    pub(super) fn new(settings: ProfileSettings) -> Result<Self, ProfileError> {
        settings.validate()?;
        Ok(Self {
            settings,
            state: Mutex::new(State::default()),
        })
    }
    pub(super) fn choose(
        &self,
        workload: Workload,
        fallback: Selection,
        budget: Duration,
        now: Instant,
    ) -> Result<(Selection, Option<Ticket>), ProfileError> {
        if workload.width > 256 {
            return Err(ProfileError("workload_width"));
        }
        if workload.width < 2
            || matches!(
                fallback.reason,
                SelectionReason::SmallWorkload
                    | SelectionReason::ExplicitRule
                    | SelectionReason::AdmissionPressure
            )
        {
            return Ok((fallback, None));
        }
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        let reset = state.classes.get(&workload.key).is_none_or(|class| {
            class.width != workload.width
                || now.saturating_duration_since(class.updated) >= self.settings.ttl
        });
        if reset {
            if !state.classes.contains_key(&workload.key)
                && state.classes.len() == self.settings.capacity
            {
                if let Some(key) = state.recent.pop_front() {
                    state.classes.remove(&key);
                }
                state.statistics.evictions = state.statistics.evictions.saturating_add(1);
            }
            state.classes.insert(
                workload.key,
                Class {
                    stamp: Arc::new(()),
                    width: workload.width,
                    costs: [Cost::default(); 2],
                    attempts: [0; 2],
                    calls: 0,
                    preferred: fallback.path,
                    updated: now,
                },
            );
        }
        state.recent.retain(|key| *key != workload.key);
        state.recent.push_back(workload.key);
        let class = state.classes.get_mut(&workload.key).expect("profile class");
        class.calls = class.calls.saturating_add(1);
        let warm = class
            .costs
            .iter()
            .all(|cost| cost.samples >= self.settings.min_samples);
        if warm {
            let current = class.costs[index(class.preferred)].nanos as u128;
            let alternative = class.costs[index(other(class.preferred))].nanos as u128;
            if alternative * 100 < current * (100 - self.settings.improvement_percent) as u128 {
                class.preferred = other(class.preferred);
            }
        }
        let (mut path, mut reason) = if !warm {
            let path = if class.attempts[index(fallback.path)]
                <= class.attempts[index(other(fallback.path))]
            {
                fallback.path
            } else {
                other(fallback.path)
            };
            (path, SelectionReason::ProfileWarmup)
        } else if class.calls.is_multiple_of(self.settings.explore_every) {
            (other(class.preferred), SelectionReason::ProfileExplore)
        } else {
            (class.preferred, SelectionReason::MeasuredCost)
        };
        if !warm || reason == SelectionReason::ProfileExplore {
            // An unproven serial path can cost up to width times the observed parallel path.
            let known = class.costs[index(other(path))];
            let scale = if path == Path::Linear {
                workload.width as u128
            } else {
                1
            };
            let conservative = (known.nanos as u128)
                .saturating_mul(scale)
                .saturating_mul(2);
            if budget < self.settings.min_budget
                || (known.samples > 0 && budget.as_nanos() < conservative)
            {
                path = if warm { class.preferred } else { fallback.path };
                reason = SelectionReason::ProfileBudget;
            }
        }
        class.attempts[index(path)] = class.attempts[index(path)].saturating_add(1);
        let ticket = Ticket {
            key: workload.key,
            stamp: class.stamp.clone(),
            path,
        };
        match reason {
            SelectionReason::ProfileWarmup => {
                state.statistics.warmups = state.statistics.warmups.saturating_add(1)
            }
            SelectionReason::ProfileExplore => {
                state.statistics.explorations = state.statistics.explorations.saturating_add(1)
            }
            SelectionReason::ProfileBudget => {
                state.statistics.budget_skips = state.statistics.budget_skips.saturating_add(1)
            }
            _ => state.statistics.measured = state.statistics.measured.saturating_add(1),
        }
        Ok((Selection { path, reason }, Some(ticket)))
    }
    pub(super) fn observe(&self, ticket: Ticket, elapsed: Duration, success: bool, now: Instant) {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        let Some(class) = state
            .classes
            .get_mut(&ticket.key)
            .filter(|class| Arc::ptr_eq(&class.stamp, &ticket.stamp))
        else {
            state.statistics.stale_observations =
                state.statistics.stale_observations.saturating_add(1);
            return;
        };
        if success {
            class.costs[index(ticket.path)].record(elapsed);
            class.updated = now;
            state.statistics.successes = state.statistics.successes.saturating_add(1);
        } else {
            state.statistics.failures = state.statistics.failures.saturating_add(1);
        }
    }
    pub(super) fn statistics(&self) -> ProfileStatistics {
        let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        ProfileStatistics {
            classes: state.classes.len(),
            capacity: self.settings.capacity,
            ..state.statistics
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn settings() -> ProfileSettings {
        ProfileSettings {
            min_samples: 2,
            explore_every: 8,
            ..ProfileSettings::default()
        }
    }
    fn fallback() -> Selection {
        Selection::for_width(2, 2)
    }
    #[test]
    fn successful_samples_choose_the_faster_equivalent_path_after_warmup() {
        let profile = Profile::new(settings()).unwrap();
        let now = Instant::now();
        for _ in 0..4 {
            let (choice, ticket) = profile
                .choose(
                    Workload { key: 1, width: 2 },
                    fallback(),
                    Duration::from_secs(1),
                    now,
                )
                .unwrap();
            assert_eq!(choice.reason, SelectionReason::ProfileWarmup);
            let cost = if choice.path == Path::Linear { 1 } else { 4 };
            profile.observe(ticket.unwrap(), Duration::from_millis(cost), true, now);
        }
        let (choice, _) = profile
            .choose(
                Workload { key: 1, width: 2 },
                fallback(),
                Duration::from_secs(1),
                now,
            )
            .unwrap();
        assert_eq!(choice.path, Path::Linear);
        assert_eq!(choice.reason, SelectionReason::MeasuredCost);
        assert_eq!(profile.statistics().successes, 4);
    }
    #[test]
    fn close_costs_keep_the_current_path_and_failed_samples_never_make_a_path_cheaper() {
        let profile = Profile::new(settings()).unwrap();
        let now = Instant::now();
        for _ in 0..4 {
            let (choice, ticket) = profile
                .choose(
                    Workload { key: 1, width: 2 },
                    fallback(),
                    Duration::from_secs(1),
                    now,
                )
                .unwrap();
            let cost = if choice.path == Path::Linear { 95 } else { 100 };
            profile.observe(ticket.unwrap(), Duration::from_micros(cost), true, now);
        }
        let (choice, ticket) = profile
            .choose(
                Workload { key: 1, width: 2 },
                fallback(),
                Duration::from_secs(1),
                now,
            )
            .unwrap();
        assert_eq!(choice.path, Path::Graph);
        profile.observe(ticket.unwrap(), Duration::ZERO, false, now);
        assert_eq!(profile.statistics().failures, 1);
        assert_eq!(
            profile.state.lock().unwrap().classes[&1].costs[1].nanos,
            100000
        );
    }
    #[test]
    fn exploration_respects_remaining_budget_and_hard_operator_rules() {
        let profile = Profile::new(settings()).unwrap();
        let now = Instant::now();
        let (_, ticket) = profile
            .choose(
                Workload { key: 1, width: 16 },
                fallback(),
                Duration::from_secs(1),
                now,
            )
            .unwrap();
        profile.observe(ticket.unwrap(), Duration::from_millis(20), true, now);
        let (choice, _) = profile
            .choose(
                Workload { key: 1, width: 16 },
                fallback(),
                Duration::from_millis(100),
                now,
            )
            .unwrap();
        assert_eq!(choice.path, Path::Graph);
        assert_eq!(choice.reason, SelectionReason::ProfileBudget);
        let forced = Selection {
            path: Path::Linear,
            reason: SelectionReason::ExplicitRule,
        };
        let (choice, ticket) = profile
            .choose(
                Workload { key: 2, width: 16 },
                forced,
                Duration::from_secs(1),
                now,
            )
            .unwrap();
        assert_eq!(choice.path, Path::Linear);
        assert!(ticket.is_none());
        assert_eq!(profile.statistics().classes, 1);
    }
    #[test]
    fn expiry_and_eviction_reject_late_observations_without_reusing_old_costs() {
        let profile = Profile::new(ProfileSettings {
            capacity: 1,
            ttl: Duration::from_secs(1),
            ..settings()
        })
        .unwrap();
        let now = Instant::now();
        let (_, old) = profile
            .choose(
                Workload { key: 1, width: 2 },
                fallback(),
                Duration::from_secs(1),
                now,
            )
            .unwrap();
        let (_, expired) = profile
            .choose(
                Workload { key: 1, width: 2 },
                fallback(),
                Duration::from_secs(1),
                now + Duration::from_secs(2),
            )
            .unwrap();
        profile.observe(
            old.unwrap(),
            Duration::from_millis(100),
            true,
            now + Duration::from_secs(2),
        );
        profile
            .choose(
                Workload { key: 2, width: 2 },
                fallback(),
                Duration::from_secs(1),
                now + Duration::from_secs(2),
            )
            .unwrap();
        profile.observe(
            expired.unwrap(),
            Duration::from_millis(100),
            true,
            now + Duration::from_secs(2),
        );
        let stats = profile.statistics();
        assert_eq!(stats.classes, 1);
        assert_eq!(stats.evictions, 1);
        assert_eq!(stats.stale_observations, 2);
        assert_eq!(stats.successes, 0);
    }
    #[test]
    fn workload_changes_and_invalid_bounds_do_not_keep_incompatible_profiles() {
        assert!(Profile::new(ProfileSettings {
            capacity: 0,
            ..settings()
        })
        .is_err());
        let profile = Profile::new(settings()).unwrap();
        let now = Instant::now();
        let (_, ticket) = profile
            .choose(
                Workload { key: 1, width: 2 },
                fallback(),
                Duration::from_secs(1),
                now,
            )
            .unwrap();
        profile
            .choose(
                Workload { key: 1, width: 3 },
                fallback(),
                Duration::from_secs(1),
                now,
            )
            .unwrap();
        profile.observe(ticket.unwrap(), Duration::from_secs(1), true, now);
        assert_eq!(profile.statistics().stale_observations, 1);
        assert!(profile
            .choose(
                Workload { key: 1, width: 257 },
                fallback(),
                Duration::from_secs(1),
                now
            )
            .is_err());
    }
}
