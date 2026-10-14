//! Generation-owned structural specialization. Plans never contain request values or decisions.
use super::{Context, Error, Flow, NodeSet, Port};
use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};
const CAPACITY: usize = 32;

#[derive(Clone)]
pub struct Output {
    scope: Arc<()>,
    index: usize,
}
impl<T> Port<T> {
    pub fn output(&self) -> Output {
        Output {
            scope: self.scope.clone(),
            index: self.index,
        }
    }
}
#[derive(Clone)]
pub struct Prepared {
    pub(super) scope: Arc<()>,
    pub(super) active: NodeSet,
    total: usize,
}
impl Prepared {
    /// Input is excluded from these counts.
    pub fn retained_nodes(&self) -> usize {
        self.active.iter().count() - 1
    }
    pub fn omitted_nodes(&self) -> usize {
        self.total - self.active.iter().count()
    }
    pub fn belongs_to<C>(&self, flow: &Flow<C>) -> bool {
        Arc::ptr_eq(&self.scope, &flow.scope)
    }
}
#[derive(Clone, Copy, Debug)]
pub struct CacheStatistics {
    pub entries: usize,
    pub capacity: usize,
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
}
#[derive(Default)]
pub(super) struct Cache {
    entries: HashMap<NodeSet, NodeSet>,
    recent: VecDeque<NodeSet>,
    hits: u64,
    misses: u64,
    evictions: u64,
}
impl<C: Context + Sync> Flow<C> {
    /// Prepare required output ancestry plus every control and non-discardable action.
    /// The bounded cache is owned by this compiled generation; it stores structural masks only.
    pub fn prepare(&self, outputs: &[Output]) -> Result<Prepared, Error> {
        if outputs.len() > 257 {
            return Err(Error::InvalidGraph("output_limit"));
        }
        let mut key = NodeSet::default();
        for output in outputs {
            if !Arc::ptr_eq(&output.scope, &self.scope) || output.index >= self.nodes.len() {
                return Err(Error::InvalidGraph("foreign_output"));
            }
            key.insert(output.index);
        }
        let mut cache = self.plan_cache.lock().unwrap_or_else(|p| p.into_inner());
        let active = if let Some(active) = cache.entries.get(&key).copied() {
            cache.hits = cache.hits.saturating_add(1);
            cache.recent.retain(|old| *old != key);
            cache.recent.push_back(key);
            active
        } else {
            cache.misses = cache.misses.saturating_add(1);
            let mut active = key;
            // Unannotated work can have side effects; controls are always retained.
            let mut ancestors: Vec<_> = self
                .nodes
                .iter()
                .enumerate()
                .filter_map(|(i, n)| {
                    if !n.discardable {
                        active.insert(i);
                        Some(i)
                    } else {
                        None
                    }
                })
                .chain(key.iter())
                .collect();
            let mut visited = NodeSet::default();
            while let Some(i) = ancestors.pop() {
                if visited.contains(i) {
                    continue;
                }
                visited.insert(i);
                for dependency in &self.nodes[i].deps {
                    active.insert(*dependency);
                    ancestors.push(*dependency);
                }
            }
            if cache.entries.len() == CAPACITY {
                if let Some(oldest) = cache.recent.pop_front() {
                    cache.entries.remove(&oldest);
                }
                cache.evictions = cache.evictions.saturating_add(1);
            }
            cache.entries.insert(key, active);
            cache.recent.push_back(key);
            active
        };
        Ok(Prepared {
            scope: self.scope.clone(),
            active,
            total: self.nodes.len(),
        })
    }
    pub fn plan_statistics(&self) -> CacheStatistics {
        let cache = self.plan_cache.lock().unwrap_or_else(|p| p.into_inner());
        CacheStatistics {
            entries: cache.entries.len(),
            capacity: CAPACITY,
            hits: cache.hits,
            misses: cache.misses,
            evictions: cache.evictions,
        }
    }
}
