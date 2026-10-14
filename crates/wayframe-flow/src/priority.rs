use super::{Flow, Node, NodeSet, Topology};
use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Priority {
    #[default]
    Stable,
    CriticalPath,
    Measured,
}
#[derive(Debug)]
pub struct NodeCost {
    pub id: String,
    pub samples: u64,
    pub elapsed: Duration,
}
#[derive(Default)]
pub(super) struct Observed {
    nanos: AtomicU64,
    samples: AtomicU64,
}
impl Observed {
    pub(super) fn record(&self, elapsed: Duration) {
        let sample = elapsed.as_nanos().min(u64::MAX as u128) as u64;
        let mut old = self.nanos.load(Ordering::Relaxed);
        loop {
            let next = if old == 0 {
                sample
            } else {
                ((old as u128 * 7 + sample as u128) / 8) as u64
            };
            match self
                .nanos
                .compare_exchange_weak(old, next, Ordering::Relaxed, Ordering::Relaxed)
            {
                Ok(_) => break,
                Err(current) => old = current,
            }
        }
        self.samples.fetch_add(1, Ordering::Relaxed);
    }
    pub(super) fn estimate(&self, hint: Duration) -> u64 {
        if self.samples.load(Ordering::Relaxed) >= 3 {
            self.nanos.load(Ordering::Relaxed)
        } else {
            hint.as_nanos().min(u64::MAX as u128) as u64
        }
    }
}
/// Longest downstream path through retained nodes. Dependencies were validated acyclic at compile time.
pub(super) fn scores(
    nodes: &[Node],
    topology: &Topology,
    active: Option<NodeSet>,
    measured: bool,
) -> Vec<u64> {
    fn visit(
        i: usize,
        nodes: &[Node],
        topology: &Topology,
        active: Option<NodeSet>,
        measured: bool,
        memo: &mut [Option<u64>],
    ) -> u64 {
        if let Some(score) = memo[i] {
            return score;
        }
        if active.is_some_and(|active| !active.contains(i)) {
            memo[i] = Some(0);
            return 0;
        }
        let downstream = topology.dependents[i]
            .iter()
            .map(|child| visit(*child, nodes, topology, active, measured, memo))
            .max()
            .unwrap_or(0);
        let own = if measured {
            nodes[i].observed.estimate(nodes[i].cost_hint)
        } else {
            nodes[i].cost_hint.as_nanos().min(u64::MAX as u128) as u64
        };
        let score = own.saturating_add(downstream);
        memo[i] = Some(score);
        score
    }
    let mut memo = vec![None; nodes.len()];
    for i in 0..nodes.len() {
        visit(i, nodes, topology, active, measured, &mut memo);
    }
    memo.into_iter().map(|score| score.unwrap_or(0)).collect()
}
impl<C> Flow<C> {
    pub fn node_costs(&self) -> Vec<NodeCost> {
        self.nodes
            .iter()
            .skip(1)
            .map(|node| NodeCost {
                id: node.id.clone(),
                samples: node.observed.samples.load(Ordering::Relaxed),
                elapsed: Duration::from_nanos(node.observed.nanos.load(Ordering::Relaxed)),
            })
            .collect()
    }
}
