//! Typed plugin factories and prepared lifecycle bundles. Native implementations are trusted.
use anyhow::{ensure, Result};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeSet, HashMap, HashSet},
    future::Future,
    pin::Pin,
    sync::Arc,
    time::Duration,
};
use wayframe_core::{Block, Context, Phase};
pub type AsyncResult<'a, T = ()> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>;
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub id: String,
    pub kind: String,
    pub version: String,
    #[serde(default)]
    pub settings: serde_json::Value,
    #[serde(default)]
    pub after: Vec<String>,
}
#[derive(Clone, Debug, Serialize)]
pub struct Descriptor {
    pub kind: String,
    pub version: String,
    pub phase: Option<PhaseInfo>,
    pub requires: BTreeSet<String>,
    pub provides: BTreeSet<String>,
    pub settings_schema: serde_json::Value,
}
#[derive(Clone, Copy, Debug, Serialize, PartialEq, Eq)]
pub enum PhaseInfo {
    Admit,
    Authenticate,
    Inspect,
    Authorize,
    Limit,
    Dispatch,
}
impl From<Phase> for PhaseInfo {
    fn from(p: Phase) -> Self {
        match p {
            Phase::Admit => Self::Admit,
            Phase::Authenticate => Self::Authenticate,
            Phase::Inspect => Self::Inspect,
            Phase::Authorize => Self::Authorize,
            Phase::Limit => Self::Limit,
            Phase::Dispatch => Self::Dispatch,
        }
    }
}
/// Implementations must cancel owned background resources on Drop. Explicit async shutdown provides graceful draining.
pub trait Plugin<C: Context>: Send + Sync + 'static {
    fn id(&self) -> &str;
    fn block(&self) -> Option<Arc<dyn Block<C>>> {
        None
    }
    /// Prepared transport-specific hooks. Consumers downcast explicitly typed export wrappers.
    fn exports(&self) -> Vec<Arc<dyn std::any::Any + Send + Sync>> {
        Vec::new()
    }
    fn stats(&self) -> serde_json::Value {
        serde_json::json!({})
    }
    fn readiness(&self) -> Result<()> {
        Ok(())
    }
    fn start(&self) -> AsyncResult<'_> {
        Box::pin(async { Ok(()) })
    }
    fn drain(&self) -> AsyncResult<'_> {
        Box::pin(async { Ok(()) })
    }
    fn shutdown(&self) -> AsyncResult<'_> {
        Box::pin(async { Ok(()) })
    }
}
pub trait Factory<C: Context>: Send + Sync + 'static {
    fn descriptor(&self) -> Descriptor;
    /// Validate settings with strict deserialization before opening resources.
    fn prepare<'a>(&'a self, config: &'a Config) -> AsyncResult<'a, Arc<dyn Plugin<C>>>;
}
pub struct Registry<C: Context> {
    factories: HashMap<String, Arc<dyn Factory<C>>>,
}
impl<C: Context> Default for Registry<C> {
    fn default() -> Self {
        Self {
            factories: HashMap::new(),
        }
    }
}
fn id(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 64
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
}
impl<C: Context> Registry<C> {
    pub fn register(&mut self, factory: Arc<dyn Factory<C>>) -> Result<()> {
        let d = factory.descriptor();
        ensure!(
            id(&d.kind) && !d.version.is_empty() && d.version.len() <= 64,
            "invalid plugin descriptor"
        );
        ensure!(!self.factories.contains_key(&d.kind), "duplicate factory");
        self.factories.insert(d.kind, factory);
        Ok(())
    }
    pub fn catalog(&self) -> Vec<Descriptor> {
        let mut descriptors: Vec<_> = self.factories.values().map(|f| f.descriptor()).collect();
        descriptors.sort_by(|a, b| a.kind.cmp(&b.kind));
        descriptors
    }
    pub async fn prepare(
        &self,
        configs: &[Config],
        initial: BTreeSet<String>,
        budget: Duration,
    ) -> Result<Bundle<C>> {
        ensure!(
            configs.len() <= 64 && !budget.is_zero() && budget <= Duration::from_secs(120),
            "invalid plugin preparation budget"
        );
        let mut ids = HashSet::new();
        for c in configs {
            ensure!(
                id(&c.id)
                    && ids.insert(&c.id)
                    && c.after.len() <= 64
                    && serde_json::to_vec(&c.settings)?.len() <= 65536,
                "invalid plugin config"
            );
        }
        for c in configs {
            ensure!(
                c.after.iter().all(|d| ids.contains(d) && d != &c.id),
                "unknown dependency"
            );
        }
        let deadline = tokio::time::Instant::now() + budget;
        let mut completed = HashSet::new();
        let mut capabilities = initial;
        let mut plugins: Vec<Arc<dyn Plugin<C>>> = Vec::new();
        let outcome = async {
            while completed.len() < configs.len() {
                let next = configs
                    .iter()
                    .find(|c| {
                        !completed.contains(&c.id) && c.after.iter().all(|d| completed.contains(d))
                    })
                    .ok_or_else(|| anyhow::anyhow!("plugin dependency cycle"))?;
                let factory = self
                    .factories
                    .get(&next.kind)
                    .ok_or_else(|| anyhow::anyhow!("unknown plugin kind"))?;
                let descriptor = factory.descriptor();
                ensure!(
                    next.version == descriptor.version,
                    "plugin version mismatch"
                );
                ensure!(
                    descriptor.requires.is_subset(&capabilities),
                    "missing plugin capability"
                );
                let validator = jsonschema::validator_for(&descriptor.settings_schema)
                    .map_err(|_| anyhow::anyhow!("invalid plugin settings schema"))?;
                ensure!(
                    validator.is_valid(&next.settings),
                    "plugin settings fail schema"
                );
                let plugin = factory.prepare(next).await?;
                ensure!(
                    plugin.block().map(|b| PhaseInfo::from(b.phase())) == descriptor.phase,
                    "plugin phase does not match descriptor"
                );
                ensure!(plugin.id() == next.id, "factory returned wrong instance");
                // Validate phase order before starting services.
                let mut blocks: Vec<_> = plugins.iter().filter_map(|p| p.block()).collect();
                blocks.extend(plugin.block());
                wayframe_runtime::Pipeline::compile(blocks)?;
                plugins.push(plugin);
                plugins.last().expect("prepared").start().await?;
                capabilities.extend(descriptor.provides);
                completed.insert(next.id.clone());
            }
            for plugin in &plugins {
                plugin.readiness()?;
            }
            Ok::<_, anyhow::Error>(())
        };
        let result = tokio::time::timeout_at(deadline, outcome).await;
        if !matches!(result, Ok(Ok(()))) {
            for p in plugins.iter().rev() {
                let _ =
                    tokio::time::timeout(budget.min(Duration::from_secs(5)), p.shutdown()).await;
            }
            match result {
                Ok(Err(e)) => return Err(e),
                _ => anyhow::bail!("plugin preparation deadline"),
            }
        }
        Ok(Bundle { plugins, budget })
    }
}
pub struct Bundle<C: Context> {
    plugins: Vec<Arc<dyn Plugin<C>>>,
    budget: Duration,
}
impl<C: Context> Bundle<C> {
    pub fn exports<T: Send + Sync + 'static>(&self) -> Vec<Arc<T>> {
        self.plugins
            .iter()
            .flat_map(|p| p.exports())
            .filter_map(|v| v.downcast::<T>().ok())
            .collect()
    }
    pub fn blocks(&self) -> Vec<Arc<dyn Block<C>>> {
        self.plugins.iter().filter_map(|p| p.block()).collect()
    }
    pub fn pipeline(&self) -> Result<wayframe_runtime::Pipeline<C>> {
        Ok(wayframe_runtime::Pipeline::compile(
            self.plugins.iter().filter_map(|p| p.block()).collect(),
        )?)
    }
    pub fn readiness(&self) -> Result<()> {
        for plugin in &self.plugins {
            plugin.readiness()?;
        }
        Ok(())
    }
    pub fn status(&self) -> Vec<serde_json::Value> {
        self.plugins.iter().map(|p|serde_json::json!({"id":p.id(),"ready":p.readiness().is_ok(),"stats":p.stats()})).collect()
    }
    pub fn ids(&self) -> Vec<&str> {
        self.plugins.iter().map(|p| p.id()).collect()
    }
    pub async fn shutdown(&self) -> Result<()> {
        let deadline = tokio::time::Instant::now() + self.budget;
        let mut errors = Vec::new();
        for p in self.plugins.iter().rev() {
            if !matches!(
                tokio::time::timeout_at(deadline, p.drain()).await,
                Ok(Ok(()))
            ) {
                errors.push(p.id().to_owned());
            }
            if !matches!(
                tokio::time::timeout_at(deadline, p.shutdown()).await,
                Ok(Ok(()))
            ) {
                errors.push(p.id().to_owned());
            }
        }
        ensure!(errors.is_empty(), "plugins failed shutdown: {errors:?}");
        Ok(())
    }
}
