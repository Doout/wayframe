//! Reference factory connecting the reusable plugin registry to prepared Wasm policies.
#[cfg(feature = "wasm")]
use std::sync::{Arc, Mutex};
#[cfg(feature = "wasm")]
use wayframe::{http::RequestContext, plugins::*, prelude::*};
#[cfg(feature = "wasm")]
pub struct Factory {
    runtime: tokio::sync::OnceCell<Arc<wayframe::wasm::Runtime>>,
    pub pools: Arc<Mutex<Vec<std::sync::Weak<wayframe::wasm::Pool>>>>,
}
#[cfg(feature = "wasm")]
impl Factory {
    pub fn new() -> Self {
        Self {
            runtime: Default::default(),
            pools: Default::default(),
        }
    }
}
#[cfg(feature = "wasm")]
struct Policy {
    id: String,
    pool: Arc<wayframe::wasm::Pool>,
}
#[cfg(feature = "wasm")]
impl Plugin<RequestContext> for Policy {
    fn id(&self) -> &str {
        &self.id
    }
    fn block(&self) -> Option<Arc<dyn Block<RequestContext>>> {
        Some(Arc::new(wayframe::wasm::WasmBlock::new(
            self.id.clone(),
            self.pool.clone(),
        )))
    }
    fn stats(&self) -> serde_json::Value {
        serde_json::json!(self.pool.stats())
    }
    fn readiness(&self) -> anyhow::Result<()> {
        let stats = self.pool.stats();
        anyhow::ensure!(
            stats.ready > 0 && !stats.degraded,
            "plugin pool unavailable"
        );
        Ok(())
    }
}
#[cfg(feature = "wasm")]
impl wayframe::plugins::Factory<RequestContext> for Factory {
    fn descriptor(&self) -> Descriptor {
        Descriptor {
            kind: "wasm-policy".into(),
            version: "0.1".into(),
            phase: Some(PhaseInfo::Authorize),
            requires: ["identity.verified".into(), "operation.verified".into()].into(),
            provides: Default::default(),
            settings_schema: serde_json::json!({"type":"object","properties":{"manifest":{"type":"string"},"trusted_keys":{"type":"object"},"approved_capabilities":{"type":"array"}},"required":["manifest"],"additionalProperties":false}),
        }
    }
    fn prepare<'a>(
        &'a self,
        config: &'a Config,
    ) -> AsyncResult<'a, Arc<dyn Plugin<RequestContext>>> {
        Box::pin(async move {
            #[derive(serde::Deserialize)]
            #[serde(deny_unknown_fields)]
            struct Settings {
                manifest: String,
                #[serde(default)]
                trusted_keys: std::collections::BTreeMap<String, String>,
                #[serde(default)]
                approved_capabilities: std::collections::BTreeSet<String>,
            }
            let settings: Settings = serde_json::from_value(config.settings.clone())?;
            let runtime = self
                .runtime
                .get_or_try_init(|| async { wayframe::wasm::Runtime::new() })
                .await?;
            let trust = if settings.trusted_keys.is_empty() {
                None
            } else {
                Some(wayframe::wasm::Trust::from_hex(
                    &settings.trusted_keys,
                    settings.approved_capabilities,
                )?)
            };
            let pool = if settings.manifest.starts_with("https://") {
                runtime
                    .load_remote(
                        &settings.manifest,
                        trust.as_ref().ok_or_else(|| {
                            anyhow::anyhow!("remote plugins require publisher trust")
                        })?,
                    )
                    .await?
            } else {
                runtime
                    .load_with_trust(std::path::Path::new(&settings.manifest), trust.as_ref())
                    .await?
            };
            {
                let mut pools = self
                    .pools
                    .lock()
                    .map_err(|_| anyhow::anyhow!("plugin pool registry poisoned"))?;
                pools.retain(|p| p.strong_count() > 0);
                anyhow::ensure!(pools.len() < 1024, "plugin pool registry capacity");
                pools.push(Arc::downgrade(&pool));
            }
            Ok(Arc::new(Policy {
                id: config.id.clone(),
                pool,
            }) as Arc<dyn Plugin<RequestContext>>)
        })
    }
}

#[cfg(feature = "wasm")]
impl Default for Factory {
    fn default() -> Self {
        Self::new()
    }
}
