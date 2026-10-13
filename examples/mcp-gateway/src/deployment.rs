//! Complete deployment preparation. Publication happens only after every resource is ready.
#[cfg(feature = "wasm")]
use crate::plugin;
use crate::{config, management, process, session, TenantQuota, UpstreamPath};
use anyhow::{ensure, Result};
use std::sync::Arc;
use wayframe::{
    controls::OriginGuard,
    http::{HttpGateway, RequestContext},
    mcp::McpGuard,
    prelude::*,
};
pub(crate) struct Deployment {
    pub(crate) gateway: Arc<HttpGateway>,
    pub(crate) pool: Option<wayframe::lb::Pool>,
    pub(crate) oidc: Option<Arc<wayframe::auth::JwksAuth>>,
    pub(crate) protected_resource: Option<wayframe::auth::ProtectedResource>,
    pub(crate) hosts: Arc<Vec<String>>,
    pub(crate) origins: Arc<Vec<String>>,
    pub(crate) _bridge: Option<process::Bridge>,
    pub(crate) groups: std::collections::HashMap<String, wayframe::lb::Pool>,
    pub(crate) _probes: Vec<wayframe::resilience::Probe>,
    pub(crate) plugins: Arc<wayframe::plugins::Bundle<RequestContext>>,
}
pub(crate) async fn prepare_deployment(
    config: &config::Config,
    store: Arc<management::Store>,
    sessions: Arc<session::Sessions>,
    generation: u64,
    #[cfg(feature = "wasm")] factory: Arc<plugin::Factory>,
) -> Result<Deployment> {
    let config::Credentials {
        upstream_token,
        backend_tokens,
        ..
    } = config.credentials()?;
    let bridge = if let Some(config) = &config.stdio_upstream {
        Some(process::Bridge::prepare(config.clone()).await?)
    } else {
        None
    };
    let (origin, path) = if let Some(bridge) = &bridge {
        let url: reqwest::Url = bridge.endpoint.parse()?;
        (url.origin().ascii_serialization(), url.path().into())
    } else {
        config.endpoint()?
    };
    let upstream_token = bridge.as_ref().map(|b| b.token.clone()).or(upstream_token);
    let mut services: Vec<_> = backend_tokens.values().cloned().collect();
    services.extend(upstream_token.iter().cloned());
    for control in &config.async_policies {
        services.push(config::secret(&control.token_env)?);
    }
    if let Some(oauth) = &config.upstream_oauth {
        services.push(oauth.secret()?);
    }
    for backend in config
        .load_balancer
        .iter()
        .chain(config.groups.values())
        .flat_map(|lb| &lb.backends)
    {
        if let Some(oauth) = &backend.oauth {
            services.push(oauth.secret()?);
        }
    }
    store.check_service_credentials(&services)?;
    let pool = config
        .load_balancer
        .as_ref()
        .map(config::LoadBalancerConfig::pool)
        .transpose()?;
    let groups = config
        .groups
        .iter()
        .map(|(name, lb)| Ok((name.clone(), lb.pool()?)))
        .collect::<Result<std::collections::HashMap<_, _>>>()?;
    let mut probes = Vec::new();
    for (lb, pool) in config
        .load_balancer
        .iter()
        .zip(pool.iter())
        .chain(config.groups.iter().map(|(name, lb)| (lb, &groups[name])))
    {
        if let Some(path) = &lb.health_path {
            let mut targets = Vec::new();
            for backend in &lb.backends {
                let mut url: reqwest::Url = backend.endpoint.parse()?;
                url.set_path(path);
                let mut client = reqwest::Client::builder()
                    .no_proxy()
                    .redirect(reqwest::redirect::Policy::none());
                if let Some(path) = backend.ca.as_ref().or(config.upstream_ca.as_ref()) {
                    client = client.add_root_certificate(reqwest::Certificate::from_pem(
                        &std::fs::read(path)?,
                    )?);
                }
                targets.push((backend.id.clone(), url, client.build()?));
            }
            probes.push(wayframe::resilience::Probe::start(
                pool.clone(),
                targets,
                std::time::Duration::from_millis(lb.health_interval_ms),
                std::time::Duration::from_secs(2),
            )?);
        }
    }
    if !probes.is_empty() {
        tokio::time::timeout(std::time::Duration::from_secs(25), async {
            loop {
                let ready = pool.iter().chain(groups.values()).all(|p| {
                    p.snapshot()
                        .is_ok_and(|backends| backends.iter().any(|b| b.enabled && b.healthy))
                });
                if ready {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        })
        .await
        .map_err(|_| anyhow::anyhow!("required backend health checks failed"))?;
    }
    let managed = |control| {
        Arc::new(management::ManagedBlock {
            store: store.clone(),
            control,
        }) as Arc<dyn Block<RequestContext>>
    };
    let oidc = config
        .auth
        .clone()
        .map(|auth| wayframe::auth::JwksAuth::prepare("oidc", auth));
    let oidc = if let Some(prepare) = oidc {
        Some(prepare.await?)
    } else {
        None
    };
    let protected_resource = config
        .auth
        .as_ref()
        .map(|auth| {
            wayframe::auth::ProtectedResource::new(
                auth.audience.clone(),
                vec![auth.issuer.clone()],
                auth.permission_map.keys().cloned().collect(),
            )
        })
        .transpose()?;
    let authentication: Arc<dyn Block<RequestContext>> = if let Some(auth) = &oidc {
        auth.clone()
    } else {
        managed(management::Control::Authenticate)
    };
    let mut preflight: Vec<Arc<dyn Block<RequestContext>>> = vec![
        Arc::new(
            OriginGuard::new("origin".into(), config.allowed_origins.clone())
                .map_err(anyhow::Error::msg)?,
        ) as Arc<dyn Block<RequestContext>>,
        authentication,
        sessions.clone(),
    ];
    if let Some(capacity) = config.tenant_concurrency {
        preflight.push(Arc::new(TenantQuota(wayframe::resilience::Quota::new(
            capacity, 4096, 0,
        )?)));
    }
    preflight.push(managed(management::Control::Rate));
    let preflight = Pipeline::compile(preflight)?;
    let continuations = wayframe::management::continuations::Continuations::new(store.clone());
    let mut policy: Vec<Arc<dyn Block<RequestContext>>> = vec![
        Arc::new(
            McpGuard::new("mcp-envelope".into(), config.max_request_bytes)
                .map_err(anyhow::Error::msg)?
                .allow_legacy(true)
                .allow_sessions(true),
        ),
        managed(management::Control::Authorize),
        continuations.clone(),
    ];
    if !config.async_policies.is_empty() {
        ensure!(
            config.async_policies.len() <= 16,
            "too many external policies"
        );
        let mut graph = wayframe::flow::Builder::<RequestContext>::new();
        let root = graph.input();
        let mut gates = Vec::new();
        for control in &config.async_policies {
            let token = config::secret(&control.token_env)?;
            let ca = control
                .ca
                .as_ref()
                .map(|path| {
                    ensure!(
                        std::fs::metadata(path)?.len() <= 65536,
                        "policy CA too large"
                    );
                    Ok::<_, anyhow::Error>(std::fs::read(path)?)
                })
                .transpose()?;
            ensure!(
                (1..=30000).contains(&control.timeout_ms),
                "invalid policy timeout"
            );
            let external = Arc::new(wayframe::transport::ExternalPolicy::https_with_ca(
                control.id.clone(),
                &control.url,
                token,
                std::time::Duration::from_millis(control.timeout_ms),
                ca.as_deref(),
            )?);
            gates.push(graph.gate_async(
                control.id.clone(),
                Phase::Authorize,
                &root,
                move |ctx| {
                    let external = external.clone();
                    async move { external.decide(&ctx).await }
                },
            )?);
        }
        let joined = graph.sync("policy-join", Phase::Authorize, &root, |_| Ok(()))?;
        for gate in &gates {
            graph.depends_on(&joined, gate)?;
        }
        let graph = graph.compile()?;
        policy.push(Arc::new(wayframe::http::workflow::FlowBlock::new(
            "parallel-policies",
            Phase::Authorize,
            graph,
            |ctx| {
                let mut view = ctx.clone();
                view.body = None;
                view.headers = axum::http::HeaderMap::new();
                view.extensions = Default::default();
                view
            },
            |_, _| Decision::Continue,
        )));
    }
    let plugins = {
        let mut configs = config.plugins.clone();
        if let Ok(path) = std::env::var("WAYFRAME_PLUGIN_MANIFEST") {
            configs.push(wayframe::plugins::Config {
                id: "plugin".into(),
                kind: "wasm-policy".into(),
                version: "0.1".into(),
                settings: serde_json::json!({"manifest":path}),
                after: vec![],
            });
        }
        let mut registry = wayframe::plugins::Registry::default();

        for native in wayframe::policies::Native::all() {
            registry.register(Arc::new(native))?;
        }
        #[cfg(feature = "wasm")]
        registry.register(factory.clone())?;
        let bundle = registry
            .prepare(
                &configs,
                ["identity.verified".into(), "operation.verified".into()].into(),
                std::time::Duration::from_secs(30),
            )
            .await?;
        policy.extend(bundle.blocks());
        Arc::new(bundle)
    };
    policy.push(managed(management::Control::ToolRate));
    policy.push(managed(management::Control::Reauthorize));
    policy.push(continuations.final_check());
    if !config.routes.is_empty() {
        policy.push(Arc::new(wayframe::routing::RouteBlock::new(
            "route",
            wayframe::routing::Table::new(config.routes.clone()).map_err(anyhow::Error::msg)?,
        )));
        policy.push(Arc::new(
            wayframe::routing::Groups::new(
                "backend-group",
                config
                    .groups
                    .iter()
                    .map(|(name, lb)| (name.clone(), (groups[name].clone(), lb.strategy)))
                    .collect(),
            )
            .map_err(anyhow::Error::msg)?,
        ));
    }
    if let (Some(pool), Some(lb)) = (&pool, &config.load_balancer) {
        policy.push(Arc::new(wayframe::lb::LoadBalance::new(
            "load-balancer",
            pool.clone(),
            lb.strategy,
        )));
    }
    policy.push(Arc::new(UpstreamPath(path)));
    let mut filters = vec![
        store.catalog_filter(),
        continuations as Arc<dyn wayframe::http::ResponseFilter>,
    ];
    for hooks in plugins.exports::<wayframe::policies::HttpHooks>() {
        filters.extend(hooks.0.clone());
    }
    let mut builder = HttpGateway::builder(origin)
        .upstream_http2(config.upstream_http2)
        .generation(generation)
        .response_filters(filters)
        .preflight(preflight)
        .policy(Pipeline::compile(policy)?)
        .inspect_body(config.max_request_bytes)
        .limits(config.limits())
        .authorities(config.allowed_hosts.clone());
    let caches = plugins.exports::<wayframe::http::cache::CacheHooks>();
    ensure!(
        caches.len() <= 1,
        "configure one response cache per deployment"
    );
    if let Some(cache) = caches.first() {
        builder = builder.response_cache(cache.0.clone());
    }
    if config.load_balancer.is_some() || !config.groups.is_empty() {
        let mut upstreams = Vec::new();
        for lb in config.load_balancer.iter().chain(config.groups.values()) {
            for backend in &lb.backends {
                let mut upstream = wayframe::http::Upstream::new(&backend.id, &backend.endpoint);
                upstream.http2_prior_knowledge = backend.http2;
                if let Some(oauth) = backend.oauth.as_ref().or(config.upstream_oauth.as_ref()) {
                    upstream.provider =
                        Some(wayframe::auth::upstream::OAuth::prepare(oauth.clone()).await?);
                }
                upstream.bearer = backend_tokens
                    .get(&backend.id)
                    .cloned()
                    .or_else(|| upstream_token.clone());
                if let Some(path) = &backend.ca {
                    ensure!(
                        std::fs::metadata(path)?.len() <= 64 * 1024,
                        "backend CA too large"
                    );
                    upstream.ca = Some(std::fs::read(path)?);
                } else {
                    upstream.ca = config.ca()?;
                }
                upstreams.push(upstream);
            }
        }
        builder = builder
            .upstreams(upstreams)
            .failure_cooldown(std::time::Duration::from_millis(
                config
                    .load_balancer
                    .as_ref()
                    .map_or(1000, |lb| lb.failure_cooldown_ms),
            ));
    }
    if let Some(oauth) = &config.upstream_oauth {
        builder = builder
            .upstream_credentials(wayframe::auth::upstream::OAuth::prepare(oauth.clone()).await?);
    }
    if let Some(token) = upstream_token {
        builder = builder.upstream_bearer(token);
    }
    if let Some(ca) = config.ca()? {
        builder = builder.upstream_ca(ca);
    }
    let gateway = builder.build()?;

    Ok(Deployment {
        gateway,
        pool,
        oidc,
        protected_resource,
        hosts: Arc::new(config.allowed_hosts.clone()),
        origins: Arc::new(config.allowed_origins.clone()),
        _bridge: bridge,
        groups,
        _probes: probes,
        plugins,
    })
}
